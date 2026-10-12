#!/usr/bin/env python3
"""Exercise measurement cancellation with live owned/background writers."""
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class BenchmarkLifecycle(unittest.TestCase):
    def test_cancel_joins_background_writer_and_preserves_unrelated_process(self):
        self.cancel_measurement(False)

    def test_cancel_joins_progress_reader_and_background_writer(self):
        self.cancel_measurement(True)

    def test_cancel_without_resource_sampler_joins_writers(self):
        self.cancel_measurement(True, timing_only=True)

    def cancel_measurement(self, progress, timing_only=False):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root = pathlib.Path(directory)
            scratch = root / 'scratch'
            scratch.mkdir()
            input_file = root / 'input.tar'
            input_file.write_bytes(b'fixture')
            expected = root / 'expected.json'
            expected.write_text('{}')
            producer = root / 'producer.py'
            producer.write_text("""#!/usr/bin/env python3
import os, pathlib, subprocess, sys, time
root = pathlib.Path(__file__).parent
scratch = pathlib.Path(sys.argv[sys.argv.index('--tmpdir')+1])
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
(root/'writers').write_text(str(os.getpid())+' '+str(child.pid))
with (scratch/'active').open('wb') as out:
    while True:
        out.write(b'x'); out.flush(); time.sleep(0.02)
""")
            producer.chmod(0o755)
            unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                         start_new_session=True)
            command = [sys.executable, '-B', str(pathlib.Path(__file__).with_name('bench_flatten.py')),
                       '--variant', f'fixture,{producer},/bin/true', '--input', str(input_file),
                       '--consumer', '/bin/true', '--fsck', '/bin/true', '--expected', str(expected),
                       '--scratch-parent', str(scratch), '--output-root', str(root/'results'), '--trials', '1']
            if progress:
                command.append('--progress-timing')
            if timing_only:
                command.append('--timing-only')
            harness = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic()+10
                while not (root/'writers').exists():
                    if harness.poll() is not None or time.monotonic() > deadline:
                        self.fail('measurement fixture did not become ready')
                    time.sleep(0.02)
                pids = [int(value) for value in (root/'writers').read_text().split()]
                harness.send_signal(signal.SIGTERM)
                _, errors = harness.communicate(timeout=20)
                self.assertNotEqual(harness.returncode, 0, errors)
                self.assertEqual(list(scratch.iterdir()), [], errors)
                for pid in pids:
                    stat = pathlib.Path('/proc')/str(pid)/'stat'
                    try:
                        self.assertEqual(stat.read_text().rsplit(') ', 1)[1].split()[0], 'Z', errors)
                    except FileNotFoundError:
                        pass
                self.assertIsNone(unrelated.poll(), 'unrelated session was terminated')
                self.assertFalse(list((root/'results').glob('*/result.json')), 'failed producer was accepted')
            finally:
                if harness.poll() is None:
                    harness.terminate()
                    try:
                        harness.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        harness.kill(); harness.wait()
                harness.stderr.close()
                unrelated.terminate()
                unrelated.wait()


if __name__ == '__main__':
    unittest.main()
