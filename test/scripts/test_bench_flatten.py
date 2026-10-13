#!/usr/bin/env python3
"""Exercise measurement cancellation with live owned/background writers."""
import os
import pathlib
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from types import SimpleNamespace

import bench_flatten


class BenchmarkLifecycle(unittest.TestCase):
    def test_scratch_counts_symlink_directories_and_deduplicates_hardlinks(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root = pathlib.Path(directory)
            (root/'directory').mkdir()
            (root/'payload').write_bytes(b'payload')
            (root/'alias').symlink_to('directory', target_is_directory=True)
            os.link(root/'payload', root/'hardlink')
            measured = bench_flatten.scratch_usage(root, [])
            self.assertEqual(measured['scratch_inodes'], 4)
            self.assertEqual(measured['scratch_blocks'], sum(path.lstat().st_blocks*512 for path in
                             [root, root/'directory', root/'payload', root/'alias']))

    def test_content_fingerprint_distinguishes_hardlinks_from_duplicate_files(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            # Create the canonical path last so directory enumeration order
            # cannot choose the identity of a hard-link group.
            (tree/'z').write_bytes(b'payload')
            os.link(tree/'z', tree/'a')
            (tree/'copy').write_bytes(b'payload')
            linked = bench_flatten.tree_contents(tree)
            self.assertEqual(linked['z']['hardlink_to'], 'a')
            self.assertNotIn('hardlink_to', linked['a'])
            self.assertNotIn('hardlink_to', linked['copy'])
            self.assertEqual(linked['a'], linked['copy'])
            (tree/'z').unlink()
            (tree/'z').write_bytes(b'payload')
            copied = bench_flatten.tree_contents(tree)
            self.assertEqual({k: v for k, v in linked['z'].items() if k != 'hardlink_to'}, copied['z'])
            self.assertNotEqual(linked, copied)

    def test_content_fingerprint_distinguishes_fifo_from_socket(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            node = tree/'node'
            os.mkfifo(node, 0o600)
            fifo = bench_flatten.tree_contents(tree)['node']
            node.unlink()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as endpoint:
                endpoint.bind(str(node))
                node.chmod(0o600)
                unix_socket = bench_flatten.tree_contents(tree)['node']
            self.assertEqual(fifo['kind'], stat.S_IFIFO)
            self.assertEqual(unix_socket['kind'], stat.S_IFSOCK)
            self.assertNotEqual(fifo, unix_socket)
            self.assertEqual({k: v for k, v in fifo.items() if k != 'kind'},
                             {k: v for k, v in unix_socket.items() if k != 'kind'})

    def test_content_fingerprint_distinguishes_device_kinds(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            (tree/'node').touch()
            rows = []
            # Device creation needs privileges; inject only lstat's type bits
            # to cover identical major/minor values through the real fingerprint.
            for kind in [stat.S_IFCHR, stat.S_IFBLK]:
                info = SimpleNamespace(st_mode=kind | 0o600, st_uid=1000,
                                       st_gid=1000, st_rdev=os.makedev(1, 3))
                with mock.patch.object(pathlib.Path, 'lstat', return_value=info):
                    row = bench_flatten.tree_contents(tree)['node']
                self.assertEqual(row['kind'], kind)
                rows.append(row)
            self.assertNotEqual(*rows)
            self.assertEqual({k: v for k, v in rows[0].items() if k != 'kind'},
                             {k: v for k, v in rows[1].items() if k != 'kind'})

    def test_receiver_timing_excludes_post_eof_teardown(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root = pathlib.Path(directory)
            scratch, results = root/'scratch', root/'results'
            scratch.mkdir()
            results.mkdir()
            producer = root/'producer.py'
            producer.write_text('#!/usr/bin/env python3\nimport sys,time\n'
                                'time.sleep(0.02)\nsys.stdout.buffer.write(b"fixture")\n')
            producer.chmod(0o755)
            expected = root/'expected.json'
            expected.write_text('{}')
            args = SimpleNamespace(scratch_parent=scratch, progress_timing=False,
                                   config=None, input='fixture', timing_only=True,
                                   consumer='/bin/true', fsync=False, expected=expected,
                                   keep_artifacts=False)
            clock = time.monotonic
            stop = bench_flatten.stop_owned_group
            run = subprocess.run
            offset = 0

            def delayed_stop(*args):
                nonlocal offset
                stop(*args)
                # Account for a long owned cleanup without slowing the suite.
                offset += 100

            def timed_consumer(*args, **kwargs):
                nonlocal offset
                result = run(*args, **kwargs)
                offset += 2
                return result

            with mock.patch.object(bench_flatten.time, 'monotonic', side_effect=lambda: clock()+offset), \
                 mock.patch.object(bench_flatten, 'stop_owned_group', side_effect=delayed_stop), \
                 mock.patch.object(bench_flatten.subprocess, 'run', side_effect=timed_consumer), \
                 mock.patch.object(bench_flatten, 'verify_contents'):
                result = bench_flatten.one(args, ('fixture', str(producer), '/bin/true'), 1, results, {})
            self.assertGreaterEqual(result['post_eof_teardown_seconds'], 100)
            self.assertGreaterEqual(result['receiver_validation_seconds'], 2)
            self.assertLess(result['receiver_validation_seconds'], result['post_eof_teardown_seconds'])
            self.assertAlmostEqual(result['accepted_seconds'], result['producer_eof_seconds'] +
                                   result['post_eof_teardown_seconds'] + result['receiver_validation_seconds'])
            self.assertEqual(list(scratch.iterdir()), [])

    def test_cancel_joins_writer_in_another_owned_process_group(self):
        self.cancel_measurement(True, timing_only=True, new_group=True)

    def test_cancel_joins_background_writer_and_preserves_unrelated_process(self):
        self.cancel_measurement(False)

    def test_cancel_joins_progress_reader_and_background_writer(self):
        self.cancel_measurement(True)

    def test_cancel_without_resource_sampler_joins_writers(self):
        self.cancel_measurement(True, timing_only=True)

    def cancel_measurement(self, progress, timing_only=False, new_group=False):
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
child_code = ("import pathlib,sys,time\\n"
              "out=(pathlib.Path(sys.argv[1])/'background').open('wb')\\n"
              "while True:\\n out.write(b'x'); out.flush(); time.sleep(0.02)\\n")
child = subprocess.Popen([sys.executable, '-c', child_code, str(scratch)],
                         process_group=CHILD_GROUP)
while not (scratch/'background').exists():
    if child.poll() is not None: raise RuntimeError('background writer failed')
    time.sleep(0.01)
(root/'writers').write_text(str(os.getpid())+' '+str(child.pid))
with (scratch/'active').open('wb') as out:
    while True:
        out.write(b'x'); out.flush(); time.sleep(0.02)
""".replace("CHILD_GROUP", "0" if new_group else "None"))
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
            owned = {}
            try:
                deadline = time.monotonic()+10
                while not (root/'writers').exists():
                    if harness.poll() is not None or time.monotonic() > deadline:
                        self.fail('measurement fixture did not become ready')
                    time.sleep(0.02)
                pids = [int(value) for value in (root/'writers').read_text().split()]
                owned = {pid: (pathlib.Path('/proc')/str(pid)/'stat').read_text().rsplit(') ', 1)[1].split()[19]
                         for pid in pids}
                if new_group:
                    self.assertEqual(os.getsid(pids[1]), pids[0])
                    self.assertNotEqual(os.getpgid(pids[1]), pids[0])
                # Even without task/children, collect the live producer and
                # its actual subprocess. This caught undercounting on the
                # maintained Guest kernel; no synthetic process tree is used.
                original_read = pathlib.Path.read_text
                def without_children(path, *args, **kwargs):
                    if path.name == 'children':
                        raise FileNotFoundError(path)
                    return original_read(path, *args, **kwargs)
                with mock.patch.object(pathlib.Path, 'read_text', without_children):
                    observed = dict(bench_flatten.process_tree(pids[0]))
                self.assertTrue(set(pids).issubset(observed), observed)
                self.assertTrue(all(observed[pid] > 0 for pid in pids), observed)
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
                # Rescue a deliberately regressed harness without touching
                # reused PIDs or the unrelated comparison session.
                for pid, started in owned.items():
                    try:
                        fields = (pathlib.Path('/proc')/str(pid)/'stat').read_text().rsplit(') ', 1)[1].split()
                        if fields[19] == started:
                            os.kill(pid, signal.SIGKILL)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                harness.stderr.close()
                unrelated.terminate()
                unrelated.wait()


if __name__ == '__main__':
    unittest.main()
