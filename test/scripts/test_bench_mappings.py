"""Actual mapping and non-regular hard-link measurement regressions."""
import contextlib
import ctypes
import hashlib
import io
import json
import os
import pathlib
import select
import signal
import time
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

import bench_flatten


@contextlib.contextmanager
def mapped_file(path):
    path.write_bytes(b'mapped!!' * 1024)
    file = path.open('r+b', buffering=0)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.mmap.restype = ctypes.c_void_p
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    libc.munmap.restype = ctypes.c_int
    address = libc.mmap(None, 8192, 1, 1, file.fileno(), 0)
    if address == ctypes.c_void_p(-1).value:
        file.close()
        raise OSError(ctypes.get_errno(), 'mmap')
    try:
        yield file, address
    finally:
        file.close()
        if libc.munmap(address, 8192):
            raise OSError(ctypes.get_errno(), 'munmap')


class MappingAndLinkMeasurements(unittest.TestCase):
    def test_deleted_mapping_without_fd_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory:
            root = pathlib.Path(directory)
            with mapped_file(root/'held') as (file, address):
                (root/'held').unlink()
                file.close()
                self.assertEqual(ctypes.string_at(address, 8), b'mapped!!')
                with self.assertRaisesRegex(RuntimeError, 'unmeasured scratch mapping'):
                    bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))
            self.assertEqual(list(root.iterdir()), [])

    def test_mapped_file_accounted_by_fd_or_link_is_counted_once(self):
        for name in ['held', 'literal (deleted)', 'line\nname']:
            for linked in [True, False]:
                with self.subTest(name=name, linked=linked), tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory:
                    root = pathlib.Path(directory)
                    with mapped_file(root/name) as (file, _):
                        blocks = os.fstat(file.fileno()).st_blocks * 512
                        if linked:
                            file.close()
                        else:
                            (root/name).unlink()
                        sample = bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))
                        self.assertEqual(sample['scratch_blocks'], root.stat().st_blocks * 512 + blocks)
                        self.assertEqual(sample['scratch_inodes'], 2)
                        self.assertEqual(sample['deleted_open_files'], 0 if linked else 1)

    def test_mapping_created_after_tree_walk_is_counted(self):
        with tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory, contextlib.ExitStack() as mappings:
            root = pathlib.Path(directory)
            original = bench_flatten.live_task_stats
            blocks = None
            def after_tree(*args, **kwargs):
                nonlocal blocks
                if blocks is None:
                    file, _ = mappings.enter_context(mapped_file(root/'literal (deleted)'))
                    blocks = os.fstat(file.fileno()).st_blocks * 512
                    file.close()  # Only the newly created mapping retains it.
                return original(*args, **kwargs)
            with mock.patch.object(bench_flatten, 'live_task_stats', side_effect=after_tree):
                sample = bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))
            self.assertIsNotNone(blocks)
            self.assertEqual(sample['scratch_blocks'], root.stat().st_blocks * 512 + blocks)

    def test_maps_permission_error_fails_but_disappearance_is_tolerated(self):
        with tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory:
            root = pathlib.Path(directory)
            target = pathlib.Path('/proc')/str(os.getpid())/'task'/str(os.getpid())/'maps'
            original = pathlib.Path.open
            for error in [PermissionError, FileNotFoundError, ProcessLookupError]:
                def open_path(path, *args, **kwargs):
                    if path == target:
                        raise error('maps inspection')
                    return original(path, *args, **kwargs)
                with self.subTest(error=error), mock.patch.object(pathlib.Path, 'open', new=open_path):
                    if error is PermissionError:
                        with self.assertRaises(PermissionError):
                            bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))
                    else:
                        self.assertEqual(bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))['deleted_open_files'], 0)

    def test_nonregular_hard_links_differ_from_split_inodes(self):
        with tempfile.TemporaryDirectory(prefix='special-links-') as directory:
            root = pathlib.Path(directory)
            fingerprints = []
            for linked in [True, False]:
                tree = root/str(linked); tree.mkdir()
                os.mkfifo(tree/'fifo-a', 0o600)
                os.symlink('target', tree/'symlink-a')
                for kind in ['fifo', 'symlink']:
                    if linked:
                        os.link(tree/(kind+'-a'), tree/(kind+'-b'), follow_symlinks=False)
                    elif kind == 'fifo':
                        os.mkfifo(tree/'fifo-b', 0o600)
                    else:
                        os.symlink('target', tree/'symlink-b')
                for path in [tree, *tree.iterdir()]:
                    os.utime(path, ns=(0, 0), follow_symlinks=False)
                fingerprints.append(bench_flatten.tree_contents(tree))
            self.assertNotEqual(*fingerprints)
            for kind in ['fifo', 'symlink']:
                self.assertEqual(fingerprints[0][kind+'-b']['hardlink_to'], kind+'-a')
                self.assertNotIn('hardlink_to', fingerprints[1][kind+'-b'])


class ReceiverAndRootMeasurements(unittest.TestCase):
    def test_retained_deleted_root_is_counted(self):
        with tempfile.TemporaryDirectory(prefix='retained-root-') as directory:
            parent = pathlib.Path(directory)
            root = parent/'scratch'; root.mkdir()
            fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            # This live sibling must not be classified as the deleted root.
            sibling = parent/'scratch (deleted)'; sibling.mkdir()
            other = os.open(sibling, os.O_RDONLY | os.O_DIRECTORY)
            try:
                root.rmdir(); root.mkdir()
                retained = os.fstat(fd)
                self.assertEqual(retained.st_nlink, 0)
                sample = bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))
                self.assertEqual(sample['scratch_inodes'], 2)
                self.assertEqual(sample['deleted_open_files'], 1)
                self.assertEqual(sample['scratch_blocks'], (root.stat().st_blocks + retained.st_blocks) * 512)
                self.assertEqual(sample['deleted_open_blocks'], retained.st_blocks * 512)
            finally:
                os.close(fd); os.close(other)

    def test_receiver_short_writes_match_captured_bytes(self):
        for fault in ['short', 'interrupt', 'zero', 'none', 'enospc']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory(prefix='receiver-write-') as directory:
                root=pathlib.Path(directory)
                scratch=root/'scratch';scratch.mkdir()
                results=root/'results';results.mkdir()
                payload=b'complete pipe output' * 8192
                producer=root/'producer'
                producer.write_text('#!/usr/bin/env python3\nimport os\nos.write(1, '+repr(payload)+')\n')
                producer.chmod(0o755)
                consumer=root/'consumer'
                consumer.write_text('#!/usr/bin/env python3\nprint(\'{"config":{}}\')\n')
                consumer.chmod(0o755)
                expected=root/'expected.json';expected.write_text('{}')
                args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                    timing_only=True,consumer=str(consumer),fsck='/bin/true',fsync=False,expected=expected,
                    expected_config=expected,keep_artifacts=True)
                original_open=pathlib.Path.open
                class PartialOutput:
                    def __init__(self, stream):self.stream=stream;self.calls=0
                    def __enter__(self):return self
                    def __exit__(self,*exc):self.stream.close()
                    def write(self, block):
                        self.calls+=1
                        if self.calls==1:
                            if fault=='interrupt':raise InterruptedError('injected interruption')
                            if fault=='zero':return 0
                            if fault=='none':return None
                        if fault=='enospc' and self.calls==2:raise OSError(28,'injected storage exhaustion')
                        return self.stream.write(block[:137])
                def open_path(path,*a,**kw):
                    stream=original_open(path,*a,**kw)
                    return PartialOutput(stream) if path.name=='image.artifact' and a and a[0]=='xb' else stream
                with mock.patch.object(pathlib.Path,'open',new=open_path),mock.patch.object(bench_flatten,'verify_contents') as verify:
                    if fault in ['short','interrupt']:
                        result=bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                        captured=(results/'1-fixture/image.artifact').read_bytes()
                        self.assertEqual(captured,payload)
                        self.assertEqual(result['artifact_bytes'],len(captured))
                        self.assertEqual(result['artifact_blocks'],(results/'1-fixture/image.artifact').stat().st_blocks*512)
                        for field in ['artifact_blocks','scratch_blocks','deleted_open_blocks','process_rss']:
                            self.assertEqual(result['metric_units'][field],'bytes')
                        for field in ['child_io_input_512blocks','child_io_output_512blocks']:
                            self.assertEqual(result['metric_units'][field],'512-byte blocks')
                        self.assertEqual(result['artifact_sha256'],hashlib.sha256(captured).hexdigest())
                        verify.assert_called_once()
                    else:
                        with self.assertRaises(OSError):bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                        verify.assert_not_called()
                        self.assertFalse((results/'1-fixture/result.json').exists())
                self.assertEqual(list(scratch.iterdir()),[])


class AggregatePublication(unittest.TestCase):
    def test_only_matching_accepted_artifacts_publish_aggregate(self):
        for matching in [False, True]:
            with self.subTest(matching=matching), tempfile.TemporaryDirectory(prefix='aggregate-identity-') as directory:
                root = pathlib.Path(directory)
                scratch = root/'scratch'; scratch.mkdir()
                output = root/'results'
                expected = root/'expected.json'; expected.write_text('{}')
                consumer = root/'consumer'
                consumer.write_text('#!/usr/bin/env python3\nprint(\'{"config":{}}\')\n')
                consumer.chmod(0o755)
                argv = ['bench_flatten.py', '--input', 'registry.invalid/image@sha256:'+'0'*64,
                        '--consumer', str(consumer), '--fsck', '/bin/true', '--expected', str(expected),
                        '--expected-config', str(expected), '--scratch-parent', str(scratch),
                        '--output-root', str(output), '--trials', '2', '--timing-only']
                for variant in ['first', 'second']:
                    producer = root/variant
                    payload = 'identical' if matching else variant
                    producer.write_text('#!/usr/bin/env python3\nprint('+repr(payload)+')\n')
                    producer.chmod(0o755)
                    argv.extend(['--variant', variant+','+str(producer)+',/bin/true'])
                # Model byte-different carriers whose decoded contents are both
                # accepted. The actual receiver, config check and aggregate run.
                with mock.patch.object(sys, 'argv', argv), mock.patch.object(bench_flatten, 'verify_contents'), contextlib.redirect_stdout(io.StringIO()):
                    if matching:
                        bench_flatten.main()
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'artifact identities differ'):
                            bench_flatten.main()
                rounds = sorted(output.glob('*/result.json'))
                self.assertEqual(len(rounds), 4)
                self.assertTrue(all(json.loads(p.read_text())['scratch_clean'] for p in rounds))
                self.assertEqual(list(scratch.iterdir()), [])
                aggregate = output/'results.json'
                if matching:
                    results = json.loads(aggregate.read_text())
                    self.assertEqual(len(results), 4)
                    self.assertEqual(len({r['artifact_sha256'] for r in results}), 1)
                else:
                    self.assertFalse(aggregate.exists(), 'rejected comparison published a success aggregate')


class ProcessDirectoryReferences(unittest.TestCase):
    def retained_directory(self, kind):
        with tempfile.TemporaryDirectory(prefix='retained-process-directory-') as directory:
            scratch = pathlib.Path(directory)
            held = scratch/'held'; held.mkdir()
            code = ("import os,sys\n"
                    "path,kind=sys.argv[1:]\n"
                    "if kind in ('root','both'):os.chroot(path)\n"
                    "if kind=='cwd':os.chdir(path)\n"
                    "elif kind=='both':os.chdir('/')\n"
                    "print('ready',flush=True)\n"
                    "sys.stdin.buffer.read(1)\n")
            child = subprocess.Popen([sys.executable, '-c', code, str(held), kind],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertTrue(select.select([child.stdout], [], [], 5)[0], 'child did not become ready')
                self.assertEqual(child.stdout.readline(), b'ready\n')
                processes = bench_flatten.process_tree(child.pid)
                self.assertEqual(bench_flatten.scratch_usage(scratch, processes)['scratch_inodes'], 2)
                held.rmdir()
                reference = pathlib.Path('/proc')/str(child.pid)/('cwd' if kind=='cwd' else 'root')
                retained = reference.stat()
                self.assertEqual(retained.st_nlink, 0)
                sample = bench_flatten.scratch_usage(scratch, processes)
                self.assertEqual(sample['scratch_inodes'], 2)
                self.assertEqual(sample['scratch_blocks'], (scratch.stat().st_blocks+retained.st_blocks)*512)
                self.assertEqual(sample['deleted_open_files'], 1)
                self.assertEqual(sample['deleted_open_blocks'], retained.st_blocks*512)
            finally:
                child.communicate(b'x', timeout=5)
                self.assertEqual(child.returncode, 0)

    def test_deleted_cwd_without_fd_is_counted(self):
        self.retained_directory('cwd')

    @unittest.skipUnless(os.geteuid()==0, 'actual process root reference requires chroot privilege')
    def test_deleted_process_root_is_counted_and_deduplicated(self):
        for kind in ['root', 'both']:
            with self.subTest(kind=kind):
                self.retained_directory(kind)

    def test_directory_reference_errors(self):
        with tempfile.TemporaryDirectory(prefix='directory-reference-errors-') as directory:
            scratch = pathlib.Path(directory)
            original_open = os.open
            for name in ['cwd', 'root']:
                target = pathlib.Path('/proc')/str(os.getpid())/'task'/str(os.getpid())/name
                for error in [PermissionError, FileNotFoundError, ProcessLookupError]:
                    def open_reference(path, *args, **kwargs):
                        if path==target:raise error('directory reference inspection')
                        return original_open(path, *args, **kwargs)
                    with self.subTest(name=name,error=error), mock.patch.object(os,'open',new=open_reference):
                        if error is PermissionError:
                            with self.assertRaisesRegex(PermissionError,'directory reference inspection'):
                                bench_flatten.scratch_usage(scratch, bench_flatten.process_tree(os.getpid()))
                        else:
                            self.assertEqual(bench_flatten.scratch_usage(scratch, bench_flatten.process_tree(os.getpid()))['scratch_inodes'], 1)


class ProcessNameEncoding(unittest.TestCase):
    @contextlib.contextmanager
    def processes_with_byte_names(self):
        code = ("import ctypes,sys\n"
                "libc=ctypes.CDLL(None,use_errno=True)\n"
                "libc.prctl.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong]\n"
                "if libc.prctl(15,b'flat\\xff) \\nname',0,0,0):raise OSError(ctypes.get_errno(),'prctl')\n"
                "print('ready',flush=True)\n"
                "sys.stdin.buffer.read(1)\n")
        children = []
        try:
            # Two independent sessions: the first belongs to the measurement;
            # the second is visible in procfs but must survive its teardown.
            for _ in range(2):
                child = subprocess.Popen([sys.executable, '-c', code], start_new_session=True,
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                children.append(child)
                self.assertTrue(select.select([child.stdout], [], [], 5)[0], 'name fixture did not become ready')
                self.assertEqual(child.stdout.readline(), b'ready\n')
            record = (pathlib.Path('/proc')/str(children[0].pid)/'stat').read_bytes()
            self.assertIn(b'flat\xff) \nname', record)
            start = record.rsplit(b') ', 1)[1].split()[19].decode('ascii')
            yield children[0], children[1], start
        finally:
            for child in children:
                if child.poll() is None:child.terminate()
                child.wait(timeout=5)
                for stream in [child.stdin, child.stdout, child.stderr]:stream.close()

    def test_sampling_accepts_non_utf8_owned_and_unrelated_names(self):
        with self.processes_with_byte_names() as (owned, unrelated, started):
            observed = {pid: rss for pid, rss, _ in bench_flatten.process_tree(owned.pid, started)}
            self.assertGreater(observed[owned.pid], 0)
            self.assertNotIn(unrelated.pid, observed)

    def test_teardown_accepts_non_utf8_names_and_preserves_unrelated_session(self):
        with self.processes_with_byte_names() as (owned, unrelated, started):
            bench_flatten.stop_owned_group(owned, started)
            self.assertIsNotNone(owned.poll())
            self.assertIsNone(unrelated.poll(), 'unrelated session was terminated')


class BoundMeasurementReferences(unittest.TestCase):
    def test_directory_swap_does_not_scan_symlink_target(self):
        with tempfile.TemporaryDirectory(prefix='bound-scratch-walk-') as directory:
            parent = pathlib.Path(directory)
            scratch = parent/'scratch'; scratch.mkdir()
            child = scratch/'changing'; child.mkdir()
            outside = parent/'outside'; outside.mkdir()
            payload = outside/'payload'; payload.write_bytes(b'x'*(1<<20))
            foreign_blocks = payload.stat().st_blocks*512
            original_scandir = os.scandir
            swapped = False
            def scandir(path):
                nonlocal swapped
                named = os.readlink(f'/proc/self/fd/{path}') if isinstance(path, int) else str(path)
                if named==str(child) and not swapped:
                    child.rmdir(); child.symlink_to(outside, target_is_directory=True); swapped=True
                return original_scandir(path)
            before = len(os.listdir('/proc/self/fd'))
            with mock.patch.object(os, 'scandir', new=scandir):
                sample = bench_flatten.scratch_usage(scratch, [])
            self.assertTrue(swapped)
            self.assertLess(sample['scratch_blocks'], foreign_blocks, 'followed replacement symlink outside scratch')
            self.assertEqual(len(os.listdir('/proc/self/fd')), before, 'directory scan leaked descriptors')
            self.assertEqual(payload.read_bytes(), b'x'*(1<<20))

    def test_fd_reuse_keeps_path_and_stat_on_the_bound_object(self):
        with tempfile.TemporaryDirectory(prefix='bound-proc-reference-') as directory:
            parent = pathlib.Path(directory)
            scratch = parent/'scratch'; scratch.mkdir()
            held = scratch/'held'; held.write_bytes(b'h'*8192)
            outside = parent/'outside'; outside.write_bytes(b'x'*(1<<20))
            source_fd, other_fd = os.open(held, os.O_RDONLY), os.open(outside, os.O_RDONLY)
            retained = os.fstat(source_fd); held.unlink()
            original_readlink = os.readlink
            swapped = False
            def readlink(path, *args, **kwargs):
                nonlocal swapped
                target = original_readlink(path, *args, **kwargs)
                if target==str(held)+' (deleted)' and not swapped:
                    os.dup2(other_fd, source_fd); swapped=True
                return target
            before = len(os.listdir('/proc/self/fd'))
            try:
                with mock.patch.object(os, 'readlink', new=readlink):
                    sample = bench_flatten.scratch_usage(scratch, bench_flatten.process_tree(os.getpid()))
                self.assertTrue(swapped)
                self.assertEqual(sample['scratch_blocks'], (scratch.stat().st_blocks+retained.st_blocks)*512)
                self.assertEqual(sample['deleted_open_blocks'], retained.st_blocks*512)
                self.assertEqual(sample['deleted_open_files'], 1)
                self.assertEqual(len(os.listdir('/proc/self/fd')), before, 'proc scan leaked descriptors')
            finally:
                os.close(source_fd); os.close(other_fd)


class ThreadGroupMeasurements(unittest.TestCase):
    @contextlib.contextmanager
    def worker(self, mode, scratch):
        code = """import ctypes,os,pathlib,sys,threading,time
libc=ctypes.CDLL(None,use_errno=True)
mode=sys.argv[1];scratch=pathlib.Path(sys.argv[2]);ready=threading.Event()
def work():
 if mode=='files' and libc.unshare(0x400):
  print('UNSHARE',ctypes.get_errno(),flush=True);os._exit(3)
 path=scratch/'held';path.write_bytes(b'x'*8192);fd=os.open(path,os.O_RDONLY);os.dup(fd);path.unlink()
 print('READY',os.fstat(fd).st_blocks*512,flush=True);ready.set()
 while True:time.sleep(1)
threading.Thread(target=work).start();ready.wait()
if mode=='zombie':libc.pthread_exit(None)
while True:time.sleep(1)
"""
        process = subprocess.Popen([sys.executable, '-B', '-c', code, mode, str(scratch)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, text=True)
        wait = process.wait
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0], 'worker did not become ready')
            ready = process.stdout.readline().strip()
            if ready.startswith('UNSHARE '):
                self.skipTest('CLONE_FILES fixture unavailable: errno '+ready.split()[1])
            self.assertTrue(ready.startswith('READY '), ready)
            path = pathlib.Path('/proc')/str(process.pid)/'stat'
            fields = path.read_text().rsplit(') ',1)[1].split()
            if mode == 'zombie':
                deadline = time.monotonic()+5
                while fields[0] != 'Z' and time.monotonic() < deadline:
                    time.sleep(.01);fields = path.read_text().rsplit(') ',1)[1].split()
                self.assertEqual(fields[0], 'Z', 'main thread did not exit')
            # Bound a regressed unbounded wait in the test, then always kill
            # and join this fixture's own session in the finally block.
            process.wait = lambda timeout=None: wait(timeout=1 if timeout is None else timeout)
            yield process, fields[19], int(ready.split()[1])
        finally:
            if process.poll() is None:os.killpg(process.pid, signal.SIGKILL)
            wait(timeout=5);process.stdout.close();process.stderr.close()

    def test_private_worker_file_table_is_counted_once(self):
        with tempfile.TemporaryDirectory(prefix='thread-fds-') as directory:
            root = pathlib.Path(directory)
            with self.worker('files', root) as (process, start, blocks):
                sample = bench_flatten.scratch_usage(root, bench_flatten.process_tree(process.pid, start))
                self.assertEqual(sample['deleted_open_files'], 1)
                self.assertEqual(sample['deleted_open_blocks'], blocks)
                self.assertEqual(sample['scratch_blocks'], root.stat().st_blocks*512+blocks)

    def test_zombie_leader_keeps_live_worker_observed_and_stoppable(self):
        with tempfile.TemporaryDirectory(prefix='thread-leader-') as directory:
            root = pathlib.Path(directory)
            with self.worker('zombie', root) as (process, start, blocks):
                observed = bench_flatten.process_tree(process.pid, start)
                self.assertEqual(len(observed), 1)
                self.assertEqual(observed[0][0], process.pid)
                self.assertGreater(observed[0][1], 0)
                sample = bench_flatten.scratch_usage(root, observed)
                self.assertEqual(sample['deleted_open_files'], 1)
                self.assertEqual(sample['deleted_open_blocks'], blocks)
                bench_flatten.stop_owned_group(process, start)
                self.assertIsNotNone(process.poll())

    def test_denied_task_enumeration_invalidates_sample(self):
        with tempfile.TemporaryDirectory(prefix='thread-inspection-') as directory:
            root = pathlib.Path(directory)
            target = pathlib.Path('/proc')/str(os.getpid())/'task'
            original = pathlib.Path.iterdir
            def tasks(path):
                if path == target:raise PermissionError('owned tasks denied')
                return original(path)
            with mock.patch.object(pathlib.Path, 'iterdir', new=tasks):
                with self.assertRaisesRegex(PermissionError, 'owned tasks denied'):
                    bench_flatten.scratch_usage(root, bench_flatten.process_tree(os.getpid()))

    def test_reused_zombie_group_is_not_observed(self):
        for reuse in ['before-tasks', 'during-tasks']:
            with self.subTest(reuse=reuse), tempfile.TemporaryDirectory(prefix='reused-task-') as directory:
                process = pathlib.Path(directory)/'100';process.mkdir()
                task = process/'task/100';task.mkdir(parents=True)
                def row(state, start, session, rss):
                    fields = [state, '1', '100', str(session)]+['0']*18
                    fields[19], fields[21] = str(start), str(rss)
                    return '100 (fixture) '+' '.join(fields)
                group_stat = process/'stat';group_stat.write_text(row('Z', 10, 100, 0))
                (task/'stat').write_text(row('S', 20, 200, 100))
                original = pathlib.Path.read_text
                reads = 0
                def read(path, *args, **kwargs):
                    nonlocal reads
                    if path == group_stat:
                        reads += 1
                        if reads > (1 if reuse == 'before-tasks' else 2):
                            return row('S', 20, 200, 100)
                    return original(path, *args, **kwargs)
                with mock.patch.object(pathlib.Path, 'glob', return_value=[group_stat]), \
                     mock.patch.object(pathlib.Path, 'read_text', new=read):
                    self.assertEqual(bench_flatten.process_tree(100, '10'), [])



class MeasurementInputIdentity(unittest.TestCase):
    def test_resource_scan_retains_observed_process_identity(self):
        for changed in ['start', 'session']:
            with self.subTest(changed=changed), tempfile.TemporaryDirectory(prefix='sample-identity-') as directory:
                root = pathlib.Path(directory)
                held = root/'held';held.write_bytes(b'x'*8192)
                with held.open('rb'):
                    held.unlink()
                    observed = bench_flatten.process_tree(os.getpid())
                    target = pathlib.Path('/proc')/str(os.getpid())/'stat'
                    original = pathlib.Path.read_text
                    def reused(path, *args, **kwargs):
                        value = original(path, *args, **kwargs)
                        if path == target:
                            prefix, tail = value.rsplit(') ', 1);fields = tail.split()
                            index = 19 if changed == 'start' else 3
                            fields[index] = str(int(fields[index])+1)
                            return prefix+') '+' '.join(fields)
                        return value
                    with mock.patch.object(pathlib.Path, 'read_text', new=reused):
                        sample = bench_flatten.scratch_usage(root, observed)
                    self.assertEqual(sample['deleted_open_files'], 0)

    def run_config(self, source='flag', mutation=None):
        with tempfile.TemporaryDirectory(prefix='producer-config-') as directory:
            root = pathlib.Path(directory)
            scratch = root/'scratch';scratch.mkdir()
            results = root/'results';results.mkdir()
            config = root/'remote.yaml';config.write_text('platform: linux/amd64\n')
            alias = root/'config-link';alias.symlink_to(config.name)
            expected = root/'expected.json';expected.write_text('{}')
            producer = root/'producer'
            code = '#!/usr/bin/env python3\nimport pathlib,sys,time\ntime.sleep(.02)\n'
            if mutation:
                code += 'config=pathlib.Path(sys.argv[sys.argv.index("--config")+1])\n'
                if mutation == 'content':code += 'config.write_text("platform: linux/arm64\\n")\n'
                if mutation == 'replace':code += 'data=config.read_bytes();config.unlink();config.write_bytes(data)\n'
                if mutation == 'remove':code += 'config.unlink()\n'
            producer.write_text(code+'print("fixture")\n');producer.chmod(0o755)
            consumer = root/'consumer'
            consumer.write_text('#!/usr/bin/env python3\nprint(\'{"config":{}}\')\n');consumer.chmod(0o755)
            args = SimpleNamespace(scratch_parent=scratch, progress_timing=False,
                config=alias if source=='flag' else None, input='fixture', timing_only=True,
                consumer=str(consumer), fsck='/bin/true', fsync=False, expected=expected,
                expected_config=expected, keep_artifacts=False)
            verified = {}
            # A flag overrides a stale inherited path, exactly as in the CLI.
            environment = {'FLATTEN_CONFIG':str(alias) if source=='env' else str(root/'absent') if source=='flag' else ''}
            with mock.patch.dict(os.environ, environment), mock.patch.object(bench_flatten,'verify_contents'):
                if mutation:
                    with self.assertRaisesRegex(RuntimeError, 'producer config changed'):
                        bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,verified)
                    self.assertFalse(verified)
                    self.assertFalse(list(results.glob('*/result.json')))
                else:
                    result = bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,verified)
                    if source == 'default':
                        self.assertIsNone(result['producer_config'])
                    else:
                        self.assertEqual(result['producer_config']['path'],str(config.resolve()))
                        self.assertEqual(result['producer_config']['sha256'],bench_flatten.file_digest(config))
                        self.assertEqual(result['command'][result['command'].index('--config')+1],str(config.resolve()))
            self.assertEqual(list(scratch.iterdir()),[])

    def test_producer_config_provenance(self):
        for source in ['flag','env','default']:
            with self.subTest(source=source):self.run_config(source)

    def test_changed_producer_config_rejects_result(self):
        for mutation in ['content','replace','remove']:
            with self.subTest(mutation=mutation):self.run_config(mutation=mutation)



class MeasurementScanLifetime(unittest.TestCase):
    def test_identity_change_during_resource_reads_discards_contributions(self):
        for when in ['before-references','during-references','failed-references','changed-task']:
            with self.subTest(when=when), tempfile.TemporaryDirectory(prefix='scan-lifetime-') as directory:
                root=pathlib.Path(directory);held=root/'held';held.write_bytes(b'x'*8192)
                with held.open('rb'):
                    held.unlink();observed=bench_flatten.process_tree(os.getpid())
                    group=pathlib.Path('/proc')/str(os.getpid())/'stat'
                    original_read=pathlib.Path.read_text;original_tasks=bench_flatten.live_task_stats
                    original_open=os.open;changed=False
                    def tasks(*args,**kwargs):
                        nonlocal changed
                        result=original_tasks(*args,**kwargs)
                        if when=='before-references':changed=True
                        return result
                    def opened(path,*args,**kwargs):
                        nonlocal changed
                        if '/task/' in str(path) and when=='failed-references':
                            changed=True
                            raise PermissionError('replacement process is inaccessible')
                        result=original_open(path,*args,**kwargs)
                        if when in ['during-references','changed-task'] and '/task/' in str(path):changed=True
                        return result
                    def read(path,*args,**kwargs):
                        value=original_read(path,*args,**kwargs)
                        target = group.parent/'task'/str(os.getpid())/'stat' if when=='changed-task' else group
                        if changed and path==target:
                            prefix,tail=value.rsplit(') ',1);fields=tail.split();fields[19]=str(int(fields[19])+1)
                            return prefix+') '+' '.join(fields)
                        return value
                    with mock.patch.object(bench_flatten,'live_task_stats',side_effect=tasks), \
                         mock.patch.object(os,'open',side_effect=opened), \
                         mock.patch.object(pathlib.Path,'read_text',new=read):
                        sample=bench_flatten.scratch_usage(root,observed)
                    self.assertTrue(changed)
                    self.assertEqual(sample['deleted_open_files'],0)

    def check_depth(self, operation):
        code=r"""
import os,pathlib,resource,sys,tempfile
sys.path.insert(0,sys.argv[1]);import bench_flatten as b
depth=int(sys.argv[2]);operation=sys.argv[3];limit=int(sys.argv[4])
root=pathlib.Path(tempfile.mkdtemp(prefix='scan-depth-'));tree=root/'tree';tree.mkdir();nodes=[tree]
old=resource.getrlimit(resource.RLIMIT_NOFILE)
try:
 for _ in range(depth):
  path=nodes[-1]/'d';path.mkdir();nodes.append(path)
 outside=root/'outside';outside.write_text('unchanged')
 (nodes[-1]/'external').symlink_to(outside)
 if limit:resource.setrlimit(resource.RLIMIT_NOFILE,(limit,old[1]))
 descriptors=len(os.listdir('/proc/self/fd'))
 if operation=='scan':
  result=b.scratch_usage(tree,[]);assert result['scratch_inodes']==depth+2,result
 else:
  for path in reversed(nodes):path.chmod(0)
  b.remove_verification_tree(tree);assert not tree.exists()
 assert outside.read_text()=='unchanged'
 assert len(os.listdir('/proc/self/fd'))==descriptors,'descriptor leak'
finally:
 resource.setrlimit(resource.RLIMIT_NOFILE,old)
 for path in nodes:
  if path.exists():path.chmod(0o700)
 link=nodes[-1]/'external'
 if link.is_symlink():link.unlink()
 for path in reversed(nodes):
  if path.exists():path.rmdir()
 (root/'outside').unlink(missing_ok=True);root.rmdir()
"""
        for depth,limit in [(1050,0),(64,32)]:
            with self.subTest(depth=depth,fd_limit=limit):
                result=subprocess.run([sys.executable,'-B','-c',code,str(pathlib.Path(bench_flatten.__file__).parent),str(depth),operation,str(limit)],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=30)
                self.assertEqual(result.returncode,0,result.stdout[-3000:])

    def test_scratch_traversal_has_bounded_descriptor_depth(self):
        self.check_depth('scan')

    def test_verification_cleanup_has_bounded_descriptor_depth(self):
        self.check_depth('cleanup')
