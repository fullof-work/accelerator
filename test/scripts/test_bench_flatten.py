#!/usr/bin/env python3
"""Exercise measurement cancellation with live owned/background writers."""
import os
import contextlib
import io
import json
import hashlib
import threading
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


    def test_orphan_helpers_are_waited_and_accounted_without_reaping_unrelated_children(self):
        with tempfile.TemporaryDirectory(prefix='flatten-orphan-test-') as directory:
            root=pathlib.Path(directory)
            scratch=root/'scratch';scratch.mkdir()
            results=root/'results';results.mkdir()
            producer=root/'producer'
            helper=root/'helper.py'
            helper.write_text("""import json,os,pathlib,resource,sys,time
root=pathlib.Path(__file__).parent
begin=time.process_time()
while time.process_time()-begin < 0.25: pass
with (root/'io').open('wb',buffering=0) as output:
    output.write(b'x'*(4<<20));os.fsync(output.fileno())
usage=resource.getrusage(resource.RUSAGE_SELF)
(root/'helper.json').write_text(json.dumps({'pid':os.getpid(),'cpu':usage.ru_utime+usage.ru_stime,
                                         'io':usage.ru_oublock}))
print('fixture')
""")
            producer.write_text('#!/usr/bin/env python3\nimport pathlib,subprocess,sys\n'
                                'subprocess.Popen([sys.executable,str(pathlib.Path(__file__).with_name("helper.py"))])\n')
            producer.chmod(0o755)
            consumer=root/'consumer'
            consumer.write_text('#!/usr/bin/env python3\nprint(\'{"config":{}}\')\n')
            consumer.chmod(0o755)
            expected=root/'expected.json';expected.write_text('{}')
            args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                timing_only=True,consumer=str(consumer),fsck='/bin/true',fsync=False,expected=expected,
                expected_config=expected,keep_artifacts=False)
            unrelated=subprocess.Popen([sys.executable,'-c','import sys;sys.exit(19)'])
            try:
                with mock.patch.object(bench_flatten,'verify_contents'):
                    result=bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                helper_usage=json.loads((root/'helper.json').read_text())
                self.assertGreaterEqual(result['child_cpu_user_seconds']+result['child_cpu_system_seconds'],helper_usage['cpu']-0.01)
                self.assertGreaterEqual(result['child_io_output_512blocks'],helper_usage['io'])
                self.assertFalse((pathlib.Path('/proc')/str(helper_usage['pid'])).exists(),'owned zombie was not reaped')
                with self.assertRaises(ChildProcessError):os.waitpid(helper_usage['pid'],os.WNOHANG)
                self.assertEqual(unrelated.wait(timeout=5),19,'unrelated child was reaped by measurement')
            finally:
                unrelated.wait(timeout=5)

    def test_failed_producer_readonly_scratch_cleanup_preserves_primary_error(self):
        for cleanup_failure in [False,True]:
            with self.subTest(cleanup_failure=cleanup_failure), tempfile.TemporaryDirectory(prefix='flatten-readonly-test-') as directory:
                root=pathlib.Path(directory)
                scratch=root/'scratch';scratch.mkdir()
                results=root/'results';results.mkdir()
                outside=root/'outside';outside.mkdir();outside.chmod(0o555)
                producer=root/'producer'
                producer.write_text("""#!/usr/bin/env python3
import pathlib,sys
root=pathlib.Path(sys.argv[sys.argv.index('--tmpdir')+1])
child=root/'readonly';child.mkdir()
(child/'payload').write_bytes(b'preserve primary failure')
(child/'external').symlink_to(pathlib.Path(__file__).parent/'outside',target_is_directory=True)
child.chmod(0o000)
sys.exit(17)
""")
                producer.chmod(0o755)
                args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',timing_only=True)
                remove=bench_flatten.remove_verification_tree
                with mock.patch.object(bench_flatten,'remove_verification_tree',
                        side_effect=PermissionError('injected final removal failure') if cleanup_failure else remove):
                    with self.assertRaisesRegex(RuntimeError,'producer exit 17') as caught:
                        bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                if cleanup_failure:
                    self.assertIn('owned scratch cleanup also failed',caught.exception.__notes__[0])
                    for path in scratch.iterdir():remove(path)
                self.assertEqual(list(scratch.iterdir()),[])
                self.assertEqual(stat.S_IMODE(outside.stat().st_mode),0o555)
                self.assertFalse(list(results.glob('*/result.json')))
                outside.chmod(0o700)

    def process_file_scan_error(self, operation, error):
        with tempfile.TemporaryDirectory(prefix='flatten-fd-test-') as directory:
            scratch = pathlib.Path(directory)
            visible = scratch/'visible'; visible.write_bytes(b'fixture')
            reference = pathlib.Path('/proc')/str(os.getpid())/'fd/fixture'
            original_open, original_readlink, original_stat = os.open, os.readlink, os.fstat
            bound = set()
            def open_reference(path, *args, **kwargs):
                if path==reference:
                    if operation=='open':raise error
                    fd=original_open(visible, os.O_PATH | os.O_CLOEXEC)
                    bound.add(fd)
                    return fd
                return original_open(path, *args, **kwargs)
            def read_reference(path, *args, **kwargs):
                if str(path) in {f'/proc/self/fd/{fd}' for fd in bound} and operation=='readlink':raise error
                return original_readlink(path, *args, **kwargs)
            def stat_reference(fd):
                if fd in bound and operation=='stat':raise error
                return original_stat(fd)
            with mock.patch.object(pathlib.Path, 'iterdir', side_effect=error if operation=='enumerate' else None,
                    return_value=iter([reference])), mock.patch.object(os,'open',new=open_reference), \
                    mock.patch.object(os,'readlink',new=read_reference), mock.patch.object(os,'fstat',new=stat_reference):
                return bench_flatten.scratch_usage(scratch, [(os.getpid(), 0)])

    def test_process_file_inspection_permission_errors_fail(self):
        for operation in ['enumerate', 'open', 'readlink', 'stat']:
            with self.subTest(operation=operation):
                with self.assertRaisesRegex(PermissionError, 'inspection denied'):
                    self.process_file_scan_error(operation, PermissionError('inspection denied'))

    def test_disappearing_process_files_are_tolerated(self):
        for operation in ['enumerate', 'open', 'readlink', 'stat']:
            for error in [FileNotFoundError, ProcessLookupError]:
                with self.subTest(operation=operation, error=error):
                    sample = self.process_file_scan_error(operation, error('process or FD vanished'))
                    self.assertEqual(sample['deleted_open_files'], 0)
                    self.assertEqual(sample['deleted_open_blocks'], 0)

    def test_resource_pass_rejects_denied_process_file_inspection(self):
        with tempfile.TemporaryDirectory(prefix='flatten-fd-test-') as directory:
            root=pathlib.Path(directory)
            scratch=root/'scratch';scratch.mkdir()
            results=root/'results';results.mkdir()
            producer=root/'producer'
            producer.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(0.1)\nprint("fixture")\n')
            producer.chmod(0o755)
            args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                                 timing_only=False,interval=0.01)
            with mock.patch.object(bench_flatten,'scratch_usage',side_effect=PermissionError('process file inspection denied')) as sample, \
                 mock.patch.object(bench_flatten,'verify_contents') as verify:
                with self.assertRaisesRegex(RuntimeError,'measurement failed.*process file inspection denied'):
                    bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                sample.assert_called_once()
                verify.assert_not_called()
            self.assertFalse(list(results.glob('*/result.json')))
            self.assertEqual(list(scratch.iterdir()),[])

    def test_deleted_open_uses_link_count(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root=pathlib.Path(directory)
            path=root/'literal (deleted)'
            path.write_bytes(b'x'*8192)
            with path.open('rb') as opened:
                sample=bench_flatten.scratch_usage(root,[(os.getpid(),0)])
                self.assertEqual(sample['deleted_open_files'],0)
                alias=root/'alias'
                os.link(path,alias)
                path.unlink()
                sample=bench_flatten.scratch_usage(root,[(os.getpid(),0)])
                self.assertEqual(sample['deleted_open_files'],0)
                alias.unlink()
                sample=bench_flatten.scratch_usage(root,[(os.getpid(),0)])
                self.assertEqual(sample['deleted_open_files'],1)
                self.assertEqual(sample['deleted_open_blocks'],os.fstat(opened.fileno()).st_blocks*512)

    def test_readonly_verification_tree_is_cleaned_after_check(self):
        for mismatch in [False,True]:
            with self.subTest(mismatch=mismatch), tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
                root=pathlib.Path(directory)
                outside=root/'outside'
                outside.mkdir();outside.chmod(0o555)
                run=root/'run';run.mkdir()
                expected=root/'expected.json'
                common={'uid':os.getuid(),'gid':os.getgid(),'mtime_ns':0}
                rows={'.':dict(common,type='directory',mode=0o555 if not mismatch else 0o755),
                      'child':dict(common,type='directory',mode=0o555),
                      'child/file':dict(common,type='file',mode=0o444,size=1,sha256=hashlib.sha256(b'x').hexdigest()),
                      'external':dict(common,type='symlink',mode=0o777,target=str(outside))}
                expected.write_text(json.dumps(rows))
                args=SimpleNamespace(expected=expected,consumer='consumer',fsck='fsck')
                def extract(command,**kwargs):
                    if command[0]=='consumer':
                        (run/'verify.erofs').write_bytes(b'raw')
                    else:
                        tree=run/'verify-root';child=tree/'child';child.mkdir()
                        (child/'file').write_bytes(b'x');(child/'file').chmod(0o444)
                        (tree/'external').symlink_to(outside,target_is_directory=True)
                        for path in [*tree.rglob('*'),tree]:
                            os.utime(path,ns=(0,0),follow_symlinks=False)
                        child.chmod(0o555);tree.chmod(0o555)
                    return subprocess.CompletedProcess(command,0)
                try:
                    with mock.patch.object(bench_flatten.subprocess,'run',side_effect=extract):
                        if mismatch:
                            with self.assertRaisesRegex(RuntimeError,'content or metadata differs'):
                                bench_flatten.verify_contents(args,root/'artifact',run)
                        else:
                            bench_flatten.verify_contents(args,root/'artifact',run)
                    self.assertFalse((run/'verify-root').exists())
                    self.assertFalse((run/'verify.erofs').exists())
                    self.assertEqual(stat.S_IMODE(outside.stat().st_mode),0o555)
                finally:
                    outside.chmod(0o700)

    def test_owned_session_sample_retains_reparented_writer(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root=pathlib.Path(directory)
            child_code="""import os,pathlib,sys,time
root=pathlib.Path(sys.argv[1])
path=root/'unlinked'
with path.open('wb') as owned:
    owned.write(b'x'*8192);owned.flush();path.unlink()
    (root/'ready').write_text(str(os.getpid()))
    time.sleep(60)
"""
            parent_code="""import pathlib,subprocess,sys,time
root=pathlib.Path(sys.argv[1])
subprocess.Popen([sys.executable,'-c',sys.argv[2],str(root)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not (root/'exit').exists():time.sleep(0.01)
"""
            parent=subprocess.Popen([sys.executable,'-c',parent_code,str(root),child_code],start_new_session=True)
            try:
                start=(pathlib.Path('/proc')/str(parent.pid)/'stat').read_text().rsplit(') ',1)[1].split()[19]
                deadline=time.monotonic()+5
                while not (root/'ready').exists():
                    if time.monotonic()>deadline:self.fail('background fixture not ready')
                    time.sleep(0.01)
                child=int((root/'ready').read_text())
                (root/'exit').touch();parent.wait(timeout=5)
                self.assertNotIn(child,dict(bench_flatten.process_tree(parent.pid)))
                observed=bench_flatten.process_tree(parent.pid,start)
                self.assertIn(child,dict(observed))
                self.assertGreater(dict(observed)[child],0)
                self.assertEqual(bench_flatten.scratch_usage(root,observed)['deleted_open_files'],1)
            finally:
                try:os.killpg(parent.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                parent.wait(timeout=5)

    def test_exited_process_is_not_a_memory_observation(self):
        child=subprocess.Popen(['/bin/true'],start_new_session=True)
        try:
            # WNOWAIT leaves an actual exited child visible in procfs without
            # relying on scheduler timing or reaping it before observation.
            os.waitid(os.P_PID,child.pid,os.WEXITED|os.WNOWAIT)
            fields=(pathlib.Path('/proc')/str(child.pid)/'stat').read_text().rsplit(') ',1)[1].split()
            self.assertEqual(fields[0],'Z')
            self.assertEqual(bench_flatten.process_tree(child.pid,fields[19]),[])
        finally:
            child.wait(timeout=5)

    def test_process_walk_preserves_edges_of_exited_rows(self):
        with tempfile.TemporaryDirectory(prefix='flatten-proc-test-') as directory:
            root=pathlib.Path(directory)
            rows=[]
            # procfs enumeration is not atomic. Preserve a live descendant
            # even if its recorded parent has exited by the time it is read.
            for pid,parent,state,rss in [(100,1,'Z',0),(101,100,'X',0),(102,101,'S',7)]:
                entry=root/str(pid);entry.mkdir()
                fields=[state,str(parent),'0','100']+['0']*18
                fields[21]=str(rss)
                path=entry/'stat';path.write_text(f'{pid} (fixture) '+ ' '.join(fields))
                rows.append(path)
            with mock.patch.object(pathlib.Path,'glob',return_value=rows):
                self.assertEqual(bench_flatten.process_tree(100),[(102,7*os.sysconf('SC_PAGE_SIZE'))])

    def test_resource_pass_without_observed_process_is_invalid(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root=pathlib.Path(directory)
            scratch=root/'scratch';scratch.mkdir()
            results=root/'results';results.mkdir()
            producer=root/'producer'
            producer.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(0.1)\nprint("fixture")\n')
            producer.chmod(0o755)
            args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                                 timing_only=False,interval=0.01)
            with mock.patch.object(bench_flatten,'process_tree',return_value=[]) as snapshots, \
                 mock.patch.object(bench_flatten,'verify_contents') as verify:
                with self.assertRaisesRegex(RuntimeError,'never observed an owned process; memory measurement is invalid'):
                    bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                self.assertGreater(snapshots.call_count,0,'fixture must take actual monitor samples')
                verify.assert_not_called()
            self.assertFalse(list(results.glob('*/result.json')))
            self.assertEqual(list(scratch.iterdir()),[])

    def test_resource_pass_without_samples_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root=pathlib.Path(directory)
            scratch=root/'scratch';scratch.mkdir()
            results=root/'results';results.mkdir()
            producer=root/'producer'
            producer.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(0.02)\nprint("fixture")\n')
            producer.chmod(0o755)
            expected=root/'expected.json';expected.write_text('{}')
            args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                                 timing_only=False,interval=0.1,consumer='/bin/true',fsync=False,
                                 expected=expected,keep_artifacts=False)
            with mock.patch.object(bench_flatten.threading,'Thread'), mock.patch.object(bench_flatten,'verify_contents'):
                with self.assertRaisesRegex(RuntimeError,'resource pass produced no samples'):
                    bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
            self.assertFalse(list(results.glob('*/result.json')))
            self.assertEqual(list(scratch.iterdir()),[])

    def test_progress_reader_stops_with_live_writer(self):
        reader,writer=os.pipe()
        output=io.BytesIO();events=[];abort=threading.Event()
        thread=threading.Thread(target=bench_flatten.read_progress_stream,
                                args=(os.fdopen(reader,'rb'),output,events,time.monotonic(),abort))
        thread.start()
        try:
            os.write(writer,b'complete line\npartial line')
            deadline=time.monotonic()+2
            while not events:
                if time.monotonic()>deadline:self.fail('reader did not consume progress')
                time.sleep(0.01)
            abort.set();thread.join(timeout=2)
            self.assertFalse(thread.is_alive(),'reader waited for a surviving writer to close')
            self.assertEqual(output.getvalue(),b'complete line\npartial line')
            self.assertEqual([entry['message'] for entry in events],['complete line','partial line'])
        finally:
            abort.set();os.close(writer);thread.join(timeout=2)

    def test_teardown_failure_surfaces_with_stderr_still_open(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root=pathlib.Path(directory)
            scratch=root/'scratch';scratch.mkdir()
            results=root/'results';results.mkdir()
            producer=root/'producer'
            producer.write_text("""#!/usr/bin/env python3
import subprocess,sys,time
subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],stdout=subprocess.DEVNULL)
time.sleep(0.02)
print('fixture')
""")
            producer.chmod(0o755)
            args=SimpleNamespace(scratch_parent=scratch,progress_timing=True,config=None,input='fixture',timing_only=True)
            owned=[];errors=[]
            original=subprocess.Popen
            def launch(*args,**kwargs):
                process=original(*args,**kwargs);owned.append(process);return process
            def measure():
                try:
                    bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,{})
                except Exception as error:
                    errors.append(error)
            worker=threading.Thread(target=measure)
            with mock.patch.object(bench_flatten.subprocess,'Popen',side_effect=launch), \
                 mock.patch.object(bench_flatten,'stop_owned_group',side_effect=RuntimeError('injected teardown failure')):
                worker.start()
                try:
                    worker.join(timeout=3)
                    self.assertFalse(worker.is_alive(),'teardown error blocked on live stderr writer')
                    self.assertEqual(len(errors),1)
                    self.assertIn('injected teardown failure',str(errors[0]))
                    self.assertTrue(list(scratch.iterdir()),'failed teardown scratch must be retained')
                    self.assertFalse(list(results.glob('*/result.json')))
                finally:
                    for process in owned:
                        try:os.killpg(process.pid,signal.SIGKILL)
                        except ProcessLookupError:pass
                        process.wait(timeout=5)
                    worker.join(timeout=5)

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

    def test_scratch_unreadable_subtree_fails_measurement(self):
        if os.geteuid() == 0:
            self.skipTest('requires ordinary UID to observe directory permissions')
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            root = pathlib.Path(directory)
            hidden = root/'hidden'
            hidden.mkdir()
            (hidden/'payload').write_bytes(b'x'*8192)
            hidden.chmod(0o100)
            try:
                with self.assertRaises(PermissionError):
                    bench_flatten.scratch_usage(root, [])
            finally:
                hidden.chmod(0o700)

    def test_scratch_disappearing_directory_is_not_a_permission_failure(self):
        def vanished(*args, onerror, **kwargs):
            onerror(FileNotFoundError('producer removed its owned directory'))
            return iter(())
        with tempfile.TemporaryDirectory(prefix='vanished-scratch-') as directory:
            with mock.patch.object(os, 'fwalk', side_effect=vanished):
                self.assertEqual(bench_flatten.scratch_usage(pathlib.Path(directory), [])['scratch_inodes'], 0)

    def test_content_fingerprint_records_mtime_without_atime(self):
        with tempfile.TemporaryDirectory(prefix='flatten-time-test-') as directory:
            tree = pathlib.Path(directory)
            (tree/'directory').mkdir()
            (tree/'file').write_bytes(b'unchanged')
            os.link(tree/'file', tree/'hardlink')
            (tree/'symlink').symlink_to('file')
            os.mkfifo(tree/'fifo')
            paths = [tree, *tree.rglob('*')]
            for path in paths:
                os.utime(path, ns=(0, 0), follow_symlinks=False)
            fixed = bench_flatten.tree_contents(tree)
            self.assertTrue(all(row['mtime_ns'] == 0 for row in fixed.values()))
            for path in paths:
                with self.subTest(path=str(path.relative_to(tree))):
                    os.utime(path, ns=(123, 0), follow_symlinks=False)
                    self.assertEqual(bench_flatten.tree_contents(tree), fixed)
                    os.utime(path, ns=(123, 1), follow_symlinks=False)
                    changed = bench_flatten.tree_contents(tree)
                    self.assertNotEqual(changed, fixed)
                    self.assertEqual(changed[str(path.relative_to(tree))]['mtime_ns'], 1)
                    os.utime(path, ns=(0, 0), follow_symlinks=False)

    def test_content_verification_rejects_noncanonical_mtime(self):
        for changed in [None, '.', 'file']:
            with self.subTest(changed=changed), tempfile.TemporaryDirectory(prefix='flatten-time-test-') as directory:
                root = pathlib.Path(directory)
                run = root/'run';run.mkdir()
                common = {'uid': os.getuid(), 'gid': os.getgid(), 'mtime_ns': 0}
                expected = root/'expected.json'
                expected.write_text(json.dumps({
                    '.': dict(common, type='directory', mode=0o755),
                    'file': dict(common, type='file', mode=0o644, size=1, sha256=hashlib.sha256(b'x').hexdigest())}))
                args = SimpleNamespace(expected=expected, consumer='consumer', fsck='fsck')
                def extract(command, **kwargs):
                    if command[0] == 'consumer':
                        (run/'verify.erofs').write_bytes(b'raw')
                    else:
                        tree = run/'verify-root'
                        (tree/'file').write_bytes(b'x')
                        (tree/'file').chmod(0o644);tree.chmod(0o755)
                        for path in [tree, tree/'file']:
                            os.utime(path, ns=(0, 0))
                        if changed is not None:
                            os.utime(tree/changed, ns=(0, 1))
                    return subprocess.CompletedProcess(command, 0)
                with mock.patch.object(bench_flatten.subprocess, 'run', side_effect=extract):
                    if changed is None:
                        bench_flatten.verify_contents(args, root/'artifact', run)
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'content or metadata differs'):
                            bench_flatten.verify_contents(args, root/'artifact', run)
                        actual = json.loads((run/'actual-content.json').read_text())
                        self.assertEqual(actual[changed]['mtime_ns'], 1)
                self.assertFalse((run/'verify-root').exists())
                self.assertFalse((run/'verify.erofs').exists())

    def test_content_fingerprint_includes_root_metadata(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            (tree/'file').write_bytes(b'unchanged')
            before = bench_flatten.tree_contents(tree)
            self.assertEqual(before['.'], {'type': 'directory', 'mode': 0o700,
                                          'uid': os.getuid(), 'gid': os.getgid(),
                                          'mtime_ns': tree.stat().st_mtime_ns})
            tree.chmod(0o750)
            after = bench_flatten.tree_contents(tree)
            self.assertEqual(before['file'], after['file'])
            self.assertNotEqual(before, after)
            self.assertEqual(after['.']['mode'], 0o750)

    def test_content_fingerprint_distinguishes_hardlinks_from_duplicate_files(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            # Create the canonical path last so directory enumeration order
            # cannot choose the identity of a hard-link group.
            (tree/'z').write_bytes(b'payload')
            os.link(tree/'z', tree/'a')
            (tree/'copy').write_bytes(b'payload')
            for path in tree.iterdir():
                os.utime(path, ns=(0, 0))
            linked = bench_flatten.tree_contents(tree)
            self.assertEqual(linked['z']['hardlink_to'], 'a')
            self.assertNotIn('hardlink_to', linked['a'])
            self.assertNotIn('hardlink_to', linked['copy'])
            self.assertEqual(linked['a'], linked['copy'])
            (tree/'z').unlink()
            (tree/'z').write_bytes(b'payload')
            os.utime(tree/'z', ns=(0, 0))
            copied = bench_flatten.tree_contents(tree)
            self.assertEqual({k: v for k, v in linked['z'].items() if k != 'hardlink_to'}, copied['z'])
            self.assertNotEqual(linked, copied)

    def test_content_fingerprint_distinguishes_fifo_from_socket(self):
        with tempfile.TemporaryDirectory(prefix='flatten-bench-test-') as directory:
            tree = pathlib.Path(directory)
            node = tree/'node'
            os.mkfifo(node, 0o600)
            os.utime(node, ns=(0, 0))
            fifo = bench_flatten.tree_contents(tree)['node']
            node.unlink()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as endpoint:
                endpoint.bind(str(node))
                node.chmod(0o600)
                os.utime(node, ns=(0, 0))
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
                identity = (tree/'node').lstat()
                info = SimpleNamespace(st_mode=kind | 0o600, st_uid=1000,
                                       st_gid=1000, st_mtime_ns=0, st_rdev=os.makedev(1, 3),
                                       st_dev=identity.st_dev, st_ino=identity.st_ino)
                original = pathlib.Path.lstat
                def lstat_node(path):
                    return info if path == tree/'node' else original(path)
                with mock.patch.object(pathlib.Path, 'lstat', new=lstat_node):
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
                                   consumer='/bin/true', fsck='/bin/true', fsync=False, expected=expected, expected_config=expected,
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
                kwargs['stdout'].write(b'{"config":{}}')
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

    def test_config_validation_and_consumer_provenance(self):
        fixed = {'Architecture':'amd64', 'Os':'linux', 'User':'10001:10002',
                 'Env':['LANG=C', 'FIXTURE=161'], 'Entrypoint':['/bin/fixture'],
                 'Healthcheck':{'Test':['CMD','/bin/fixture','--health'], 'Interval':1000000000}}
        variants = [('valid', fixed), ('missing', None)]
        for key in fixed:
            changed = dict(fixed)
            changed.pop(key)
            variants.append((key, changed))
        for label, config in variants:
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix='flatten-config-test-') as directory:
                root=pathlib.Path(directory)
                scratch=root/'scratch';scratch.mkdir()
                results=root/'results';results.mkdir()
                producer=root/'producer'
                producer.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(0.02)\nprint("same tree")\n')
                producer.chmod(0o755)
                consumer=root/'consumer'
                consumer.write_text('#!/usr/bin/env python3\nimport sys\nprint('+repr(json.dumps({'config':config}))+')\n')
                consumer.chmod(0o755)
                expected=root/'tree.json';expected.write_text('{}')
                expected_config=root/'config.json';expected_config.write_text(json.dumps(fixed))
                args=SimpleNamespace(scratch_parent=scratch,progress_timing=False,config=None,input='fixture',
                    timing_only=True,consumer=str(consumer),fsck='/bin/true',fsync=False,expected=expected,
                    expected_config=expected_config,keep_artifacts=False)
                verified={}
                with mock.patch.object(bench_flatten,'verify_contents') as tree_check:
                    if label != 'valid':
                        with self.assertRaisesRegex(RuntimeError,'runtime config differs'):
                            bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,verified)
                        self.assertFalse(verified)
                        self.assertFalse(list(results.glob('*/result.json')))
                        tree_check.assert_not_called()
                    else:
                        result=bench_flatten.one(args,('fixture',str(producer),'/bin/true'),1,results,verified)
                        self.assertEqual(result['consumer'],str(consumer.resolve()))
                        self.assertEqual(result['consumer_sha256'],bench_flatten.file_digest(consumer))
                        self.assertEqual(result['fsck'],str(pathlib.Path('/bin/true').resolve()))
                        self.assertEqual(result['fsck_sha256'],bench_flatten.file_digest('/bin/true'))
                        self.assertEqual(result['expected_config_sha256'],bench_flatten.file_digest(expected_config))
                        self.assertEqual(json.loads(pathlib.Path(result['runtime_config_verification']).read_text())['config'],fixed)
                        self.assertTrue(verified)
                        tree_check.assert_called_once()
                self.assertEqual(list(scratch.iterdir()),[])

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
readonly=scratch/'readonly';readonly.mkdir()
(readonly/'payload').write_bytes(b'cancelled')
readonly.chmod(0o555)
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
                       '--consumer', '/bin/true', '--fsck', '/bin/true', '--expected', str(expected), '--expected-config', str(expected),
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
