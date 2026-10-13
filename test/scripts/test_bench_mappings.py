"""Actual mapping and non-regular hard-link measurement regressions."""
import contextlib
import ctypes
import hashlib
import io
import json
import os
import pathlib
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
                    bench_flatten.scratch_usage(root, [(os.getpid(), 0)])
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
                        sample = bench_flatten.scratch_usage(root, [(os.getpid(), 0)])
                        self.assertEqual(sample['scratch_blocks'], root.stat().st_blocks * 512 + blocks)
                        self.assertEqual(sample['scratch_inodes'], 2)
                        self.assertEqual(sample['deleted_open_files'], 0 if linked else 1)

    def test_mapping_created_after_tree_walk_is_counted(self):
        with tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory:
            root = pathlib.Path(directory)
            with mapped_file(root/'literal (deleted)') as (file, _):
                blocks = os.fstat(file.fileno()).st_blocks * 512
                file.close()
                with mock.patch.object(bench_flatten.os, 'walk', return_value=[]):
                    sample = bench_flatten.scratch_usage(root, [(os.getpid(), 0)])
                self.assertEqual(sample['scratch_blocks'], blocks)

    def test_maps_permission_error_fails_but_disappearance_is_tolerated(self):
        with tempfile.TemporaryDirectory(prefix='mapped-scratch-') as directory:
            root = pathlib.Path(directory)
            target = pathlib.Path('/proc')/str(os.getpid())/'maps'
            original = pathlib.Path.open
            for error in [PermissionError, FileNotFoundError, ProcessLookupError]:
                def open_path(path, *args, **kwargs):
                    if path == target:
                        raise error('maps inspection')
                    return original(path, *args, **kwargs)
                with self.subTest(error=error), mock.patch.object(pathlib.Path, 'open', new=open_path):
                    if error is PermissionError:
                        with self.assertRaises(PermissionError):
                            bench_flatten.scratch_usage(root, [(os.getpid(), 0)])
                    else:
                        self.assertEqual(bench_flatten.scratch_usage(root, [(os.getpid(), 0)])['deleted_open_files'], 0)

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
                sample = bench_flatten.scratch_usage(root, [(os.getpid(), 0)])
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
