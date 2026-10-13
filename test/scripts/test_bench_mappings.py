"""Actual mapping and non-regular hard-link measurement regressions."""
import contextlib
import ctypes
import os
import pathlib
import tempfile
import unittest
from unittest import mock

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
