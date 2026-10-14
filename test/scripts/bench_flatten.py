#!/usr/bin/env python3
"""Alternate prebuilt flatten producers with a real stdout pipe and current reader.

Inputs are immutable and outside scratch. Only files created beneath output-root
are removed. Native/Go process memory, private scratch blocks and deleted-open
files are sampled; receiver P and its optional fsync are reported separately.
This host harness does not measure Guest ext4 or Host COW and is not VM evidence.
"""
import argparse
import contextlib
import ctypes
import hashlib
import json
import os
import pathlib
import resource
import re
import select
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time


def live_task_stats(process, expected=None):
    # Threads can unshare FD/FS tables, and a zombie leader may still have
    # live siblings. A denied task inspection is an incomplete observation.
    def identity():
        fields = (process / 'stat').read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()
        return fields[19], fields[3]  # start time and session
    tasks = []
    try:
        before = identity()
        if expected is not None and before != expected:
            return []
        for task in (process / 'task').iterdir():
            try:
                fields = (task / 'stat').read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()
                if fields[0] not in ('Z', 'X', 'x'):
                    tasks.append((task, fields))
            except (FileNotFoundError, ProcessLookupError):
                pass
        if identity() != before:
            return []
    except (FileNotFoundError, ProcessLookupError):
        return []
    return tasks


def process_tree(root, leader_start=None):
    # Minimal Guest/host kernels need not expose task/TID/children. Build the
    # descendant set from procfs PPIDs instead of silently missing subprocesses.
    rows, children, owned, exited = {}, {}, set(), {}
    page_size = os.sysconf('SC_PAGE_SIZE')
    for path in pathlib.Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = path.read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()
            pid, parent = int(path.parent.name), int(fields[1])
            if fields[0] not in ('Z', 'X', 'x'):
                rows[pid] = int(fields[21]) * page_size, (fields[19], fields[3])
            else:
                # Inspect siblings only for owned groups during traversal.
                # Keep parent edges even when the entire group has exited.
                exited[pid] = path.parent, fields[19], fields[3]
            children.setdefault(parent, []).append(pid)
            if leader_start is not None:
                if pid == root and fields[19] != leader_start:
                    raise RuntimeError('producer PID was reused; refusing resource sample')
                if int(fields[3]) == root and int(fields[19]) >= int(leader_start):
                    owned.add(pid)
        except (FileNotFoundError, ProcessLookupError):
            pass
    pending, result, seen = [root, *owned], [], set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if pid in rows:
            rss, identity = rows[pid]
            result.append((pid, rss, identity))
        elif pid in exited:
            process, start, session = exited[pid]
            tasks = live_task_stats(process, (start, session))
            if tasks:
                # Threads share an address space; count one live task's RSS.
                result.append((pid, int(tasks[0][1][21]) * page_size, (start, session)))
        pending.extend(children.get(pid, []))
    return result


def directory_identity(info):
    return info.st_dev, info.st_ino


def open_tree_directory(root_fd, parts, repair=False):
    # Reopen from the anchored root one component at a time. Pending entries
    # retain identities, not ancestor FDs or Python recursion frames.
    current = os.dup(root_fd)
    try:
        for name, expected in parts:
            held = os.open(name, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=current)
            try:
                info = os.fstat(held)
                if directory_identity(info) != expected:
                    raise FileNotFoundError('scratch directory changed during traversal')
                if repair and stat.S_IMODE(info.st_mode) != 0o700:
                    # Writers have joined; chmod this bound owned directory,
                    # including mode-000 directories, without following entries.
                    os.chmod(f'/proc/self/fd/{held}', 0o700)
                child = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC, dir_fd=held)
            finally:
                os.close(held)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def task_scan_unchanged(process, identity, tasks):
    # New siblings may own private FD/FS tables too. Re-enumerate the complete
    # live set, including identities, instead of checking only captured rows.
    current = live_task_stats(process, identity)
    def identities(rows):
        return {(task, fields[19], fields[3]) for task, fields in rows}
    return bool(current) and identities(current) == identities(tasks)


def scratch_usage(scratch, processes):
    objects = {}
    try:
        root_fd = os.open(scratch, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        root_fd = None
    if root_fd is not None:
        try:
            pending = [()]
            while pending:
                parts = pending.pop()
                try:
                    directory_fd = open_tree_directory(root_fd, parts)
                except (FileNotFoundError, NotADirectoryError):
                    continue
                try:
                    info = os.fstat(directory_fd)
                    objects[directory_identity(info)] = (info.st_blocks * 512, False)
                    with os.scandir(directory_fd) as entries:
                        for entry in entries:
                            try:
                                info = entry.stat(follow_symlinks=False)
                            except FileNotFoundError:
                                continue
                            key = directory_identity(info)
                            objects[key] = (info.st_blocks * 512, False)
                            if stat.S_ISDIR(info.st_mode):
                                pending.append(parts + ((entry.name, key),))
                finally:
                    os.close(directory_fd)
        finally:
            os.close(root_fd)
    deleted = set()
    def account_reference(reference, found, removed):
        try:
            # Bind the referenced object before classifying its path or stats:
            # the producer may close/reuse an FD or change cwd/root meanwhile.
            bound = os.open(reference, os.O_PATH | os.O_CLOEXEC)
            try:
                target = os.readlink(f'/proc/self/fd/{bound}')
                st = os.fstat(bound)
            finally:
                os.close(bound)
            root_name = str(scratch)
            if target.startswith(root_name + '/') or target in (root_name, root_name + ' (deleted)'):
                # A retained, removed root has no trailing slash. Do not
                # confuse a live sibling with a literal suffix.
                if target == root_name + ' (deleted)' and st.st_nlink != 0:
                    return
                key = st.st_dev, st.st_ino
                found[key] = (st.st_blocks * 512, st.st_nlink == 0)
                if st.st_nlink == 0:
                    removed.add(key)
        except (FileNotFoundError, ProcessLookupError):
            pass

    # File and filesystem tables can differ between threads. Inspect all live
    # tasks, retaining the common inode key for shared tables and references.
    for pid, _, identity in processes:
        process = pathlib.Path('/proc') / str(pid)
        tasks = live_task_stats(process, identity)
        found, removed, failure = {}, set(), None
        try:
            for task, _ in tasks:
                for name in ['cwd', 'root']:
                    account_reference(task / name, found, removed)
                try:
                    for fd in (task / 'fd').iterdir():
                        account_reference(fd, found, removed)
                except (FileNotFoundError, ProcessLookupError):
                    pass
            # Mappings are shared by the thread group; one live task suffices.
            mappings = None
            for task, _ in tasks:
                try:
                    mappings = (task / 'maps').open(errors='surrogateescape')
                    break
                except (FileNotFoundError, ProcessLookupError):
                    pass
            if mappings is not None:
                try:
                    with mappings:
                        for line in mappings:
                            fields = line.rstrip('\n').split(None, 5)
                            if len(fields) < 6 or fields[4] == '0':
                                continue
                            major, minor = (int(part, 16) for part in fields[3].split(':'))
                            key = os.makedev(major, minor), int(fields[4])
                            if key in objects or key in found:
                                continue
                            shown = re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), fields[5])
                            if not any(name.startswith(str(scratch) + '/') for name in (fields[5], shown)):
                                continue
                            # map_files is exposed by direct TID, not task/TID.
                            reference = pathlib.Path('/proc') / task.name / 'map_files' / fields[0]
                            try:
                                target = os.readlink(reference)
                                try:
                                    info = os.stat(target)
                                except FileNotFoundError:
                                    info = None
                                if info is not None and directory_identity(info) == key:
                                    found[key] = (info.st_blocks * 512, False)
                                else:
                                    raise RuntimeError('unmeasured scratch mapping retained by owned process')
                            except (FileNotFoundError, ProcessLookupError):
                                pass
                except (FileNotFoundError, ProcessLookupError):
                    pass
        except (OSError, RuntimeError) as error:
            failure = error
        # Reads above still resolve procfs paths. Only merge their allocations
        # (or report their errors) if the same group/tasks survived the scan.
        if not task_scan_unchanged(process, identity, tasks):
            continue
        if failure is not None:
            raise failure
        objects.update(found)
        deleted.update(removed)
    return {'scratch_blocks': sum(v[0] for v in objects.values()),
            'scratch_inodes': len(objects), 'deleted_open_files': len(deleted),
            'deleted_open_blocks': sum(objects[k][0] for k in deleted)}


def file_digest(path):
    with open(path, 'rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def producer_config(path):
    # Inputs remain caller-owned and immutable. Record the bytes and identity
    # without saving config contents or including reads in producer timing.
    if not path:
        return None
    path = pathlib.Path(path).resolve(strict=True)
    def identity(info):
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
    with path.open('rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError('producer config must be a regular file')
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
        after = os.fstat(source.fileno())
    if identity(before) != identity(after) or identity(after) != identity(path.stat()):
        raise RuntimeError('producer config changed while being read')
    return {'path': str(path), 'sha256': digest, 'identity': identity(after)}


@contextlib.contextmanager
def child_subreaper():
    # This private Linux harness adopts orphaned helpers so wait accounts their
    # CPU/I/O. Never waitpid(-1): unrelated children remain caller-owned.
    # Check the harness's identity-bound cleanup support before launching any
    # producer. Unsupported hosts fail without leaving work to terminate.
    fd = os.pidfd_open(os.getpid())
    try:
        signal.pidfd_send_signal(fd, 0)
        try:
            os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG)
        except ChildProcessError:
            pass
    finally:
        os.close(fd)
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    def prctl(option, argument):
        if libc.prctl(option, argument, 0, 0, 0) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
    prctl(37, ctypes.byref(previous))  # PR_GET_CHILD_SUBREAPER
    prctl(36, 1)  # PR_SET_CHILD_SUBREAPER, before producer launch
    try:
        yield
    finally:
        prctl(36, previous.value)


def signal_owned_process(path, observed, producer_pid, sig):
    # A numeric PID/PGID can be reused after enumeration. Bind before the
    # second identity check and use this handle for both signaling and reaping.
    try:
        pid = int(path.parent.name)
        fd = os.pidfd_open(pid)
        try:
            fields = path.read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()
            identity = fields[19], fields[3]
            if identity != (observed[19], observed[3]):
                return False
            if pid != producer_pid and int(fields[1]) == os.getpid():
                try:
                    if os.waitid(os.P_PIDFD, fd, os.WEXITED | os.WNOHANG) is not None:
                        return False
                except ChildProcessError:
                    pass
            if fields[0] in ('Z', 'X', 'x') and not live_task_stats(path.parent, identity):
                return False
            if sig is not None:
                signal.pidfd_send_signal(fd, sig)
            return True
        finally:
            os.close(fd)
    except (FileNotFoundError, ProcessLookupError):
        return False


def stop_owned_group(producer, leader_start):
    def active_members(sig):
        active = 0
        for path in pathlib.Path('/proc').glob('[0-9]*/stat'):
            try:
                fields = path.read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()
                if int(path.parent.name) == producer.pid and fields[19] != leader_start:
                    raise RuntimeError('producer PID was reused; refusing session termination')
                if int(fields[3]) == producer.pid:
                    if int(fields[2]) <= 1 or int(fields[19]) < int(leader_start):
                        raise RuntimeError('unexpected member of owned producer session')
                    active += signal_owned_process(path, fields, producer.pid, sig)
            except (FileNotFoundError, ProcessLookupError):
                pass
        return active

    for sig, seconds in [(signal.SIGTERM, 10), (signal.SIGKILL, 5)]:
        deadline = time.monotonic() + seconds
        while True:
            active = active_members(sig)
            if not active and producer.poll() is not None:
                producer.wait()
                if not active_members(None):
                    return
                continue
            producer.poll()
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
    raise RuntimeError('owned producer session did not stop; scratch retained')


def tree_contents(tree):
    actual, links = {}, {}
    pending = [tree]
    while pending:
        path = pending.pop()
        name = str(path.relative_to(tree))
        info = path.lstat()
        row = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
               'mtime_ns': info.st_mtime_ns}
        if stat.S_ISREG(info.st_mode):
            row.update(type='file', size=info.st_size, sha256=file_digest(path))
        elif stat.S_ISDIR(info.st_mode):
            row.update(type='directory')
            # iterdir reports unreadable directories instead of silently
            # omitting descendants; explicit pending paths avoid rglob's
            # recursive implementation on supported Python 3.11.
            pending.extend(path.iterdir())
        elif stat.S_ISLNK(info.st_mode):
            row.update(type='symlink', target=os.readlink(path))
        else:
            row.update(type='special', kind=stat.S_IFMT(info.st_mode), device=info.st_rdev)
        if not stat.S_ISDIR(info.st_mode):
            links.setdefault((info.st_dev, info.st_ino), []).append(name)
        actual[name] = row
    for names in links.values():
        canonical = min(names)
        for name in names:
            if name != canonical:
                actual[name]['hardlink_to'] = canonical
    return actual


def remove_verification_tree(tree):
    # All writers and metadata checks have finished. Only this owned tree is
    # removed; pending traversal state stores identities rather than open FDs.
    info = tree.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise RuntimeError('verification root is not a directory')
    root_identity = directory_identity(info)
    held = os.open(tree, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if directory_identity(os.fstat(held)) != root_identity:
            raise RuntimeError('verification root changed')
        os.chmod(f'/proc/self/fd/{held}', 0o700)
        root_fd = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC, dir_fd=held)
    finally:
        os.close(held)
    try:
        pending = [((), False)]
        while pending:
            parts, visited = pending.pop()
            if visited:
                if parts:
                    parent = open_tree_directory(root_fd, parts[:-1], repair=True)
                    try:
                        name, expected = parts[-1]
                        if directory_identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != expected:
                            raise RuntimeError('verification directory changed')
                        os.rmdir(name, dir_fd=parent)
                    finally:
                        os.close(parent)
                continue
            directory_fd = open_tree_directory(root_fd, parts, repair=True)
            try:
                pending.append((parts, True))
                with os.scandir(directory_fd) as entries:
                    for entry in entries:
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            pending.append((parts + ((entry.name, directory_identity(info)),), False))
                        else:
                            os.unlink(entry.name, dir_fd=directory_fd)
            finally:
                os.close(directory_fd)
        if directory_identity(tree.lstat()) != root_identity:
            raise RuntimeError('verification root changed')
        os.rmdir(tree)
    finally:
        os.close(root_fd)


def read_progress_stream(stream, output, events, started, abort):
    # Poll an owned descriptor so failed writer teardown can stop this reader
    # even when a surviving process still has the write end open.
    pending = bytearray()
    fd = stream.fileno()
    os.set_blocking(fd, False)
    try:
        while not abort.is_set():
            if not select.select([fd], [], [], 0.1)[0]:
                continue
            try:
                block = os.read(fd, 65536)
            except BlockingIOError:
                continue
            if not block:
                break
            output.write(block)
            pending.extend(block)
            while pending:
                newline = pending.find(b'\n', 0, 65536)
                length = newline+1 if newline >= 0 else min(len(pending), 65536)
                if newline < 0 and length < 65536:
                    break
                line = pending[:length]
                del pending[:length]
                events.append({'seconds': time.monotonic()-started,
                               'message': line[:8192].decode(errors='replace').rstrip()})
        if pending:
            events.append({'seconds': time.monotonic()-started,
                           'message': pending[:8192].decode(errors='replace').rstrip()})
    finally:
        stream.close()


def verify_contents(args, artifact, run):
    # This extra extraction is outside the timed producer/receiver interval.
    # Its raw file and extracted tree are evidence scratch, never producer peak.
    raw, tree = run / 'verify.erofs', run / 'verify-root'
    expected = json.loads(args.expected.read_text())
    try:
        subprocess.run([args.consumer, 'tar', 'extract', '-f', str(artifact),
                        '--dense', '--no-chown', 'image:' + str(raw)], check=True,
                       stdout=subprocess.DEVNULL)
        tree.mkdir()
        with (run / 'content-check.log').open('wb') as output:
            subprocess.run([args.fsck, '--extract=' + str(tree), str(raw)],
                           stdout=output, stderr=subprocess.STDOUT, check=True)
        actual = tree_contents(tree)
        if actual != expected:
            (run / 'actual-content.json').write_text(json.dumps(actual, indent=2) + '\n')
            raise RuntimeError('extracted content or metadata differs from fixed fixture')
    finally:
        raw.unlink(missing_ok=True)
        if tree.exists():
            remove_verification_tree(tree)


def one(args, variant, trial, result_root, verified):
    with child_subreaper():
        return measure_one(args, variant, trial, result_root, verified)



def write_all(output, block):
    pending = memoryview(block)
    while pending:
        try:
            written = output.write(pending)
        except InterruptedError:
            continue
        if written is None or written <= 0 or written > len(pending):
            raise OSError('artifact receiver made no valid write progress')
        pending = pending[written:]


def measure_one(args, variant, trial, result_root, verified):
    name, cli, mkfs = variant
    config = producer_config(args.config or os.environ.get('FLATTEN_CONFIG'))
    run = result_root / f'{trial}-{name}'
    run.mkdir()
    scratch = pathlib.Path(tempfile.mkdtemp(prefix='flatten-bench-', dir=args.scratch_parent)).resolve()
    artifact = run / 'image.artifact'
    baseline_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    samples, failures = [], []
    done = threading.Event()
    command = [cli, 'export', '--tmpdir', str(scratch), '--output', '-']
    if not args.progress_timing:
        command.append('--no-progress')
    if config:
        command += ['--config', config['path']]
    command.append(args.input)
    environment = dict(os.environ, MKFS_EROFS_PATH=mkfs, TMPDIR=str(scratch))
    stopped = False
    producer = None
    leader_start = None
    sampler = None
    stderr_reader = None
    progress_abort = threading.Event()
    progress_events = []
    first_output_seconds = None
    output_read = None
    try:
        with (run / 'producer.stderr').open('wb') as errors:
            producer = subprocess.Popen(command, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE if args.progress_timing else errors,
                                        env=environment, start_new_session=True)
            if args.progress_timing:
                def read_progress():
                    try:
                        read_progress_stream(producer.stderr, errors, progress_events, started, progress_abort)
                    except Exception as error:
                        failures.append('progress reader: '+str(error))
                stderr_reader = threading.Thread(target=read_progress)
                stderr_reader.start()
            leader_start = (pathlib.Path('/proc') / str(producer.pid) / 'stat').read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()[19]
            if not stat.S_ISFIFO(os.fstat(producer.stdout.fileno()).st_mode):
                raise RuntimeError('producer output is not a real pipe')
            def monitor():
                while not done.is_set():
                    try:
                        processes = process_tree(producer.pid, leader_start)
                        samples.append(dict(seconds=time.monotonic()-started,
                                            processes=len(processes),
                                            process_rss=sum(rss for _, rss, _ in processes),
                                            **scratch_usage(scratch, processes)))
                    except Exception as error:
                        failures.append(str(error))
                        return
                    done.wait(args.interval)
            # Resource scans touch every selected inode and contend with the
            # Python pipe receiver. Use a separate timing pass for wide trees;
            # its result explicitly has no sampled resource measurements.
            if not args.timing_only:
                sampler = threading.Thread(target=monitor)
                sampler.start()
            digest, size = hashlib.sha256(), 0
            try:
                with artifact.open('xb', buffering=0) as output:
                    # read1 observes the first available pipe bytes for phase
                    # diagnosis, without waiting for a full MiB or carrier EOF.
                    output_read = producer.stdout.read1 if args.progress_timing else producer.stdout.read
                    while block := output_read(1 << 20):
                        if first_output_seconds is None:
                            first_output_seconds = time.monotonic()-started
                        write_all(output, block)
                        digest.update(block)
                        size += len(block)
                code = producer.wait()
                eof = time.monotonic()
            finally:
                producer.stdout.close()
                try:
                    stop_owned_group(producer, leader_start)
                    stopped = True
                finally:
                    done.set()
                    if sampler is not None:
                        sampler.join()
                    if stderr_reader is not None:
                        if not stopped:
                            progress_abort.set()
                        stderr_reader.join()
        if code:
            raise RuntimeError(f'{name}: producer exit {code}; see {run / "producer.stderr"}')
        if failures:
            raise RuntimeError(f'measurement failed: {failures}')
        if not args.timing_only:
            if not samples:
                raise RuntimeError('resource pass produced no samples')
            if not any(sample['processes'] for sample in samples):
                raise RuntimeError('resource pass never observed an owned process; memory measurement is invalid')
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        validation_started = time.monotonic()
        with (run / 'consumer.json').open('wb') as output:
            subprocess.run([args.consumer, 'info', '--json', str(artifact)], stdout=output, check=True)
        # Compare the appended config before either acceptance or caching a
        # verified artifact identity. A valid EROFS tree alone is insufficient.
        info = json.loads((run / 'consumer.json').read_text())
        expected_config = json.loads(args.expected_config.read_text())
        if 'config' not in info or info['config'] != expected_config:
            raise RuntimeError('runtime config differs from fixed fixture')
        accepted = time.monotonic()
        fsync_seconds = 0
        if args.fsync:
            sync_start = time.monotonic()
            with artifact.open('rb') as data:
                os.fsync(data.fileno())
            fsync_seconds = time.monotonic()-sync_start
        if list(scratch.iterdir()):
            raise RuntimeError(f'producer left scratch objects: {scratch}')
        identity = digest.hexdigest()
        if identity not in verified:
            verify_contents(args, artifact, run)
        if config:
            try:
                unchanged = producer_config(config['path']) == config
            except OSError as error:
                raise RuntimeError('producer config changed or became unreadable') from error
            if not unchanged:
                raise RuntimeError('producer config changed during measurement')
        if identity not in verified:
            verified[identity] = str(run / 'content-check.log')
        result = {'variant': name, 'trial': trial, 'command': command,
                  'producer_config': config,
                  # Keep historical field names/values; make the units explicit
                  # for both sampled rows and their reported peak fields.
                  'metric_units': {'artifact_blocks': 'bytes', 'scratch_blocks': 'bytes',
                                   'deleted_open_blocks': 'bytes', 'process_rss': 'bytes',
                                   'child_io_input_512blocks': '512-byte blocks',
                                   'child_io_output_512blocks': '512-byte blocks'},
                  'producer_sha256': file_digest(cli),
                  'mkfs_sha256': file_digest(mkfs),
                  'consumer': str(pathlib.Path(args.consumer).resolve(strict=True)),
                  'consumer_sha256': file_digest(args.consumer),
                  'fsck': str(pathlib.Path(args.fsck).resolve(strict=True)),
                  'fsck_sha256': file_digest(args.fsck),
                  'artifact_sha256': digest.hexdigest(), 'artifact_bytes': size,
                  'artifact_blocks': artifact.stat().st_blocks*512,
                  'producer_eof_seconds': eof-started, 'accepted_seconds': accepted-started,
                  'first_output_seconds': first_output_seconds if args.progress_timing else None,
                  'progress_events': progress_events,
                  'post_eof_teardown_seconds': validation_started-eof,
                  'receiver_validation_seconds': accepted-validation_started,
                  'receiver_fsync_seconds': fsync_seconds,
                  'child_cpu_user_seconds': usage.ru_utime-baseline_usage.ru_utime,
                  'child_cpu_system_seconds': usage.ru_stime-baseline_usage.ru_stime,
                  'child_io_input_512blocks': usage.ru_inblock-baseline_usage.ru_inblock,
                  'child_io_output_512blocks': usage.ru_oublock-baseline_usage.ru_oublock,
                  'peak': {key: None if args.timing_only else max((s[key] for s in samples), default=0) for key in
                           ['process_rss','scratch_blocks','scratch_inodes','deleted_open_files','deleted_open_blocks']},
                  'sampling_interval_seconds': None if args.timing_only else args.interval, 'scratch_clean': True,
                  'timing_only': args.timing_only,
                  'content_verification': verified[identity],
                  'expected_content_sha256': file_digest(args.expected),
                  'runtime_config_verification': str(run / 'consumer.json'),
                  'expected_config_sha256': file_digest(args.expected_config),
                  'measurement_scope': ('timing only; no sampled scratch or process RSS; receiver P separate' if args.timing_only else 'host producer private scratch and sampled process RSS; receiver P separate; rusage I/O may be unavailable; not Guest ext4 or Host COW')}
        (run / 'samples.json').write_text(json.dumps(samples)+'\n')
        (run / 'result.json').write_text(json.dumps(result, indent=2)+'\n')
        if not args.keep_artifacts:
            artifact.unlink()
        return result
    finally:
        # Cover errors between Popen and the reader loop as well as cancellation.
        # Wait for owned writers before removing their scratch.
        if producer is not None and not stopped:
            if leader_start is None:
                leader_start = (pathlib.Path('/proc') / str(producer.pid) / 'stat').read_text(encoding='utf-8', errors='surrogateescape').rsplit(') ', 1)[1].split()[19]
            try:
                stop_owned_group(producer, leader_start)
                stopped = True
            finally:
                done.set()
                if sampler is not None:
                    sampler.join()
                producer.stdout.close()
                if stderr_reader is not None:
                    if not stopped:
                        progress_abort.set()
                    stderr_reader.join()
        if stopped or producer is None:
            primary = sys.exc_info()[1]
            try:
                remove_verification_tree(scratch)
            except Exception as cleanup_error:
                if primary is None:
                    raise
                primary.add_note(f'owned scratch cleanup also failed: {cleanup_error}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant', action='append', required=True, help='NAME,CLI,MKFS (prebuilt, native)')
    parser.add_argument('--input', required=True, help='fixed Registry digest, directory or seekable Docker-save file')
    parser.add_argument('--consumer', required=True, help='current flatten-ctl')
    parser.add_argument('--config', type=pathlib.Path)
    parser.add_argument('--expected', type=pathlib.Path, required=True, help='fixed extracted content and metadata JSON')
    parser.add_argument('--expected-config', type=pathlib.Path, required=True, help='fixed projected runtime config JSON; null only when no appended config is expected')
    parser.add_argument('--fsck', required=True, help='trusted native fsck.erofs with extraction support')
    parser.add_argument('--scratch-parent', required=True, type=pathlib.Path)
    parser.add_argument('--output-root', required=True, type=pathlib.Path)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--interval', type=float, default=0.1)
    parser.add_argument('--fsync', action='store_true')
    parser.add_argument('--progress-timing', action='store_true', help='timestamp existing CLI progress lines for stage attribution')
    parser.add_argument('--keep-artifacts', action='store_true')
    parser.add_argument('--timing-only', action='store_true', help='omit intrusive inode/FD/RSS sampling; run resource measurements separately')
    args = parser.parse_args()
    variants = [value.split(',') for value in args.variant]
    if args.trials < 1 or not 0.01 <= args.interval <= 1:
        parser.error('positive trials and a 0.01..1 second sampling interval are required')
    if any(len(v) != 3 or not v[0].replace('-', '').isalnum() for v in variants):
        parser.error('invalid variant')
    if len({v[0] for v in variants}) != len(variants):
        parser.error('duplicate variant')
    for v in variants:
        for index in [1,2]:
            v[index] = str(pathlib.Path(v[index]).resolve(strict=True))
    args.consumer = str(pathlib.Path(args.consumer).resolve(strict=True))
    args.fsck = str(pathlib.Path(args.fsck).resolve(strict=True))
    args.expected = args.expected.resolve(strict=True)
    args.expected_config = args.expected_config.resolve(strict=True)
    expected_config = json.loads(args.expected_config.read_text())
    if expected_config is not None and not isinstance(expected_config, dict):
        parser.error("expected runtime config must be an object or null")
    args.scratch_parent = args.scratch_parent.resolve(strict=True)
    args.output_root = args.output_root.resolve()
    local_input = pathlib.Path(args.input)
    if local_input.exists():
        local_input = local_input.resolve(strict=True)
        args.input = str(local_input)
        for mutable in (args.output_root, args.scratch_parent):
            if local_input.is_relative_to(mutable) or mutable.is_relative_to(local_input):
                parser.error('input and mutable measurement directories must be disjoint')
    elif '@sha256:' not in args.input:
        parser.error('Registry measurements require a fixed digest')
    args.output_root.mkdir(parents=True, exist_ok=False)
    results, verified = [], {}
    for trial in range(1, args.trials+1):
        # Reverse adjacent rounds, keeping each variant on the same resources.
        for variant in variants if trial % 2 else reversed(variants):
            result = one(args, variant, trial, args.output_root, verified)
            results.append(result)
            print(json.dumps(result), flush=True)
    if len({r['artifact_sha256'] for r in results}) != 1:
        raise RuntimeError('normal-corpus artifact identities differ')
    (args.output_root / 'results.json').write_text(json.dumps(results,indent=2)+'\n')


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    main()
