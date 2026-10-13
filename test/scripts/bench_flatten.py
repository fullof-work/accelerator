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
import itertools
import json
import os
import pathlib
import resource
import re
import select
import signal
import stat
import shutil
import subprocess
import sys
import tempfile
import threading
import time


def process_tree(root, leader_start=None):
    # Minimal Guest/host kernels need not expose task/TID/children. Build the
    # descendant set from procfs PPIDs instead of silently missing subprocesses.
    rows, children, owned = {}, {}, set()
    page_size = os.sysconf('SC_PAGE_SIZE')
    for path in pathlib.Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = path.read_text().rsplit(') ', 1)[1].split()
            pid, parent = int(path.parent.name), int(fields[1])
            # Exited rows have no resident memory to measure. Keep their
            # edges below so an observed descendant is still traversed.
            if fields[0] not in ('Z', 'X', 'x'):
                rows[pid] = int(fields[21]) * page_size
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
            result.append((pid, rows[pid]))
        pending.extend(children.get(pid, []))
    return result


def scratch_usage(scratch, processes):
    objects = {}
    def scan_error(error):
        # A producer can remove its own directory between enumeration and scan.
        # Permission and I/O failures are incomplete measurements, not zero use.
        if not isinstance(error, FileNotFoundError):
            raise error

    for directory, directories, files in os.walk(scratch, onerror=scan_error):
        # os.walk lists symlinked directories but does not descend into them.
        links = [name for name in directories if (pathlib.Path(directory) / name).is_symlink()]
        for name in ['.', *files, *links]:
            try:
                st = (pathlib.Path(directory) / name).lstat()
                objects[(st.st_dev, st.st_ino)] = (st.st_blocks * 512, False)
            except FileNotFoundError:
                pass
    deleted = set()
    # A disappearing process/FD is normal. Denied inspection cannot establish
    # whether an unlinked scratch file still occupies space.
    for pid, _ in processes:
        try:
            for fd in (pathlib.Path('/proc') / str(pid) / 'fd').iterdir():
                try:
                    target = os.readlink(fd)
                    root_name = str(scratch)
                    if target.startswith(root_name + '/') or target in (root_name, root_name + ' (deleted)'):
                        st = fd.stat()
                        # A retained, removed root has no trailing slash. Do
                        # not confuse a live sibling with a literal suffix.
                        if target == root_name + ' (deleted)' and st.st_nlink != 0:
                            continue
                        key = st.st_dev, st.st_ino
                        objects[key] = (st.st_blocks * 512, st.st_nlink == 0)
                        if st.st_nlink == 0:
                            deleted.add(key)
                except (FileNotFoundError, ProcessLookupError):
                    pass
        except (FileNotFoundError, ProcessLookupError):
            pass
    # An mmap can pin an unlinked inode after its last FD closes. Do not
    # silently accept an incomplete resource sample when that inode is absent.
    for pid, _ in processes:
        try:
            with (pathlib.Path('/proc') / str(pid) / 'maps').open(errors='surrogateescape') as mappings:
                for line in mappings:
                    fields = line.rstrip('\n').split(None, 5)
                    if len(fields) < 6 or fields[4] == '0':
                        continue
                    major, minor = (int(part, 16) for part in fields[3].split(':'))
                    key = os.makedev(major, minor), int(fields[4])
                    if key in objects:
                        continue
                    # maps quotes newlines as octal; map_files gives the exact
                    # pathname for the uncommon, not-yet-accounted candidate.
                    shown = re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), fields[5])
                    if not any(name.startswith(str(scratch) + '/') for name in (fields[5], shown)):
                        continue
                    reference = pathlib.Path('/proc') / str(pid) / 'map_files' / fields[0]
                    try:
                        target = os.readlink(reference)
                        # A newly created linked file may have missed the tree
                        # walk. Only accept a stat of that same mapped inode.
                        try:
                            st = os.stat(target)
                        except FileNotFoundError:
                            st = None
                        if st is not None and (st.st_dev, st.st_ino) == key:
                            objects[key] = (st.st_blocks * 512, False)
                        else:
                            raise RuntimeError('unmeasured scratch mapping retained by owned process')
                    except (FileNotFoundError, ProcessLookupError):
                        pass  # The mapping/process disappeared during sampling.
        except (FileNotFoundError, ProcessLookupError):
            pass
    return {'scratch_blocks': sum(v[0] for v in objects.values()),
            'scratch_inodes': len(objects), 'deleted_open_files': len(deleted),
            'deleted_open_blocks': sum(objects[k][0] for k in deleted)}


def file_digest(path):
    with open(path, 'rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


@contextlib.contextmanager
def child_subreaper():
    # This private Linux harness adopts orphaned helpers so wait accounts their
    # CPU/I/O. Never waitpid(-1): unrelated children remain caller-owned.
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


def stop_owned_group(producer, leader_start):
    def active_groups():
        groups = set()
        for path in pathlib.Path('/proc').glob('[0-9]*/stat'):
            try:
                fields = path.read_text().rsplit(') ', 1)[1].split()
                if int(path.parent.name) == producer.pid and fields[19] != leader_start:
                    raise RuntimeError('producer PID was reused; refusing group termination')
                if int(fields[3]) == producer.pid:
                    group = int(fields[2])
                    if group <= 1 or int(fields[19]) < int(leader_start):
                        raise RuntimeError('unexpected member of owned producer session')
                    pid = int(path.parent.name)
                    if pid != producer.pid and int(fields[1]) == os.getpid():
                        # Only adopted members of this exact session are ours
                        # to reap. The leader remains managed by Popen.
                        try:
                            waited, _ = os.waitpid(pid, os.WNOHANG)
                            if waited:
                                continue
                        except ChildProcessError:
                            pass
                    if fields[0] != 'Z':
                        groups.add(group)
            except (FileNotFoundError, ProcessLookupError):
                pass
        return groups

    for sig, seconds in [(signal.SIGTERM, 10), (signal.SIGKILL, 5)]:
        deadline = time.monotonic() + seconds
        while True:
            groups = active_groups()
            if not groups:
                producer.wait()
                if not active_groups():
                    return
                continue
            for group in groups:
                try:
                    os.killpg(group, sig)
                except ProcessLookupError:
                    pass
            producer.poll()
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
    raise RuntimeError('owned producer session did not stop; scratch retained')


def tree_contents(tree):
    actual, links = {}, {}
    for path in itertools.chain((tree,), tree.rglob('*')):
        name = str(path.relative_to(tree))
        info = path.lstat()
        row = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
               'mtime_ns': info.st_mtime_ns}
        if stat.S_ISREG(info.st_mode):
            row.update(type='file', size=info.st_size, sha256=file_digest(path))
        elif stat.S_ISDIR(info.st_mode):
            row.update(type='directory')
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
    # All writers and metadata checks have finished. Recover directory access
    # only in this owned tree; symlink targets are never visited.
    if not stat.S_ISDIR(tree.lstat().st_mode):
        raise RuntimeError('verification root is not a directory')
    tree.chmod(0o700)
    for directory, directories, _ in os.walk(tree):
        for name in directories:
            child = pathlib.Path(directory) / name
            if stat.S_ISDIR(child.lstat().st_mode):
                os.chmod(child, 0o700, follow_symlinks=False)
    shutil.rmtree(tree)


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
    if args.config:
        command += ['--config', str(args.config)]
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
            leader_start = (pathlib.Path('/proc') / str(producer.pid) / 'stat').read_text().rsplit(') ', 1)[1].split()[19]
            if not stat.S_ISFIFO(os.fstat(producer.stdout.fileno()).st_mode):
                raise RuntimeError('producer output is not a real pipe')
            def monitor():
                while not done.is_set():
                    try:
                        processes = process_tree(producer.pid, leader_start)
                        samples.append(dict(seconds=time.monotonic()-started,
                                            processes=len(processes),
                                            process_rss=sum(rss for _, rss in processes),
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
            verified[identity] = str(run / 'content-check.log')
        result = {'variant': name, 'trial': trial, 'command': command,
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
                leader_start = (pathlib.Path('/proc') / str(producer.pid) / 'stat').read_text().rsplit(') ', 1)[1].split()[19]
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
    (args.output_root / 'results.json').write_text(json.dumps(results,indent=2)+'\n')
    if len({r['artifact_sha256'] for r in results}) != 1:
        raise RuntimeError('normal-corpus artifact identities differ')


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    main()
