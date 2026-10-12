#!/usr/bin/env python3
"""Alternate prebuilt flatten producers with a real stdout pipe and current reader.

Inputs are immutable and outside scratch. Only files created beneath output-root
are removed. Native/Go process memory, private scratch blocks and deleted-open
files are sampled; receiver P and its optional fsync are reported separately.
This host harness does not measure Guest ext4 or Host COW and is not VM evidence.
"""
import argparse
import hashlib
import json
import os
import pathlib
import resource
import signal
import stat
import shutil
import subprocess
import tempfile
import threading
import time


def process_tree(root):
    pending, result, seen = [root], [], set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            base = pathlib.Path('/proc') / str(pid)
            stat = (base / 'stat').read_text().split(') ', 1)[1].split()
            result.append((pid, int(stat[21]) * os.sysconf('SC_PAGE_SIZE')))
            for task in (base / 'task').iterdir():
                pending.extend(int(p) for p in (task / 'children').read_text().split())
        except (FileNotFoundError, ProcessLookupError):
            pass
    return list(dict(result).items())


def scratch_usage(scratch, processes):
    objects = {}
    for directory, _, files in os.walk(scratch):
        for name in ['.', *files]:
            try:
                st = (pathlib.Path(directory) / name).lstat()
                objects[(st.st_dev, st.st_ino)] = (st.st_blocks * 512, False)
            except FileNotFoundError:
                pass
    deleted = set()
    for pid, _ in processes:
        try:
            for fd in (pathlib.Path('/proc') / str(pid) / 'fd').iterdir():
                try:
                    target = os.readlink(fd)
                    if target.startswith(str(scratch) + '/'):
                        st = fd.stat()
                        key = st.st_dev, st.st_ino
                        objects[key] = (st.st_blocks * 512, target.endswith(' (deleted)'))
                        if target.endswith(' (deleted)'):
                            deleted.add(key)
                except (FileNotFoundError, PermissionError):
                    pass
        except (FileNotFoundError, PermissionError):
            pass
    return {'scratch_blocks': sum(v[0] for v in objects.values()),
            'scratch_inodes': len(objects), 'deleted_open_files': len(deleted),
            'deleted_open_blocks': sum(objects[k][0] for k in deleted)}


def file_digest(path):
    with open(path, 'rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def stop_owned_group(producer, leader_start):
    def active_members():
        active = []
        for path in pathlib.Path('/proc').glob('[0-9]*/stat'):
            try:
                fields = path.read_text().rsplit(') ', 1)[1].split()
                if int(path.parent.name) == producer.pid and fields[19] != leader_start:
                    raise RuntimeError('producer PID was reused; refusing group termination')
                if int(fields[2]) == producer.pid and fields[0] != 'Z':
                    if int(fields[3]) != producer.pid or int(fields[19]) < int(leader_start):
                        raise RuntimeError('unexpected member of owned producer session')
                    active.append(path.parent.name)
            except (FileNotFoundError, ProcessLookupError):
                pass
        return active

    for sig, seconds in [(signal.SIGTERM, 10), (signal.SIGKILL, 5)]:
        if not active_members():
            producer.wait()
            return
        try:
            os.killpg(producer.pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + seconds
        while active_members() and time.monotonic() < deadline:
            producer.poll()
            time.sleep(0.05)
    raise RuntimeError('owned producer session did not stop; scratch retained')


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
        actual = {}
        for path in tree.rglob('*'):
            info = path.lstat()
            row = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid}
            if stat.S_ISREG(info.st_mode):
                row.update(type='file', size=info.st_size, sha256=file_digest(path))
            elif stat.S_ISDIR(info.st_mode):
                row.update(type='directory')
            elif stat.S_ISLNK(info.st_mode):
                row.update(type='symlink', target=os.readlink(path))
            else:
                row.update(type='special', device=info.st_rdev)
            actual[str(path.relative_to(tree))] = row
        if actual != expected:
            (run / 'actual-content.json').write_text(json.dumps(actual, indent=2) + '\n')
            raise RuntimeError('extracted content or metadata differs from fixed fixture')
    finally:
        raw.unlink(missing_ok=True)
        if tree.exists():
            shutil.rmtree(tree)


def one(args, variant, trial, result_root, verified):
    name, cli, mkfs = variant
    run = result_root / f'{trial}-{name}'
    run.mkdir()
    scratch = pathlib.Path(tempfile.mkdtemp(prefix='flatten-bench-', dir=args.scratch_parent)).resolve()
    artifact = run / 'image.artifact'
    baseline_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    samples, failures = [], []
    done = threading.Event()
    command = [cli, 'export', '--no-progress', '--tmpdir', str(scratch), '--output', '-']
    if args.config:
        command += ['--config', str(args.config)]
    command.append(args.input)
    environment = dict(os.environ, MKFS_EROFS_PATH=mkfs, TMPDIR=str(scratch))
    stopped = False
    producer = None
    leader_start = None
    sampler = None
    try:
        with (run / 'producer.stderr').open('wb') as errors:
            producer = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors, env=environment, start_new_session=True)
            leader_start = (pathlib.Path('/proc') / str(producer.pid) / 'stat').read_text().rsplit(') ', 1)[1].split()[19]
            if not stat.S_ISFIFO(os.fstat(producer.stdout.fileno()).st_mode):
                raise RuntimeError('producer output is not a real pipe')
            def monitor():
                while not done.is_set():
                    try:
                        processes = process_tree(producer.pid)
                        samples.append(dict(seconds=time.monotonic()-started,
                                            process_rss=sum(rss for _, rss in processes),
                                            **scratch_usage(scratch, processes)))
                    except Exception as error:
                        failures.append(str(error))
                        return
                    done.wait(args.interval)
            sampler = threading.Thread(target=monitor)
            sampler.start()
            digest, size = hashlib.sha256(), 0
            try:
                with artifact.open('xb', buffering=0) as output:
                    while block := producer.stdout.read(1 << 20):
                        output.write(block)
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
                    sampler.join()
        if code:
            raise RuntimeError(f'{name}: producer exit {code}; see {run / "producer.stderr"}')
        if failures:
            raise RuntimeError(f'measurement failed: {failures}')
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        with (run / 'consumer.json').open('wb') as output:
            subprocess.run([args.consumer, 'info', str(artifact)], stdout=output, check=True)
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
                  'producer_sha256': file_digest(cli),
                  'mkfs_sha256': file_digest(mkfs),
                  'artifact_sha256': digest.hexdigest(), 'artifact_bytes': size,
                  'artifact_blocks': artifact.stat().st_blocks*512,
                  'producer_eof_seconds': eof-started, 'accepted_seconds': accepted-started,
                  'receiver_validation_seconds': accepted-eof, 'receiver_fsync_seconds': fsync_seconds,
                  'child_cpu_user_seconds': usage.ru_utime-baseline_usage.ru_utime,
                  'child_cpu_system_seconds': usage.ru_stime-baseline_usage.ru_stime,
                  'child_io_input_512blocks': usage.ru_inblock-baseline_usage.ru_inblock,
                  'child_io_output_512blocks': usage.ru_oublock-baseline_usage.ru_oublock,
                  'peak': {key: max((s[key] for s in samples), default=0) for key in
                           ['process_rss','scratch_blocks','scratch_inodes','deleted_open_files','deleted_open_blocks']},
                  'sampling_interval_seconds': args.interval, 'scratch_clean': True,
                  'content_verification': verified[identity],
                  'expected_content_sha256': file_digest(args.expected),
                  'measurement_scope': 'host producer private scratch and sampled process RSS; receiver P separate; rusage I/O may be unavailable; not Guest ext4 or Host COW'}
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
        if stopped or producer is None:
            shutil.rmtree(scratch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant', action='append', required=True, help='NAME,CLI,MKFS (prebuilt, native)')
    parser.add_argument('--input', required=True, help='fixed Registry digest, directory or seekable Docker-save file')
    parser.add_argument('--consumer', required=True, help='current flatten-ctl')
    parser.add_argument('--config', type=pathlib.Path)
    parser.add_argument('--expected', type=pathlib.Path, required=True, help='fixed extracted content and metadata JSON')
    parser.add_argument('--fsck', required=True, help='trusted native fsck.erofs with extraction support')
    parser.add_argument('--scratch-parent', required=True, type=pathlib.Path)
    parser.add_argument('--output-root', required=True, type=pathlib.Path)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--interval', type=float, default=0.1)
    parser.add_argument('--fsync', action='store_true')
    parser.add_argument('--keep-artifacts', action='store_true')
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
