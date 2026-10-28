#!/bin/bash
# e2e-requires: ["systemd"]
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
for product in cache-ctl store-ctl manifest-ctl flatten-ctl; do require_binary "$product"; done
require_command systemctl
require_command redis-server
if [ "$(id -u)" -ne 0 ]; then sudo -n true; fi
scratch=$(mktemp -d /tmp/acc-activation-ports-XXXXXX)
trap 'rm -rf "$scratch"' EXIT
E2E_PORT_LEASE_FILE="$scratch/ports"
source "$E2E_LIB/accelerator/port_lease.sh"
ports=()
for _ in $(seq 1 10); do ports+=("$(e2e_free_port)"); done

python3 - "$BIN" "${ports[@]}" <<'PY'
import contextlib
import fcntl
import hashlib
import http.client
import json
import os
import re
from pathlib import Path
import signal
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time

binary_dir = Path(sys.argv[1]).resolve()
ports = iter(sys.argv[2:])
privilege = [] if os.geteuid() == 0 else ['sudo', '-n']
sleep_binary = shutil.which('sleep')
assert sleep_binary, 'sleep executable is required'
payload = b'activation-response-' * (3 * 1024 * 1024 // 20)
key = hashlib.sha256(payload).digest()

def ctl(*args, check=True, timeout=20):
    return subprocess.run(privilege + ['systemctl', *args], check=check,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout).stdout

def wait_for(check, label):
    end = time.monotonic() + 10
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.02)
    raise AssertionError(label)

def endpoint(root, name, network):
    return str(root / (name + '.sock')) if network == 'unix' else '127.0.0.1:' + next(ports)

class Wire:
    def __init__(self, address, budget=5):
        self.deadline = time.monotonic() + budget
        self.sock = socket.socket(socket.AF_UNIX if address.startswith('/') else socket.AF_INET)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
        self.limit()
        try:
            self.sock.connect(address if address.startswith('/') else (address.split(':')[0], int(address.split(':')[1])))
        except BaseException:
            self.sock.close()
            raise
    def limit(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('total wire operation budget expired')
        self.sock.settimeout(remaining)
    def close(self):
        self.sock.close()
    def request(self, opcode, value=b''):
        self.limit()
        self.sock.sendall(struct.pack('<IBBB', 39 + len(value), opcode, 3, 0) + key + value)
    def receive(self, size, slow=False):
        data = bytearray()
        while len(data) < size:
            self.limit()
            part = self.sock.recv(min(size - len(data), 4096 if slow else 65536))
            if not part:
                raise EOFError(f'truncated response {len(data)}/{size}')
            data.extend(part)
            if slow:
                time.sleep(.001)
        return bytes(data)
    def response(self, header=None, slow=False):
        size, status, reserved, error_len = struct.unpack('<IBBH', self.receive(8) if header is None else header)
        assert 8 <= size <= 4 << 20, size
        body = self.receive(size - 8, slow)
        assert status == 0 and error_len == 0, (status, body[:200])
        return body

@contextlib.contextmanager
def owned_directory():
    root = Path(tempfile.mkdtemp(prefix='acc-activation-'))
    state = {'safe': True}
    try:
        yield root, state
    finally:
        if state['safe']:
            shutil.rmtree(root)
        else:
            print(f'Preserving task directory with unconfirmed cleanup: {root}', file=sys.stderr)

# Validate errors using the actual prebuilt executables, before systemd's valid
# descriptor matrix. Helper unit tests additionally check in-process rollback.
exec_with_fds = '''
import json, os, sys
binary, config, names, pid_mode, descriptors = sys.argv[1:]
fds = json.loads(descriptors)
for index, fd in enumerate(fds):
    os.dup2(fd, 3 + index, inheritable=True)
for fd in fds:
    os.close(fd)
env = dict(os.environ)
for name in ('LISTEN_PID', 'LISTEN_FDS', 'LISTEN_FDNAMES'):
    env.pop(name, None)
if pid_mode != 'absent':
    env.update(LISTEN_PID=str(os.getpid() if pid_mode == 'self' else 1),
               LISTEN_FDS=str(len(fds)), LISTEN_FDNAMES=names)
os.execve(binary, [binary, 'serve', '--config', config], env)
'''
with tempfile.TemporaryDirectory(prefix='acc-activation-invalid-') as raw:
    invalid_root = Path(raw)
    for network in ('tcp', 'unix'):
        with contextlib.ExitStack() as originals:
            managers = []
            for index in range(4):
                manager = originals.enter_context(socket.socket(socket.AF_UNIX if network == 'unix' else socket.AF_INET))
                manager.bind(str(invalid_root / f'{index}.sock') if network == 'unix' else ('127.0.0.1', 0))
                manager.listen()
                manager.settimeout(1)
                managers.append(manager)
            regular = originals.enter_context((invalid_root / 'regular').open('w'))
            datagram = originals.enter_context(socket.socket(socket.AF_UNIX if network == 'unix' else socket.AF_INET, socket.SOCK_DGRAM))
            connected, other = socket.socketpair()
            originals.enter_context(connected)
            originals.enter_context(other)
            for product in ('cache', 'store'):
                count = 3 if product == 'cache' else 2
                names = ['data', 'health', 'pprof'][:count]
                files = managers[:count]
                cfg = {'listen': 'fd=data'}
                if product == 'cache':
                    cfg.update(mode='local', type='embedded', health_listen='fd=health', pprof_listen='fd=pprof',
                               rocks={'path': str(invalid_root / 'must-not-open'), 'disk_bytes': '64MiB', 'mem_ratio': .05})
                else:
                    cfg.update(backend='fs', cache_listen='fd=health', fs={'root': str(invalid_root / 'must-not-open')},
                               generations={'file': {'path': str(invalid_root / 'must-not-read')}})
                cfg_file = invalid_root / (product + '.json')
                cfg_file.write_text(json.dumps(cfg))
                scenarios = [
                    ('missing-env', [], [], 'absent'),
                    ('wrong-pid', files, names, 'wrong'),
                    ('wrong-name', files, ['wrong'] + names[1:], 'self'),
                    ('missing-fd', files[:-1], names[:-1], 'self'),
                    ('extra-fd', files + [managers[count]], names + ['unused'], 'self'),
                    ('duplicate-name', files, ['data', 'data'] + names[2:], 'self'),
                    ('duplicate-socket', [files[0], files[0]] + files[2:], names, 'self'),
                    ('regular-file', [regular] + files[1:], names, 'self'),
                    ('datagram', [datagram] + files[1:], names, 'self'),
                    ('connected-stream', [connected] + files[1:], names, 'self'),
                ]
                for label, supplied, declared, pid_mode in scenarios:
                    baseline = len(os.listdir('/proc/self/fd'))
                    copies = []
                    try:
                        # Keep sources away from 3..N so remapping cannot
                        # overwrite a later source descriptor.
                        for file in supplied:
                            copies.append(fcntl.fcntl(file.fileno(), fcntl.F_DUPFD_CLOEXEC, 100))
                        result = subprocess.run([sys.executable, '-c', exec_with_fds,
                                                 str(binary_dir / (product + '-ctl')), str(cfg_file),
                                                 ':'.join(declared), pid_mode, json.dumps(copies)],
                                                pass_fds=copies, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                    finally:
                        for fd in copies:
                            os.close(fd)
                    diagnostic = result.stdout + result.stderr
                    assert result.returncode != 0 and b'activation:' in diagnostic, (label, diagnostic)
                    assert not (invalid_root / 'must-not-open').exists(), (label, 'backend created before FD rejection')
                    assert len(os.listdir('/proc/self/fd')) == baseline, (label, 'caller FD leaked')
                    # Failure must not close/shutdown the manager's original or
                    # unlink a UDS path. One connect/accept per original proves it.
                    for manager in managers:
                        address = manager.getsockname()
                        if network == 'unix':
                            assert stat.S_ISSOCK(os.stat(address).st_mode)
                        with socket.socket(manager.family) as peer:
                            peer.settimeout(1)
                            peer.connect(address)
                            accepted, _ = manager.accept()
                            accepted.close()
                print(f'PASS {product}/{network}: 10 invalid FD configurations rejected by real CLI before backend creation; caller sockets preserved', flush=True)

for backend in ('embedded', 'redis', 'store'):
    for network in ('tcp', 'unix'):
        with owned_directory() as (root, directory_state), contextlib.ExitStack() as streams:
            unit = root.name
            service = unit + '.service'
            run_directory = Path('/run') / unit
            unit_files, socket_units, children = [], [], []
            service_owned = False
            log = root / 'service.log'
            cfg_path = root / 'service.json'
            data_endpoint = endpoint(root, 'wire', network)
            entrances = {'data': data_endpoint}
            if backend == 'store':
                entrances['store'] = endpoint(root, 'store', 'unix' if network == 'tcp' else 'tcp')
                generation_file = root / 'generations'
                generation_file.write_text('G1\n')
                cfg = {'backend': 'fs', 'listen': 'fd=store', 'cache_listen': 'fd=data',
                       'fs': {'root': str(root / 'data')},
                       'generations': {'file': {'path': str(generation_file)}}}
                digest = key.hex()
                object_path = root / 'data' / 'blob' / 'G1' / digest[:2] / digest[2:4] / digest
                object_path.parent.mkdir(parents=True)
                object_path.write_bytes(payload)
                executable = binary_dir / 'store-ctl'
            else:
                entrances['health'] = endpoint(root, 'health', 'unix' if network == 'tcp' else 'tcp')
                entrances['pprof'] = endpoint(root, 'pprof', 'tcp')
                cfg = {'mode': 'shard', 'type': backend, 'listen': 'fd=data',
                       'health_listen': 'fd=health', 'pprof_listen': 'fd=pprof',
                       'freq': {'counters': '1M', 'reset_after': '100K', 'disable_eviction': True}}
                if backend == 'embedded':
                    cfg['rocks'] = {'path': str(root / 'db'), 'disk_bytes': '64MiB', 'mem_ratio': .05, 'direct_reads': False}
                else:
                    cfg['redis'] = {'endpoint': str(root / 'redis.sock'), 'get_pool': 1, 'set_pool': 1, 'timeout': '2s'}
                executable = binary_dir / 'cache-ctl'
            cfg_path.write_text(json.dumps(cfg))
            manifest_cfg = root / 'manifest.json'
            manifest_key = None
            manifest_bytes = None

            def spawn(name, args):
                directory_state['safe'] = False
                out = streams.enter_context((root / (name + '.log')).open('w'))
                proc = subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT)
                children.append(proc)
                return proc
            def show():
                out = ctl('show', service, '--property=MainPID,ControlPID,SubState,NRestarts,ActiveState')
                return dict(line.split('=', 1) for line in out.splitlines() if '=' in line)
            def text():
                return log.read_text() if log.exists() else ''
            def starts():
                return text().count('msg="' + ('store' if backend == 'store' else 'cache') + ' started"')
            def wait_started(previous):
                wait_for(lambda: int(show().get('MainPID', 0)) > 0 and starts() > previous,
                         'new instance never completed initialization')
                return int(show()['MainPID'])
            def command(*args):
                result = subprocess.run([str(x) for x in args], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, timeout=10)
                if result.returncode:
                    raise RuntimeError(f'{args}: exit {result.returncode}: '
                                       + result.stdout.decode() + result.stderr.decode())
                return result.stdout
            def check_requests():
                with contextlib.closing(Wire(data_endpoint)) as c:
                    c.request(5)
                    assert c.response() == b''
                with contextlib.closing(Wire(data_endpoint)) as c:
                    c.request(1)
                    assert c.response() == payload, 'single-attempt Get lost committed content'
                if backend == 'store':
                    target = root / 'manifest-returned'
                    command(binary_dir / 'manifest-ctl', 'get-manifest', '--manifest-config', manifest_cfg,
                            '--output', target, manifest_key)
                    assert target.read_bytes() == manifest_bytes, 'Store gRPC Get changed content'
                else:
                    health = entrances['health']
                    info = command(binary_dir / 'cache-ctl', 'info', '--endpoint',
                                   'unix://' + health if health.startswith('/') else health, '--json')
                    assert json.loads(info), 'empty Info response'
                    host, port = entrances['pprof'].split(':')
                    connection = http.client.HTTPConnection(host, int(port), timeout=3)
                    try:
                        connection.request('GET', '/debug/pprof/')
                        response = connection.getresponse()
                        assert response.status == 200 and b'profile' in response.read()
                    finally:
                        connection.close()
            def install(name, contents):
                directory_state['safe'] = False
                src, dst = root / name, Path('/run/systemd/system') / name
                assert not dst.exists(), f'unrelated unit already exists: {dst}'
                src.write_text(contents)
                subprocess.run(privilege + ['install', '-m', '0644', str(src), str(dst)], check=True)
                unit_files.append(dst)
            def cleanup():
                # Keep every cleanup attempt independent. The manager owns a
                # 90-second stop budget; do not abandon it after ctl's ordinary
                # command timeout or remove files beneath a surviving process.
                failures = []
                targets = socket_units + ([service] if service_owned else [])
                try:
                    if targets:
                        ctl('stop', *targets, timeout=100)
                except BaseException as error:
                    failures.append(str(error))
                    try:
                        if service_owned:
                            ctl('kill', '--kill-who=all', '--signal=SIGKILL', service, check=False)
                        ctl('stop', *targets, timeout=100)
                    except BaseException as error:
                        failures.append(str(error))
                for proc in reversed(children):
                    try:
                        if proc.poll() is None:
                            proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=10)
                    except BaseException as error:
                        failures.append(str(error))
                safe = all(proc.poll() is not None for proc in children)
                try:
                    if service_owned:
                        state = show()
                        safe = safe and state.get('ActiveState') in ('inactive', 'failed')
                        safe = safe and int(state['MainPID']) == 0 and int(state['ControlPID']) == 0
                    for sock_unit in socket_units:
                        state = ctl('show', sock_unit, '--property=ActiveState', '--value').strip()
                        safe = safe and state in ('inactive', 'failed')
                except BaseException as error:
                    safe = False
                    failures.append(str(error))
                if safe:
                    try:
                        if targets:
                            ctl('reset-failed', *targets, check=False)
                        if unit_files:
                            subprocess.run(privilege + ['rm', '-f', '--', *map(str, unit_files)], check=True)
                            ctl('daemon-reload')
                    except BaseException as error:
                        failures.append(str(error))
                    directory_state['safe'] = True
                else:
                    failures.append('task process/socket ownership is still live or unconfirmed')
                if failures:
                    raise RuntimeError('task cleanup failed: ' + '; '.join(failures))
            try:
                if backend == 'redis':
                    redis = spawn('redis', ['redis-server', '--port', '0', '--unixsocket', cfg['redis']['endpoint'],
                                            '--save', '', '--appendonly', 'no'])
                    def redis_ready():
                        assert redis.poll() is None, 'owned Redis exited'
                        try:
                            with contextlib.closing(Wire(cfg['redis']['endpoint'])) as c:
                                c.sock.sendall(b'*1\r\n$4\r\nPING\r\n')
                                return c.sock.recv(32) == b'+PONG\r\n'
                        except OSError:
                            return False
                    wait_for(redis_ready, 'owned Redis did not start')
                for name, address in entrances.items():
                    sock_unit = unit + '-' + name + '.socket'
                    install(sock_unit, f'''[Unit]
Description=Owned accelerator activation test {name}
[Socket]
ListenStream={address}
FileDescriptorName={name}
Service={service}
Accept=no
FlushPending=no
RemoveOnStop=yes
SocketUser={os.getuid()}
SocketGroup={os.getgid()}
SocketMode=0600
TriggerLimitIntervalSec=2s
TriggerLimitBurst=20
''')
                    socket_units.append(sock_unit)
                sockets = ' '.join(socket_units)
                install(service, f'''[Unit]
Description=Owned accelerator activation test service
Requires={sockets}
After={sockets}
StartLimitIntervalSec=10s
StartLimitBurst=5
[Service]
Type=exec
User={os.getuid()}
Group={os.getgid()}
Sockets={sockets}
RuntimeDirectory={unit}
RuntimeDirectoryMode=0700
ExecStartPre={json.dumps(sleep_binary)} 0.5
ExecStart={json.dumps(str(executable))} serve --config {json.dumps(str(cfg_path))} --drain-timeout 2s
Restart=on-failure
RestartSec=1s
TimeoutStartSec=90s
TimeoutStopSec=90s
StandardOutput=append:{log}
StandardError=append:{log}
''')
                service_owned = True
                ctl('daemon-reload')
                ctl('start', *socket_units)
                assert int(show()['MainPID']) == 0, 'socket start spawned business process without demand'
                for address in entrances.values():
                    if address.startswith('/'):
                        assert stat.S_ISSOCK(os.stat(address).st_mode), 'manager UDS missing without service'
                # A real manager listener connects before the Go service starts.
                # The fixture's ExecStartPre holds startup for this single probe.
                with contextlib.closing(Wire(data_endpoint, .15)) as c:
                    c.request(5)
                    try:
                        c.response()
                    except TimeoutError:
                        pass
                    else:
                        raise AssertionError('listener-only endpoint passed protocol Ping')
                    assert int(show()['MainPID']) == 0
                pid = wait_started(0)
                if backend != 'store':
                    with contextlib.closing(Wire(data_endpoint)) as c:
                        c.request(2, payload)
                        assert c.response() == b''
                else:
                    primary = entrances['store']
                    manifest_cfg.write_text(json.dumps({'manifest': {'key': '12' * 32},
                        'store': {'endpoint': 'unix://' + primary if primary.startswith('/') else primary, 'pool': 1, 'timeout': '5s'},
                        'chunker': {'mode': 'cdc'}, 'crypto': {'chunk': 'aes', 'manifest': 'aes'}}))
                    source, archive = root / 'payload', root / 'artifact.tar'
                    source.write_bytes(b'activation-store-put-get' * 1024)
                    command(binary_dir / 'flatten-ctl', 'tar', 'stream', '-f', archive, 'payload:' + str(source))
                    manifest_key = command(binary_dir / 'manifest-ctl', 'store', '--manifest-config', manifest_cfg,
                                           '--no-progress', archive).decode().strip()
                    assert re.fullmatch('[0-9a-f]{64}', manifest_key), manifest_key
                    target = root / 'manifest-original'
                    command(binary_dir / 'manifest-ctl', 'get-manifest', '--manifest-config', manifest_cfg,
                            '--output', target, manifest_key)
                    manifest_bytes = target.read_bytes()
                check_requests()
                print(f'PASS {backend}/{network}: manager-only listener rejects Ping; actual binary serves all configured entrances', flush=True)

                # Prove admission has stopped by observing closure of each App
                # listener copy. The CLI retains exactly one original per socket.
                old, previous = pid, starts()
                originals = {os.readlink(f'/proc/{old}/fd/{3+i}') for i in range(len(entrances))}
                with contextlib.closing(Wire(data_endpoint)) as active:
                    active.request(1)
                    header = active.receive(8)
                    ctl('restart', '--no-block', service)
                    def copies_closed():
                        assert int(show()['MainPID']) == old, 'old process exited before admitted response finished'
                        targets = []
                        for fd in Path(f'/proc/{old}/fd').iterdir():
                            try:
                                targets.append(os.readlink(fd))
                            except FileNotFoundError:
                                pass
                        return all(targets.count(original) == 1 for original in originals)
                    wait_for(copies_closed, 'App listening copies remained during drain')
                    with contextlib.closing(Wire(data_endpoint)) as queued:
                        queued.request(1)
                        assert active.response(header, slow=True) == payload, 'planned restart truncated admitted response'
                        pid = wait_started(previous)
                        assert pid != old and not Path(f'/proc/{old}').exists(), 'instances overlapped'
                        assert queued.response() == payload, 'manager lost unaccepted queued request'
                check_requests()
                print(f'PASS {backend}/{network}: planned restart drains old response and serves queued single-attempt request in next process', flush=True)

                previous, old = starts(), pid
                ctl('kill', '--kill-who=main', '--signal=SIGKILL', service)
                pid = wait_started(previous)
                assert pid != old and not Path(f'/proc/{old}').exists()
                check_requests()
                for _ in range(2):
                    previous, old = starts(), pid
                    ctl('restart', service)
                    pid = wait_started(previous)
                    assert pid != old and not Path(f'/proc/{old}').exists()
                    check_requests()
                print(f'PASS {backend}/{network}: SIGKILL and repeated restarts recover actual requests with one complete process', flush=True)

                # Let the configured start-limit window from the preceding
                # independent scenarios expire. Do not reset limits or retry a
                # business request to disguise the initialization-failure case.
                time.sleep(10.05)
                ctl('stop', service)
                assert not run_directory.exists(), 'service runtime directory was not cleaned'
                for address in entrances.values():
                    if address.startswith('/'):
                        assert stat.S_ISSOCK(os.stat(address).st_mode), 'service directory cleanup deleted manager pathname'
                previous, log_offset = starts(), len(text())
                if backend == 'embedded':
                    database = root / 'db'
                    saved = root / 'db-saved'
                    database.rename(saved)
                    database.write_text('temporary initialization failure')
                    expected = 'open rocks'
                elif backend == 'redis':
                    cfg['redis']['endpoint'] = str(root / 'missing-redis.sock')
                    cfg_path.write_text(json.dumps(cfg))
                    expected = 'open redis'
                else:
                    generation_file.rename(root / 'generations-saved')
                    expected = 'initial generations'
                ctl('start', service)
                wait_for(lambda: expected in text()[log_offset:] and show()['SubState'] == 'auto-restart',
                         'temporary initialization failure did not schedule recovery')
                if backend == 'embedded':
                    database.unlink()
                    saved.rename(database)
                elif backend == 'redis':
                    cfg['redis']['endpoint'] = str(root / 'redis.sock')
                    cfg_path.write_text(json.dumps(cfg))
                else:
                    (root / 'generations-saved').rename(generation_file)
                # No systemctl start/reset-failed here: the existing Restart
                # policy must recover this instance itself.
                pid = wait_started(previous)
                check_requests()
                assert int(show()['NRestarts']) >= 1
                print(f'PASS {backend}/{network}: first initialization failure recovers through existing Restart budget; UDS survives runtime cleanup', flush=True)

                # Release manager ownership before ordinary bind uses the same
                # endpoints. The process owns only its own bind-created UDS now.
                ctl('stop', *socket_units, service)
                assert int(show()['MainPID']) == 0
                for sock_unit in socket_units:
                    assert ctl('show', sock_unit, '--property=ActiveState', '--value').strip() == 'inactive'
                if backend == 'store':
                    cfg['listen'], cfg['cache_listen'] = entrances['store'], data_endpoint
                else:
                    cfg['listen'], cfg['health_listen'], cfg['pprof_listen'] = data_endpoint, entrances['health'], entrances['pprof']
                cfg_path.write_text(json.dumps(cfg))
                ordinary_log = root / 'ordinary.log'
                ordinary = spawn('ordinary', [str(executable), 'serve', '--config', str(cfg_path), '--drain-timeout', '2s'])
                def ordinary_started():
                    assert ordinary.poll() is None, 'ordinary bind restart failed'
                    return ' started"' in ordinary_log.read_text()
                wait_for(ordinary_started, 'ordinary bind never initialized')
                check_requests()
                ordinary.terminate()
                assert ordinary.wait(timeout=10) == 0
                print(f'PASS {backend}/{network}: socket units released before rollback; ordinary bind serves unchanged endpoints', flush=True)
            except BaseException:
                for path in root.glob('*.log'):
                    print(path.name + ':\n' + path.read_text(), file=sys.stderr)
                if service_owned:
                    print(ctl('show', service, '--property=MainPID,ControlPID,SubState,Result,ExecStartPre,ExecMainStatus,NRestarts', check=False), file=sys.stderr)
                raise
            finally:
                cleanup()
PY
