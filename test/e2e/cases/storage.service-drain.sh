#!/bin/bash
# e2e-requires: []
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
require_binary store-ctl
require_binary cache-ctl
command -v redis-server >/dev/null
scratch=$(mktemp -d /tmp/acc-drain-ports-XXXXXX)
trap 'rm -rf "$scratch"' EXIT
E2E_PORT_LEASE_FILE="$scratch/ports"
source "$E2E_LIB/accelerator/port_lease.sh"
ports=()
for _ in 1 2 3 4; do ports+=("$(e2e_free_port)"); done

# Only prebuilt products. Protocol operations are single attempt; polling is
# restricted to observing our processes, readiness and shutdown transitions.
python3 - "$BIN/cache-ctl" "$BIN/store-ctl" "${ports[@]}" <<'PY'
import contextlib
import hashlib
import json
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

cache_binary, store_binary = sys.argv[1:3]
ports = iter(sys.argv[3:])
payload = b'drain-response-' * (3 * 1024 * 1024 // 15)
key = hashlib.sha256(payload).digest()

def connect(endpoint):
    sock = socket.socket(socket.AF_UNIX if endpoint.startswith('/') else socket.AF_INET)
    sock.settimeout(5)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
    try:
        sock.connect(endpoint if endpoint.startswith('/') else (endpoint.split(':')[0], int(endpoint.split(':')[1])))
        return sock
    except BaseException:
        sock.close()
        raise

def receive(sock, size, slow=False):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(min(size - len(data), 4096 if slow else 65536))
        if not part:
            raise EOFError(f'truncated frame: {len(data)}/{size}')
        data.extend(part)
        if slow:
            time.sleep(.001)
    return bytes(data)

def request(sock, opcode, value=b''):
    sock.sendall(struct.pack('<IBBB', 39 + len(value), opcode, 3, 0) + key + value)

def response(sock, header=None, slow=False):
    header = receive(sock, 8) if header is None else header
    size, status, reserved, error_len = struct.unpack('<IBBH', header)
    assert 8 <= size <= 4 << 20, size
    body = receive(sock, size - 8, slow)
    assert status == 0 and error_len == 0, (status, body[:200])
    return body

def wait_for(check, label):
    until = time.monotonic() + 10
    while time.monotonic() < until:
        if check():
            return
        time.sleep(.02)
    raise AssertionError(label)

for backend in ('embedded', 'redis', 'store'):
    for network in ('tcp', 'unix'):
        with tempfile.TemporaryDirectory(prefix='acc-drain-') as directory, contextlib.ExitStack() as stack:
            root = Path(directory)
            processes, logs = [], []
            def spawn(name, args):
                path = root / (name + '.log')
                stream = stack.enter_context(path.open('w'))
                proc = subprocess.Popen(args, stdout=stream, stderr=subprocess.STDOUT)
                processes.append(proc)
                logs.append(path)
                return proc, path
            def cleanup():
                for proc in reversed(processes):
                    if proc.poll() is None:
                        proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
            stack.callback(cleanup)
            try:
                endpoint = str(root / 'wire.sock') if network == 'unix' else '127.0.0.1:' + next(ports)
                cfg = root / 'service.yaml'
                if backend == 'store':
                    primary = str(root / 'store.sock') if network == 'unix' else '127.0.0.1:' + next(ports)
                    cfg.write_text(f'listen: {json.dumps(primary)}\ncache_listen: {json.dumps(endpoint)}\n'
                                   f'backend: fs\nfs: {{root: {root / "data"}}}\ngenerations: {{config: [G1]}}\n')
                    digest = key.hex()
                    path = root / 'data' / 'blob' / 'G1' / digest[:2] / digest[2:4] / digest
                    path.parent.mkdir(parents=True)
                    path.write_bytes(payload)
                    binary = store_binary
                else:
                    binary = cache_binary
                    redis_path = str(root / 'redis.sock')
                    if backend == 'redis':
                        redis, _ = spawn('redis', ['redis-server', '--port', '0', '--unixsocket', redis_path,
                                                  '--save', '', '--appendonly', 'no'])
                        def redis_ready():
                            assert redis.poll() is None, 'owned Redis exited'
                            try:
                                with connect(redis_path) as c:
                                    c.sendall(b'*1\r\n$4\r\nPING\r\n')
                                    return c.recv(32) == b'+PONG\r\n'
                            except OSError:
                                return False
                        wait_for(redis_ready, 'owned Redis never started')
                    storage = (f'rocks: {{path: {root / "db"}, disk_bytes: 64MiB, mem_ratio: 0.05, direct_reads: false}}\n'
                               if backend == 'embedded' else
                               f'redis: {{endpoint: {json.dumps(redis_path)}, get_pool: 1, set_pool: 1, timeout: 2s}}\n')
                    cfg.write_text(f'mode: local\ntype: {backend}\nlisten: {json.dumps(endpoint)}\n'
                                   'freq: {counters: 1M, reset_after: 100K, disable_eviction: true}\n' + storage)
                for phase in ('drain', 'second-signal', 'budget', 'restart'):
                    args = [binary, 'serve', '--config', str(cfg)]
                    if phase == 'budget':
                        args += ['--drain-timeout', '200ms']
                    proc, log = spawn(phase, args)
                    def started():
                        assert proc.poll() is None, f'owned service exited: {proc.returncode}'
                        return 'msg="' + ('store' if backend == 'store' else 'cache') + ' started"' in log.read_text()
                    wait_for(started, 'owned service did not start')
                    with connect(endpoint) as c:
                        request(c, 5)
                        assert response(c) == b''
                    if phase == 'drain' and backend != 'store':
                        with connect(endpoint) as c:
                            request(c, 2, payload)
                            assert response(c) == b''
                    with connect(endpoint) as c:
                        request(c, 1)
                        header = receive(c, 8)  # admitted and response started
                        if phase == 'restart':
                            assert response(c, header) == payload, 'restart lost committed content'
                            proc.terminate()
                            assert proc.wait(timeout=10) == 0
                            continue
                        proc.send_signal(signal.SIGTERM)
                        wait_for(lambda: 'draining...' in log.read_text(), 'first signal did not drain')
                        def no_admission():
                            try:
                                with connect(endpoint):
                                    return False
                            except (ConnectionRefusedError, FileNotFoundError):
                                return True
                        wait_for(no_admission, 'drain did not close ordinary listener')
                        assert proc.poll() is None, 'first signal abandoned slow response'
                        proc.send_signal(signal.SIGHUP)  # cannot resume admission/refresh during drain
                        if phase == 'drain':
                            assert response(c, header, slow=True) == payload, 'drained response corrupt'
                            assert c.recv(1) == b'', 'unexpected response after drained frame'
                            assert proc.wait(timeout=10) == 0
                        else:
                            if phase == 'second-signal':
                                proc.send_signal(signal.SIGINT)
                            code = proc.wait(timeout=10)
                            assert (code == 0) if phase == 'second-signal' else (code != 0), (phase, code)
                            try:
                                response(c, header)
                            except (OSError, EOFError):
                                pass
                            else:
                                raise AssertionError('interrupted response reported success')
                            expected = 'cancelling...' if phase == 'second-signal' else 'drain timeout'
                            assert expected in log.read_text(), log.read_text()
                    print(f'PASS {backend}/{network}/{phase}: actual CLI, single-attempt response, owned process joined', flush=True)
                print(f'PASS {backend}/{network}/restart: same endpoint and committed content after old process exited', flush=True)
            except BaseException:
                for path in logs:
                    print(path.name + ':\n' + path.read_text(), file=sys.stderr)
                raise
PY
