#!/bin/bash
# e2e-requires: []
set -euo pipefail
source "${E2E_LIB:?E2E_LIB is required}/common.sh"
require_binary cache-ctl
require_command redis-server
lease_dir=$(mktemp -d /tmp/acc-probe-ports-XXXXXX)
trap 'rm -rf "$lease_dir"' EXIT
E2E_PORT_LEASE_FILE="$lease_dir/ports"
source "$E2E_LIB/accelerator/port_lease.sh"
ports=()
for _ in 1 2 3 4; do ports+=("$(e2e_free_port)"); done

# Exercise the actual daemon's SIGHUP -> ProbePeer path. No business operation
# is retried; polling below only observes startup and reload completion.
python3 - "$BIN/cache-ctl" "${ports[@]}" <<'PY'
import contextlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

binary = sys.argv[1]
ports = iter(sys.argv[2:])

def connect(endpoint):
    sock = socket.socket(socket.AF_UNIX if endpoint.startswith('/') else socket.AF_INET)
    sock.settimeout(3)
    sock.connect(endpoint if endpoint.startswith('/') else (endpoint.split(':')[0], int(endpoint.split(':')[1])))
    return sock

def receive(sock, size):
    data = b''
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise AssertionError('truncated wire response')
        data += part
    return data

def exchange(endpoint, opcode, value=b'', key=bytes(32)):
    with connect(endpoint) as sock:
        sock.sendall(struct.pack('<IBBB', 39 + len(value), opcode, 1, 0) + key + value)
        header = receive(sock, 8)
        size, status, reserved, error_len = struct.unpack('<IBBH', header)
        assert 8 <= size <= 4 << 20, header
        body = receive(sock, size - 8)
        assert status == 0 and error_len == 0, (status, body)
        return body

def wait_for(check, label):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, subprocess.SubprocessError):
            pass
        time.sleep(.05)
    raise AssertionError(label)

for backend in ('embedded', 'redis'):
    for network in ('tcp', 'unix'):
        with tempfile.TemporaryDirectory(prefix='acc-probe-') as directory, contextlib.ExitStack() as stack:
            root = Path(directory)
            processes = []
            logs = []
            def spawn(name, args):
                path = root / (name + '.log')
                stream = stack.enter_context(path.open('w'))
                proc = subprocess.Popen(args, stdout=stream, stderr=subprocess.STDOUT)
                processes.append(proc)
                logs.append(path)
                return proc
            def stop_all():
                for proc in reversed(processes):
                    if proc.poll() is None:
                        proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
            stack.callback(stop_all)
            def endpoint(name):
                if network == 'unix':
                    return str(root / (name + '.sock'))
                return '127.0.0.1:' + next(ports)
            def started(proc, name):
                if proc.poll() is not None:
                    raise AssertionError(f'{name} exited before readiness: {proc.returncode}')
                return 'msg="cache started"' in (root / (name + '.log')).read_text()
            try:
                redis_path = str(root / 'redis.sock')
                if backend == 'redis':
                    spawn('redis', ['redis-server', '--port', '0', '--unixsocket', redis_path,
                                    '--save', '', '--appendonly', 'no'])
                    def redis_ready():
                        with connect(redis_path) as sock:
                            sock.sendall(b'*1\r\n$4\r\nPING\r\n')
                            return sock.recv(32) == b'+PONG\r\n'
                    wait_for(redis_ready, 'owned Redis never became ready')
                peers = []
                for index in range(2):
                    ep = endpoint('peer' + str(index))
                    peers.append(ep)
                    cfg = root / ('peer' + str(index) + '.yaml')
                    storage = (f'rocks: {{path: {root / ("db" + str(index))}, disk_bytes: 64MiB, mem_ratio: 0.05, direct_reads: false}}\n'
                               if backend == 'embedded' else
                               f'redis: {{endpoint: {json.dumps(redis_path)}, get_pool: 1, set_pool: 1, timeout: 2s}}\n')
                    cfg.write_text(f'mode: shard\ntype: {backend}\nlisten: {json.dumps(ep)}\n'
                                   f'freq: {{counters: 1M, reset_after: 100K}}\n' + storage)
                    name = 'peer' + str(index)
                    proc = spawn(name, [binary, 'serve', '--config', str(cfg)])
                    # Confirm this process bound its listener before sending
                    # any request, so a port collision cannot target another service.
                    wait_for(lambda: started(proc, name), 'owned peer did not start')
                    assert exchange(ep, 5) == b''
                    payload = bytes((0, 1)) + b'probe-single-attempt-roundtrip' + bytes((index,))
                    key = bytes((index + 1,)) * 32
                    assert exchange(ep, 4, payload, key) == b''
                    assert exchange(ep, 3, key=key) == payload
                dead = stack.enter_context(socket.socket(socket.AF_UNIX if network == 'unix' else socket.AF_INET))
                dead.bind(str(root / 'dead.sock') if network == 'unix' else ('127.0.0.1', 0))
                dead.listen(16)  # Intentionally no accept; connect alone succeeds.
                dead_ep = str(root / 'dead.sock') if network == 'unix' else '127.0.0.1:' + str(dead.getsockname()[1])
                tier_ep, health = str(root / 'tier.sock'), str(root / 'health.sock')
                cfg = root / 'tier.yaml'
                def configure(include_dead):
                    candidates = [{'id': 'live' + str(i), 'endpoint': ep} for i, ep in enumerate(peers)]
                    if include_dead:
                        candidates.append({'id': 'listener-only', 'endpoint': dead_ep})
                    cfg.write_text(f'mode: tiered\nlisten: {json.dumps(tier_ep)}\nhealth_listen: {json.dumps(health)}\n'
                                   'tiers:\n  - type: ec\n    cluster:\n      data_shards: 1\n      parity_shards: 1\n'
                                   f'      peers: {json.dumps(candidates)}\n      pool: 1\n      timeout: 2s\n'
                                   f'origin:\n  type: upstream\n  upstream: {{endpoint: {json.dumps(peers[0])}, pool: 1, timeout: 2s}}\n')
                configure(False)
                tier = spawn('tier', [binary, 'serve', '--config', str(cfg)])
                wait_for(lambda: started(tier, 'tier'), 'owned tier did not start')
                assert exchange(tier_ep, 5) == b''
                configure(True)
                tier.send_signal(signal.SIGHUP)
                tier_log = root / 'tier.log'
                wait_for(lambda: 'membership epoch 1' in tier_log.read_text(), 'membership reload did not finish')
                log = tier_log.read_text()
                assert 'peer probe failed' in log and 'listener-only' in log, log
                info = json.loads(subprocess.check_output([binary, 'info', '--endpoint', 'unix://' + health, '--json'], timeout=5))
                actual = {peer['id'] for peer in info['tiered']['tiers'][0]['peers']}
                assert actual == {'live0', 'live1'}, actual
                assert exchange(tier_ep, 5) == b''
                print(f'PASS {backend}/{network}: real Ping, single-attempt shard roundtrip, listener-only excluded by actual SIGHUP probe', flush=True)
            except BaseException:
                for path in logs:
                    print(path.name + ':\n' + path.read_text(), file=sys.stderr)
                raise
PY
