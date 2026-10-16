#!/bin/bash
# e2e-requires: []
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
require_command timeout
require_binary store-ctl
require_binary manifest-ctl
require_binary cache-ctl
require_binary flatten-ctl
REDIS_SERVER="${REDIS_SERVER:-redis-server}"
require_command "$REDIS_SERVER"

# Prepared tiered/EC/Redis cache correctness.
# Run with the platform runner: e2e run --include storage.tiered-cache.sh.

TMPDIR=$(mktemp -d /tmp/acc-cache-e2e-XXXXXX)
E2E_PORT_LEASE_FILE="$TMPDIR/ports"
source "$E2E_LIB/accelerator/port_lease.sh"
KEY=$(openssl rand -hex 32)

PASS=0
FAIL=0
PIDS=()
cleanup() {
    # A SIGSTOP-based slow-peer test may abort before its matching SIGCONT.
    # Resume every child first so TERM + wait cannot strand cleanup.
    for pid in "${PIDS[@]}"; do
        kill -CONT "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

ok() {
    PASS=$((PASS + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    echo "  FAIL: $1"
}

assert_eq() {
    if [ "$1" = "$2" ]; then
        ok "$3"
    else
        fail "$3 (expected '$1', got '$2')"
    fi
}

# Wait for a cache-ctl health endpoint to become healthy.
wait_ready() {
    local endpoint=$1
    for i in $(seq 1 50); do
        if "$BIN/cache-ctl" ping --endpoint "$endpoint" 2>/dev/null | grep -Fxq SERVING; then
            return 0
        fi
        sleep 0.1
    done
    echo "  ERROR: cache-ctl health at $endpoint did not become ready"
    return 1
}

wait_shard_present() {
    local endpoint=$1 namespace=$2 hash=$3
    for _ in $(seq 1 100); do
        if "$BIN/cache-ctl" shard get --endpoint "$endpoint" \
            --namespace "$namespace" --hash "$hash" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

origin_hits() {
    local endpoint=$1
    "$BIN/cache-ctl" info --endpoint "$endpoint" --json | \
        python3 -c 'import json,sys; print(json.load(sys.stdin)["tiered"]["origin"]["hits"])'
}

wait_manifest_load() {
    local config=$1 output=$2 key=$3 expected_hash=$4
    for _ in $(seq 1 100); do
        if "$BIN/manifest-ctl" load --manifest-config "$config" \
            --output "$output" --no-progress "$key" >/dev/null 2>&1 &&
            [ "$(payload_hash "$output" 2>/dev/null)" = "$expected_hash" ]; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

# Start a disposable origin over the already initialized FS store. Tests use
# this process only for reads, then stop it without disrupting the main origin.
start_store_reader() {
    local name=$1
    STORE_READER_PORT=$(e2e_free_port)
    cat > "$TMPDIR/$name.yaml" <<EOF
listen: 127.0.0.1:$STORE_READER_PORT
backend: fs
fs:
  root: $STORE_ROOT
  verify_content_key: true
EOF
    "$BIN/store-ctl" serve --config "$TMPDIR/$name.yaml" >"$TMPDIR/$name.log" 2>&1 &
    STORE_READER_PID=$!
    PIDS+=("$STORE_READER_PID")
    for _ in $(seq 1 50); do
        if (echo >/dev/tcp/127.0.0.1/"$STORE_READER_PORT") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: store reader $name did not listen on 127.0.0.1:$STORE_READER_PORT" >&2
    cat "$TMPDIR/$name.log" >&2 || true
    return 1
}

# Start an isolated Redis protocol test server with both loopback TCP and UDS.
# An optional second argument reuses a TCP port for cold-restart tests.
start_redis() {
    local name=$1
    local requested_port=${2:-}
    local dir="$TMPDIR/$name"
    mkdir -p "$dir"
    REDIS_SOCKET="$dir/redis.sock"
    REDIS_TCP_PORT=${requested_port:-$(e2e_free_port)}
    rm -f "$REDIS_SOCKET"
    "$REDIS_SERVER" \
        --bind 127.0.0.1 \
        --port "$REDIS_TCP_PORT" \
        --unixsocket "$REDIS_SOCKET" \
        --unixsocketperm 700 \
        --save "" \
        --appendonly no \
        --dir "$dir" \
        --daemonize no >"$dir/redis.log" 2>&1 &
    REDIS_PID=$!
    PIDS+=("$REDIS_PID")
    for _ in $(seq 1 50); do
        if [ -S "$REDIS_SOCKET" ] && (echo >/dev/tcp/127.0.0.1/"$REDIS_TCP_PORT") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: redis-server did not expose $REDIS_SOCKET and 127.0.0.1:$REDIS_TCP_PORT" >&2
    cat "$dir/redis.log" >&2 || true
    return 1
}

# Hash the payload inside a tarstream artifact. store/load round-trip the
# payload losslessly but re-canonicalize the tar envelope (entry name → image,
# mtime → epoch), so roundtrip checks compare extracted payloads, not the
# envelope bytes.
payload_hash() {
    tar xOf "$1" | sha256sum | awk '{print $1}'
}

# Prepare a 2 MiB snapshot-like payload. Every page has a distinct header, so
# CDC produces multiple independently addressed chunks, while the repeated
# page body makes those chunks exercise the canonical Snappy path through store,
# embedded/Redis caches, and EC. Other E2E fixtures remain high-entropy RAW.
python3 - "$TMPDIR/payload.bin" <<'PY'
import sys

with open(sys.argv[1], "wb") as output:
    for page in range(512):
        header = f"snapshot-page={page:06d}\n".encode()
        output.write(header + b"A" * (4096 - len(header)))
PY
"$BIN/flatten-ctl" tar stream -f "$TMPDIR/test.bin" \
    "payload.bin:$TMPDIR/payload.bin"

# ============================================================
# Spin up store-ctl sidecar. All subsequent manifest-ctl / cache-ctl
# tests talk to the store only through this daemon's gRPC endpoint.
# ============================================================
echo ""
echo "=== Spin up store-ctl sidecar ==="
STORE_PORT=$(e2e_free_port)
STORE_ROOT="$TMPDIR/store-data"
cat > "$TMPDIR/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $STORE_ROOT
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$TMPDIR/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$TMPDIR/store-ctl.yaml" &
STORE_PID=$!
PIDS+=($STORE_PID)
# Poll for the gRPC port to be accepting connections.
for i in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null; then
        break
    fi
    sleep 0.1
done
echo "  store-ctl listen=127.0.0.1:$STORE_PORT root=$STORE_ROOT"

# Write config for manifest-ctl. The store endpoint points at the
# store-ctl daemon above; there is no `backend: fs` notion anymore.
cat > "$TMPDIR/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 5s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

COMMON="--manifest-config $TMPDIR/accelerator.yaml"

# manifest-ctl no longer accepts --cache-endpoint flag override. Build a
# variant of the YAML with a specific cache.endpoint on demand and pass
# --config to manifest-ctl explicitly.
accel_cfg_for_cache() {
    local ep="$1"
    local out="$TMPDIR/accelerator-cache-$(echo "$ep" | tr ':.' '__').yaml"
    cat > "$out" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 5s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
cache:
  endpoint: $ep
  pool: 2
  timeout: 5s
EOF
    echo "$out"
}

# Store test data via store-ctl.
echo ""
echo "=== Ingest test data (via store-ctl) ==="
MKEY=$("$BIN/manifest-ctl" store $COMMON --no-progress "$TMPDIR/test.bin" 2>"$TMPDIR/store-summary.log")
cat "$TMPDIR/store-summary.log" >&2
if grep -Eq 'compression:.*snappy=[1-9][0-9]*' "$TMPDIR/store-summary.log"; then
    ok "main cache/EC artifact contains compressed chunks"
else
    fail "main cache/EC artifact did not produce compressed chunks"
fi
ORIG_HASH=$(payload_hash "$TMPDIR/test.bin")
echo "  Stored. SHA256=$ORIG_HASH  manifest-key=$MKEY"

# The store is empty before the main artifact is ingested, so this snapshot is
# exactly the set of non-zero chunks referenced by MKEY. Capture it before
# Test 0 adds its unrelated object.
mapfile -t MAIN_CHUNK_KEYS < <(find "$STORE_ROOT/chunk/G1" -type f -printf '%f\n' | sort)
if [ "${#MAIN_CHUNK_KEYS[@]}" -lt 2 ]; then
    echo "ERROR: 2 MiB main artifact produced fewer than two CDC chunks" >&2
    exit 1
fi
MAIN_CACHE_OBJECTS=("manifest:$MKEY")
for chunk_key in "${MAIN_CHUNK_KEYS[@]}"; do
    MAIN_CACHE_OBJECTS+=("chunk:$chunk_key")
done

echo "=== Test 8: 5-node shard cluster + tiered EC mode ==="

SHARD_PORTS=()
SHARD_HEALTH_PORTS=()
SHARD_PIDS=()
for i in $(seq 1 5); do
    SP=$(e2e_free_port)
    SHP=$(e2e_free_port)
    SHARD_PORTS+=("$SP")
    SHARD_HEALTH_PORTS+=("$SHP")
    cat > "$TMPDIR/shard-$i.yaml" <<EOF
mode: shard
type: embedded
listen: 127.0.0.1:$SP
health_listen: 127.0.0.1:$SHP
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-shard-$i
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF
    "$BIN/cache-ctl" serve --config "$TMPDIR/shard-$i.yaml" &
    shard_pid=$!
    PIDS+=($shard_pid)
    SHARD_PIDS+=($shard_pid)
done

# Wait for all shard nodes.
for SHP in "${SHARD_HEALTH_PORTS[@]}"; do
    wait_ready "127.0.0.1:$SHP"
done

# Tiered with EC.
EC_TIERED_PORT=$(e2e_free_port)
EC_TIERED_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/tiered-ec.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$EC_TIERED_PORT
health_listen: 127.0.0.1:$EC_TIERED_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      peers:
        - {id: s1, endpoint: "127.0.0.1:${SHARD_PORTS[0]}"}
        - {id: s2, endpoint: "127.0.0.1:${SHARD_PORTS[1]}"}
        - {id: s3, endpoint: "127.0.0.1:${SHARD_PORTS[2]}"}
        - {id: s4, endpoint: "127.0.0.1:${SHARD_PORTS[3]}"}
        - {id: s5, endpoint: "127.0.0.1:${SHARD_PORTS[4]}"}
      pool: 1
      timeout: 2s
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/tiered-ec.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$EC_TIERED_HEALTH_PORT"

# Load through EC tiered cache.
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$EC_TIERED_PORT")" \
    --output "$TMPDIR/test-ec.bin" --no-progress "$MKEY" 2>&1
EC_HASH=$(payload_hash "$TMPDIR/test-ec.bin")
assert_eq "$ORIG_HASH" "$EC_HASH" "tiered+EC load roundtrip"

# Observe the manifest and every referenced chunk through each shard peer's
# public data plane before injecting a failure.
ALL_SHARDS_PRESENT=1
for peer_port in "${SHARD_PORTS[@]}"; do
    for pair in "${MAIN_CACHE_OBJECTS[@]}"; do
        namespace=${pair%%:*}
        object_key=${pair#*:}
        if ! wait_shard_present "127.0.0.1:$peer_port" "$namespace" "$object_key"; then
            ALL_SHARDS_PRESENT=0
        fi
    done
done
assert_eq "1" "$ALL_SHARDS_PRESENT" "EC cold fill populated every embedded shard peer"
if [ "$ALL_SHARDS_PRESENT" != "1" ]; then
    exit 1
fi
EC_ORIGIN_HITS_BEFORE=$(origin_hits "127.0.0.1:$EC_TIERED_HEALTH_PORT")

# ============================================================
echo ""
echo "=== Test 9: EC failure injection — kill 1 shard node ==="
# Kill one EC shard node.
kill "${SHARD_PIDS[0]}" 2>/dev/null || true
wait "${SHARD_PIDS[0]}" 2>/dev/null || true

# Second load must reconstruct from four EC shards without falling through.
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$EC_TIERED_PORT")" \
    --output "$TMPDIR/test-ec-1down.bin" --no-progress "$MKEY" 2>&1
DOWN1_HASH=$(payload_hash "$TMPDIR/test-ec-1down.bin")
assert_eq "$ORIG_HASH" "$DOWN1_HASH" "tiered+EC load with 1 shard down"
EC_ORIGIN_HITS_AFTER=$(origin_hits "127.0.0.1:$EC_TIERED_HEALTH_PORT")
assert_eq "$EC_ORIGIN_HITS_BEFORE" "$EC_ORIGIN_HITS_AFTER" \
    "EC load with 1 shard down did not fall through to origin"

# ============================================================
echo ""
echo "=== Test 10: tiered + upstream — read-through + writeback ==="
# Two-process layout:
#   remote = local mode (terminal; its own RocksDB is the backing store)
#   front  = tiered mode with a single upstream tier pointing at remote
#            (NO embedded — otherwise reads would never reach upstream)
# Cold load through front:
#   front.Get(chunk) → upstream.Get → miss → origin.Get → HIT (fs backend)
#                    → TieredCache.startFill writes back into upstream
#                    → client.Tier.Fill → writer.Put → remote.Put → rocks
# Then kill front and read the same manifest directly from remote.
# remote is local mode, no origin — it can only serve chunks that were
# written during the writeback phase. Hash match ⇒ writeback works.
REMOTE_PORT=$(e2e_free_port)
REMOTE_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/upstream-remote.yaml" <<EOF
mode: local
type: embedded
listen: 127.0.0.1:$REMOTE_PORT
health_listen: 127.0.0.1:$REMOTE_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-upstream-remote
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/upstream-remote.yaml" &
REMOTE_PID=$!
PIDS+=($REMOTE_PID)
wait_ready "127.0.0.1:$REMOTE_HEALTH_PORT"

FRONT_PORT=$(e2e_free_port)
FRONT_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/upstream-front.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$FRONT_PORT
health_listen: 127.0.0.1:$FRONT_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: upstream
    endpoint: 127.0.0.1:$REMOTE_PORT
    pool: 1
    timeout: 2s
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/upstream-front.yaml" &
FRONT_PID=$!
PIDS+=($FRONT_PID)
wait_ready "127.0.0.1:$FRONT_HEALTH_PORT"

# Cold read through front: fill chain runs all the way to origin and
# writes back through upstream into remote.
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$FRONT_PORT")" \
    --output "$TMPDIR/test-upstream.bin" --no-progress "$MKEY" 2>&1
UP_HASH=$(payload_hash "$TMPDIR/test-upstream.bin")
assert_eq "$ORIG_HASH" "$UP_HASH" "upstream tier read-through (cold)"

# Fill/writeback is asynchronous. Verify the complete artifact directly on the
# remote before stopping the front cache; this checks manifest and chunk data,
# rather than an internal goroutine count.
if ! wait_manifest_load "$(accel_cfg_for_cache "127.0.0.1:$REMOTE_PORT")" \
    "$TMPDIR/test-from-remote-ready.bin" "$MKEY" "$ORIG_HASH"; then
    fail "upstream tier writeback did not become readable on remote"
    exit 1
fi

# Kill front; remote must now serve the same manifest on its own.
# makeCacheReader has no local-store fall-through — if writeback did
# not populate remote, the next load fails with "chunk not found".
kill $FRONT_PID 2>/dev/null; wait $FRONT_PID 2>/dev/null || true
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$REMOTE_PORT")" \
    --output "$TMPDIR/test-from-remote.bin" --no-progress "$MKEY" 2>&1
REM_HASH=$(payload_hash "$TMPDIR/test-from-remote.bin")
assert_eq "$ORIG_HASH" "$REM_HASH" "upstream tier writeback populated remote"

# ============================================================
echo ""
echo "=== Test 11: upstream dial failure rejects cache-ctl startup ==="
# 127.0.0.1:1 is RFC-reserved and refuses connections immediately, so
# DialConnPool fails at the first dial and buildTieredChain rolls back.
# serve then calls fatal → os.Exit(1). Verify the process exits non-zero
# and the error message names the failure site.
BAD_PORT=$(e2e_free_port)
BAD_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/upstream-bad.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$BAD_PORT
health_listen: 127.0.0.1:$BAD_HEALTH_PORT
rpc_timeout: 2s
tiers:
  - type: upstream
    endpoint: 127.0.0.1:1
    pool: 1
    timeout: 500ms
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
# Bound the negative test: accepting the invalid upstream must fail, not hang.
# timeout owns and reaps the process; it is never treated as the expected error.
if ERR_OUT=$(timeout --signal=TERM --kill-after=1s 5s "$BIN/cache-ctl" serve --config "$TMPDIR/upstream-bad.yaml" 2>&1); then
    fail "upstream bad endpoint should have rejected startup (exited 0; output: $ERR_OUT)"
else
    ERR_STATUS=$?
    if [ "$ERR_STATUS" -eq 1 ] && grep -qiE "dial reader|dial writer|upstream.*connection refused|connection refused" <<<"$ERR_OUT"; then
        ok "upstream bad endpoint rejected at startup"
    else
        fail "upstream bad endpoint did not reject startup as expected (exit=$ERR_STATUS; output: $ERR_OUT)"
    fi
fi

# ============================================================
echo ""
echo "=== Test 12: Redis local mode — object and shard interfaces ==="
start_redis redis-local
REDIS_LOCAL_SOCKET=$REDIS_SOCKET
REDIS_LOCAL_ENDPOINT="unix://$REDIS_LOCAL_SOCKET"
REDIS_LOCAL_PORT=$(e2e_free_port)
REDIS_LOCAL_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/redis-local.yaml" <<EOF
mode: local
type: redis
listen: 127.0.0.1:$REDIS_LOCAL_PORT
health_listen: 127.0.0.1:$REDIS_LOCAL_HEALTH_PORT
rpc_timeout: 2s
redis:
  endpoint: $REDIS_LOCAL_ENDPOINT
  get_pool: 2
  set_pool: 2
  timeout: 2s
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/redis-local.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$REDIS_LOCAL_HEALTH_PORT"

REDIS_HASH="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
echo -n "redis-object" | "$BIN/cache-ctl" object put --endpoint "127.0.0.1:$REDIS_LOCAL_PORT" --namespace chunk --hash "$REDIS_HASH" --value -
GOT=$("$BIN/cache-ctl" object get --endpoint "127.0.0.1:$REDIS_LOCAL_PORT" --namespace chunk --hash "$REDIS_HASH")
assert_eq "redis-object" "$GOT" "Redis local object put/get"
echo -n "redis-shard" | "$BIN/cache-ctl" shard put --endpoint "127.0.0.1:$REDIS_LOCAL_PORT" --namespace chunk --hash "$REDIS_HASH" --idx 2 --total 5 --value -
GOT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$REDIS_LOCAL_PORT" --namespace chunk --hash "$REDIS_HASH" 2>/dev/null)
assert_eq "redis-shard" "$GOT" "Redis local shard put/get"
if "$BIN/cache-ctl" info --endpoint "127.0.0.1:$REDIS_LOCAL_HEALTH_PORT" --json | \
    EXPECTED_REDIS_ENDPOINT="$REDIS_LOCAL_ENDPOINT" python3 -c 'import json,os,sys; d=json.load(sys.stdin); assert d["backend_type"] == "redis"; assert d["redis"]["endpoint"] == os.environ["EXPECTED_REDIS_ENDPOINT"]; assert d["redis"]["transport"] == "unix"'; then
    ok "Redis local Info reports UDS endpoint and transport"
else
    fail "Redis local Info should report its UDS endpoint and transport"
fi

# ============================================================
echo ""
echo "=== Test 13: tiered type=redis — cold fill and warm hit ==="
start_redis redis-tiered
REDIS_TIER_ENDPOINT="127.0.0.1:$REDIS_TCP_PORT"
start_store_reader redis-tier-origin
REDIS_TIER_ORIGIN_PORT=$STORE_READER_PORT
REDIS_TIER_ORIGIN_PID=$STORE_READER_PID
REDIS_TIER_PORT=$(e2e_free_port)
REDIS_TIER_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/redis-tiered.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$REDIS_TIER_PORT
health_listen: 127.0.0.1:$REDIS_TIER_HEALTH_PORT
rpc_timeout: 5s
tiers:
  - type: redis
    redis:
      endpoint: $REDIS_TIER_ENDPOINT
      get_pool: 2
      set_pool: 2
      timeout: 2s
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$REDIS_TIER_ORIGIN_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/redis-tiered.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$REDIS_TIER_HEALTH_PORT"
REDIS_TIER_MANIFEST_CONFIG=$(accel_cfg_for_cache "127.0.0.1:$REDIS_TIER_PORT")
"$BIN/manifest-ctl" load --manifest-config "$REDIS_TIER_MANIFEST_CONFIG" \
    --output "$TMPDIR/test-redis-tier-cold.bin" --no-progress "$MKEY" 2>&1
kill "$REDIS_TIER_ORIGIN_PID" 2>/dev/null || true
wait "$REDIS_TIER_ORIGIN_PID" 2>/dev/null || true
if wait_manifest_load "$REDIS_TIER_MANIFEST_CONFIG" "$TMPDIR/test-redis-tier-probe.bin" \
    "$MKEY" "$ORIG_HASH"; then
    ok "tiered Redis cold fill became readable with origin stopped"
else
    fail "tiered Redis cold fill did not become readable with origin stopped"
    exit 1
fi
"$BIN/manifest-ctl" load --manifest-config "$REDIS_TIER_MANIFEST_CONFIG" \
    --output "$TMPDIR/test-redis-tier-warm.bin" --no-progress "$MKEY" 2>&1
REDIS_TIER_HASH=$(payload_hash "$TMPDIR/test-redis-tier-warm.bin")
assert_eq "$ORIG_HASH" "$REDIS_TIER_HASH" "tiered Redis warm load without origin"
if "$BIN/cache-ctl" info --endpoint "127.0.0.1:$REDIS_TIER_HEALTH_PORT" --json | \
    EXPECTED_REDIS_ENDPOINT="$REDIS_TIER_ENDPOINT" python3 -c 'import json,os,sys; d=json.load(sys.stdin); r=d["tiered"]["tiers"][0]; assert r["type"] == "redis" and r["hits"] > 0; assert r["redis"]["endpoint"] == os.environ["EXPECTED_REDIS_ENDPOINT"]; assert r["redis"]["transport"] == "tcp"'; then
    ok "tiered Redis over TCP reports warm hits and transport"
else
    fail "tiered Redis over TCP should report warm hits and transport"
fi

# ============================================================
echo ""
echo "=== Test 14: five Redis shard peers — EC fill and origin-free hit ==="
REDIS_SHARD_PORTS=()
REDIS_SHARD_HEALTH_PORTS=()
REDIS_SHARD_PIDS=()
REDIS_SHARD_TCP_PORTS=()
for i in $(seq 1 5); do
    start_redis "redis-ec-$i"
    sock=$REDIS_SOCKET
    redis_tcp_port=$REDIS_TCP_PORT
    redis_endpoint="unix://$sock"
    if [ "$i" -eq 1 ]; then
        redis_endpoint="127.0.0.1:$redis_tcp_port"
    fi
    REDIS_SHARD_PIDS+=("$REDIS_PID")
    REDIS_SHARD_TCP_PORTS+=("$redis_tcp_port")
    data_port=$(e2e_free_port)
    health_port=$(e2e_free_port)
    REDIS_SHARD_PORTS+=("$data_port")
    REDIS_SHARD_HEALTH_PORTS+=("$health_port")
    cat > "$TMPDIR/redis-shard-$i.yaml" <<EOF
mode: shard
type: redis
listen: 127.0.0.1:$data_port
health_listen: 127.0.0.1:$health_port
rpc_timeout: 2s
redis:
  endpoint: $redis_endpoint
  get_pool: 2
  set_pool: 2
  timeout: 2s
EOF
    "$BIN/cache-ctl" serve --config "$TMPDIR/redis-shard-$i.yaml" &
    cache_pid=$!
    PIDS+=("$cache_pid")
done
for health_port in "${REDIS_SHARD_HEALTH_PORTS[@]}"; do
    wait_ready "127.0.0.1:$health_port"
done

REDIS_EC_PORT=$(e2e_free_port)
REDIS_EC_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/redis-ec-tiered.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$REDIS_EC_PORT
health_listen: 127.0.0.1:$REDIS_EC_HEALTH_PORT
rpc_timeout: 5s
tiers:
  - type: ec
    cluster:
      data_shards: 4
      parity_shards: 1
      peers:
        - {id: r1, endpoint: "127.0.0.1:${REDIS_SHARD_PORTS[0]}"}
        - {id: r2, endpoint: "127.0.0.1:${REDIS_SHARD_PORTS[1]}"}
        - {id: r3, endpoint: "127.0.0.1:${REDIS_SHARD_PORTS[2]}"}
        - {id: r4, endpoint: "127.0.0.1:${REDIS_SHARD_PORTS[3]}"}
        - {id: r5, endpoint: "127.0.0.1:${REDIS_SHARD_PORTS[4]}"}
      pool: 1
      timeout: 2s
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF
"$BIN/cache-ctl" serve --config "$TMPDIR/redis-ec-tiered.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$REDIS_EC_HEALTH_PORT"
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$REDIS_EC_PORT")" \
    --output "$TMPDIR/test-redis-ec-cold.bin" --no-progress "$MKEY" 2>&1

# Prove EC fill wrote the manifest and every referenced chunk to each Redis
# shard peer.
ALL_SHARDS_PRESENT=1
for peer_port in "${REDIS_SHARD_PORTS[@]}"; do
    for pair in "${MAIN_CACHE_OBJECTS[@]}"; do
        namespace=${pair%%:*}
        object_key=${pair#*:}
        if ! wait_shard_present "127.0.0.1:$peer_port" "$namespace" "$object_key"; then
            ALL_SHARDS_PRESENT=0
        fi
    done
done
assert_eq "1" "$ALL_SHARDS_PRESENT" "EC cold fill populated every Redis shard peer"
if [ "$ALL_SHARDS_PRESENT" != "1" ]; then
    exit 1
fi

# Stop one Redis process without closing its sockets. The corresponding shard
# cache enters Redis drain after the EC coordinator obtains four fast shards
# and closes the canceled wire read. The object read must complete before the Redis command
# timeout, and normal cancellation must not reconnect the UDS data worker.
SLOW_REDIS_PID=${REDIS_SHARD_PIDS[4]}
CANCEL_BEFORE=$("$BIN/cache-ctl" info --endpoint "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[4]}" --json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["redis"]["cancelled"])')
kill -STOP "$SLOW_REDIS_PID"
SLOW_START_MS=$(date +%s%3N)
"$BIN/cache-ctl" object get --endpoint "127.0.0.1:$REDIS_EC_PORT" \
    --namespace manifest --hash "$MKEY" >/dev/null
SLOW_ELAPSED_MS=$(( $(date +%s%3N) - SLOW_START_MS ))
kill -CONT "$SLOW_REDIS_PID"
if [ "$SLOW_ELAPSED_MS" -lt 1500 ]; then
    ok "EC returns after four Redis shards without waiting for slow peer (${SLOW_ELAPSED_MS}ms)"
else
    fail "EC slow-peer read took ${SLOW_ELAPSED_MS}ms (Redis timeout is 2000ms)"
fi
CANCEL_SEEN=0
for _ in $(seq 1 50); do
    if "$BIN/cache-ctl" info --endpoint "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[4]}" --json | \
        python3 -c 'import json,sys; assert json.load(sys.stdin)["redis"]["cancelled"] > int(sys.argv[1])' "$CANCEL_BEFORE" 2>/dev/null; then
        CANCEL_SEEN=1
        break
    fi
    sleep 0.1
done
assert_eq "1" "$CANCEL_SEEN" "EC slow peer canceled its Redis operation after wire read cancellation"
DRAINED=0
for _ in $(seq 1 50); do
    if "$BIN/cache-ctl" info --endpoint "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[4]}" --json | \
        python3 -c 'import json,sys; r=json.load(sys.stdin)["redis"]; assert r["draining"] == 0 and r["reconnects"] == 0' 2>/dev/null; then
        DRAINED=1
        break
    fi
    sleep 0.1
done
assert_eq "1" "$DRAINED" "Redis cancel drain completed without backend reconnect"
wait_ready "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[4]}"

ALL_SHARDS_PRESENT=1
for peer_port in "${REDIS_SHARD_PORTS[@]}"; do
    for pair in "${MAIN_CACHE_OBJECTS[@]}"; do
        namespace=${pair%%:*}
        object_key=${pair#*:}
        if ! "$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$peer_port" \
            --namespace "$namespace" --hash "$object_key" >/dev/null 2>&1; then
            ALL_SHARDS_PRESENT=0
        fi
    done
done
assert_eq "1" "$ALL_SHARDS_PRESENT" "all Redis shards remain readable after cancellation drain"

# The warm read must now be fully satisfied by EC. Stopping the origin turns
# any accidental fallthrough into a deterministic test failure.
kill "$STORE_PID" 2>/dev/null || true
wait "$STORE_PID" 2>/dev/null || true
"$BIN/manifest-ctl" load --manifest-config "$(accel_cfg_for_cache "127.0.0.1:$REDIS_EC_PORT")" \
    --output "$TMPDIR/test-redis-ec-warm.bin" --no-progress "$MKEY" 2>&1
REDIS_EC_HASH=$(payload_hash "$TMPDIR/test-redis-ec-warm.bin")
assert_eq "$ORIG_HASH" "$REDIS_EC_HASH" "Redis shard EC warm load without origin"

# Cold-restart one Redis shard backend in place. cache-ctl stays alive, replaces
# its broken workers, and reports a confirmed miss from the empty server. EC
# reconstructs the manifest from the other four peers and repairs the cold one.
RESTARTED_REDIS_PID=${REDIS_SHARD_PIDS[0]}
kill "$RESTARTED_REDIS_PID" 2>/dev/null || true
wait "$RESTARTED_REDIS_PID" 2>/dev/null || true
REDIS_NOT_SERVING=0
for _ in $(seq 1 30); do
    if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[0]}" 2>/dev/null | grep -q NOT_SERVING; then
        REDIS_NOT_SERVING=1
        break
    fi
    sleep 0.1
done
assert_eq "1" "$REDIS_NOT_SERVING" "Redis backend failure marks cache-ctl NOT_SERVING"
start_redis redis-ec-1 "${REDIS_SHARD_TCP_PORTS[0]}"
REDIS_SHARD_PIDS[0]=$REDIS_PID
wait_ready "127.0.0.1:${REDIS_SHARD_HEALTH_PORTS[0]}"

COLD_MISS=0
for _ in $(seq 1 50); do
    COLD_OUT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:${REDIS_SHARD_PORTS[0]}" \
        --namespace manifest --hash "$MKEY" 2>&1 || true)
    if echo "$COLD_OUT" | grep -q MISS; then
        COLD_MISS=1
        break
    fi
    sleep 0.1
done
assert_eq "1" "$COLD_MISS" "restarted Redis shard is a confirmed cold miss"

REPAIRED=0
for _ in $(seq 1 100); do
    "$BIN/cache-ctl" object get --endpoint "127.0.0.1:$REDIS_EC_PORT" \
        --namespace manifest --hash "$MKEY" >/dev/null
    if "$BIN/cache-ctl" shard get --endpoint "127.0.0.1:${REDIS_SHARD_PORTS[0]}" \
        --namespace manifest --hash "$MKEY" >/dev/null 2>&1; then
        REPAIRED=1
        break
    fi
    sleep 0.05
done
assert_eq "1" "$REPAIRED" "EC repaired manifest shard after Redis cold restart"

# Remove a different backend: a successful origin-free read now necessarily
# includes the repaired shard from peer 1 among its four inputs.
SECOND_REDIS_PID=${REDIS_SHARD_PIDS[1]}
kill "$SECOND_REDIS_PID" 2>/dev/null || true
wait "$SECOND_REDIS_PID" 2>/dev/null || true
if "$BIN/cache-ctl" object get --endpoint "127.0.0.1:$REDIS_EC_PORT" \
    --namespace manifest --hash "$MKEY" >/dev/null; then
    ok "repaired Redis shard participates in 4-of-5 read"
else
    fail "4-of-5 read failed after repairing restarted Redis shard"
fi

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
