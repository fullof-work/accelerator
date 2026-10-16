#!/bin/bash
# e2e-requires: []
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
require_binary store-ctl
require_binary manifest-ctl
require_binary cache-ctl
require_binary flatten-ctl

# Prepared Store/cache object, shard and embedded-tier correctness.
# Run with the platform runner: e2e run --include storage.cache.sh.

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

# ============================================================
echo ""
echo "=== Test 0: store-ctl standalone roundtrip ==="
# Write + read a distinct manifest through manifest-ctl → store-ctl
# to prove the standalone path is healthy before any cache-ctl
# tests run. Uses a fresh 64 KiB payload so it doesn't collide with
# the main artifact's chunks.
dd if=/dev/urandom of="$TMPDIR/store0-payload.bin" bs=1024 count=64 2>/dev/null
"$BIN/flatten-ctl" tar stream -f "$TMPDIR/store0.bin" \
    "store0-payload.bin:$TMPDIR/store0-payload.bin"
MKEY0=$("$BIN/manifest-ctl" store $COMMON --no-progress "$TMPDIR/store0.bin")
"$BIN/manifest-ctl" get-manifest $COMMON --output "$TMPDIR/store0.manifest.rt" "$MKEY0" 2>&1
"$BIN/manifest-ctl" load $COMMON --output "$TMPDIR/store0.rt" --no-progress "$MKEY0" 2>&1
H_ORIG=$(payload_hash "$TMPDIR/store0.bin")
H_RT=$(payload_hash "$TMPDIR/store0.rt")
assert_eq "$H_ORIG" "$H_RT" "store-ctl standalone store + get-manifest + load roundtrip (one-step store)"

# ============================================================
echo ""
echo "=== Test 1: local mode — object put/get roundtrip ==="
LOCAL_PORT=$(e2e_free_port)
LOCAL_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/local.yaml" <<EOF
mode: local
type: embedded
listen: 127.0.0.1:$LOCAL_PORT
health_listen: 127.0.0.1:$LOCAL_HEALTH_PORT
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-local
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/local.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$LOCAL_HEALTH_PORT"

TEST_HASH="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
echo -n "test-value-local" | "$BIN/cache-ctl" object put --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" --value -
GOT=$("$BIN/cache-ctl" object get --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH")
assert_eq "test-value-local" "$GOT" "local object put/get roundtrip"

# ============================================================
echo ""
echo "=== Test 2: local mode — shard put/get also works (unified handler) ==="
# shard put attaches [idx][total] prefix to the raw data before sending;
# shard get parses the prefix off and echoes only the raw body to stdout,
# so the assertion matches the input verbatim.
echo -n "shard-on-local" | "$BIN/cache-ctl" shard put --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" --idx 0 --total 5 --value -
GOT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$LOCAL_PORT" --namespace chunk --hash "$TEST_HASH" 2>/dev/null)
assert_eq "shard-on-local" "$GOT" "shard put/get on local mode (unified handler)"

# ============================================================
echo ""
echo "=== Test 3: shard mode — shard put/get roundtrip ==="
SHARD_PORT=$(e2e_free_port)
SHARD_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/shard.yaml" <<EOF
mode: shard
type: embedded
listen: 127.0.0.1:$SHARD_PORT
health_listen: 127.0.0.1:$SHARD_HEALTH_PORT
rpc_timeout: 2s
freq:
  counters: 1M
  reset_after: 100K
rocks:
  path: $TMPDIR/rocks-shard
  disk_bytes: 1GiB
  mem_ratio: 0.05
  direct_reads: false
  bloom_bits: 10
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/shard.yaml" &
PIDS+=($!)
wait_ready "127.0.0.1:$SHARD_HEALTH_PORT"

echo -n "shard-data-idx3" | "$BIN/cache-ctl" shard put --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" --idx 3 --total 5 --value -
GOT=$("$BIN/cache-ctl" shard get --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" 2>/dev/null)
assert_eq "shard-data-idx3" "$GOT" "shard put/get roundtrip"

# ============================================================
echo ""
echo "=== Test 4: shard mode — object put/get also works (unified handler) ==="
echo -n "obj-on-shard" | "$BIN/cache-ctl" object put --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH" --value -
GOT=$("$BIN/cache-ctl" object get --endpoint "127.0.0.1:$SHARD_PORT" --namespace chunk --hash "$TEST_HASH")
assert_eq "obj-on-shard" "$GOT" "object put/get on shard mode (unified handler)"

# ============================================================
echo ""
echo "=== Test 5: tiered mode (embedded only) — manifest-ctl load through cache ==="
start_store_reader embedded-origin
TIERED_ORIGIN_PORT=$STORE_READER_PORT
TIERED_ORIGIN_PID=$STORE_READER_PID
TIERED_PORT=$(e2e_free_port)
TIERED_HEALTH_PORT=$(e2e_free_port)
cat > "$TMPDIR/tiered.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$TIERED_PORT
health_listen: 127.0.0.1:$TIERED_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $TMPDIR/rocks-tiered
      disk_bytes: 1GiB
      mem_ratio: 0.05
      direct_reads: false
      bloom_bits: 10
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$TIERED_ORIGIN_PORT
    pool: 2
    timeout: 2s
  max_inflight: 16
EOF

"$BIN/cache-ctl" serve --config "$TMPDIR/tiered.yaml" &
TIERED_PID=$!
PIDS+=($TIERED_PID)
wait_ready "127.0.0.1:$TIERED_HEALTH_PORT"
TIERED_MANIFEST_CONFIG=$(accel_cfg_for_cache "127.0.0.1:$TIERED_PORT")

# Load via cache (first read — cold, fills embedded from origin).
"$BIN/manifest-ctl" load --manifest-config "$TIERED_MANIFEST_CONFIG" \
    --output "$TMPDIR/test-cached.bin" --no-progress "$MKEY" 2>&1
CACHED_HASH=$(payload_hash "$TMPDIR/test-cached.bin")
assert_eq "$ORIG_HASH" "$CACHED_HASH" "tiered load roundtrip (cold)"
kill "$TIERED_ORIGIN_PID" 2>/dev/null || true
wait "$TIERED_ORIGIN_PID" 2>/dev/null || true
if wait_manifest_load "$TIERED_MANIFEST_CONFIG" "$TMPDIR/test-embedded-probe.bin" \
    "$MKEY" "$ORIG_HASH"; then
    ok "tiered embedded cold fill became readable with origin stopped"
else
    fail "tiered embedded cold fill did not become readable with origin stopped"
    exit 1
fi

# ============================================================
echo ""
echo "=== Test 6: tiered mode — warm read (second load hits embedded cache) ==="
"$BIN/manifest-ctl" load --manifest-config "$TIERED_MANIFEST_CONFIG" \
    --output "$TMPDIR/test-warm.bin" --no-progress "$MKEY" 2>&1
WARM_HASH=$(payload_hash "$TMPDIR/test-warm.bin")
assert_eq "$ORIG_HASH" "$WARM_HASH" "tiered embedded warm load without origin"

# ============================================================
echo ""
echo "=== Test 7: tiered mode — object put returns error (writes not supported) ==="
if PUT_OUT=$("$BIN/cache-ctl" object put --endpoint "127.0.0.1:$TIERED_PORT" --namespace chunk --hash "$TEST_HASH" --value /dev/null 2>&1); then
    fail "object put on tiered mode unexpectedly succeeded (got: $PUT_OUT)"
elif grep -Fq "writes not supported" <<<"$PUT_OUT"; then
    ok "object put on tiered mode rejects writes"
else
    fail "object put on tiered mode returned an unrelated error (got: $PUT_OUT)"
fi
kill "$TIERED_PID" 2>/dev/null || true
wait "$TIERED_PID" 2>/dev/null || true

# ============================================================

echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="
[ "$FAIL" -eq 0 ]
