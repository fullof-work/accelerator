#!/bin/bash
# e2e-requires: []
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
require_binary store-ctl
require_binary manifest-ctl
require_binary cache-ctl
require_binary flatten-ctl

# E2E test for store-ctl's embedded read-only cache wire server
# (cache_listen). Proves store-ctl can serve the cache wire protocol —
# chunk / manifest / blob — straight over its backend, read-only, so cache
# clients reach store content without a separate cache-ctl.
#
# Self-contained: store-ctl + manifest-ctl + cache-ctl, with flatten-ctl used
# only to write strict tarstream fixtures. Does not exercise the tiered/EC
# chain.

TMPDIR=$(mktemp -d /tmp/acc-store-cache-e2e-XXXXXX)
E2E_PORT_LEASE_FILE="$TMPDIR/ports"
[ -r "$E2E_LIB/accelerator/port_lease.sh" ] || e2e_fail "missing prepared accelerator helper: port_lease.sh"
source "$E2E_LIB/accelerator/port_lease.sh"
KEY=$(openssl rand -hex 32)

PASS=0
FAIL=0
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

ok() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }
assert_eq() {
    if [ "$1" = "$2" ]; then ok "$3"; else fail "$3 (expected '$1', got '$2')"; fi
}

# ============================================================
echo ""
echo "=== Spin up store-ctl with cache_listen ==="
STORE_PORT=$(e2e_free_port)
STORE_CACHE_PORT=$(e2e_free_port)
STORE_ROOT="$TMPDIR/store-data"
cat > "$TMPDIR/store-ctl.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
cache_listen: 127.0.0.1:$STORE_CACHE_PORT
fs:
  root: $STORE_ROOT
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$TMPDIR/store-ctl.yaml" --generation G1
"$BIN/store-ctl" serve --config "$TMPDIR/store-ctl.yaml" &
STORE_PID=$!
PIDS+=("$STORE_PID")
for _ in $(seq 1 30); do
    (echo >/dev/tcp/127.0.0.1/$STORE_PORT) 2>/dev/null && break
    sleep 0.1
done
for _ in $(seq 1 30); do
    (echo >/dev/tcp/127.0.0.1/$STORE_CACHE_PORT) 2>/dev/null && break
    sleep 0.1
done
echo "  store-ctl gRPC=127.0.0.1:$STORE_PORT cache_listen=127.0.0.1:$STORE_CACHE_PORT"
STORE_CACHE_EP="127.0.0.1:$STORE_CACHE_PORT"

# manifest-ctl config pointing at the store gRPC.
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
CFG="--manifest-config $TMPDIR/accelerator.yaml"

# Ingest a tarstream payload → chunks + a manifest land in the store.
dd if=/dev/urandom of="$TMPDIR/payload.bin" bs=1024 count=128 2>/dev/null
"$BIN/flatten-ctl" tar stream -f "$TMPDIR/artifact.tar" \
    "payload.bin:$TMPDIR/payload.bin"
MKEY=$("$BIN/manifest-ctl" store $CFG --no-progress "$TMPDIR/artifact.tar")
echo "  ingested manifest-key=$MKEY"

# Roll out G2 without restarting serve. Repeatedly store a second artifact
# until its manifest appears under G2; this proves SIGHUP changed admission
# in the running daemon. The G1 manifest above is then used by Test 1 to prove
# cache_listen shares the same newest-to-oldest lookup as gRPC Get.
"$BIN/store-ctl" rollout --config "$TMPDIR/store-ctl.yaml" --generation G2
kill -HUP "$STORE_PID"
dd if=/dev/urandom of="$TMPDIR/payload-g2.bin" bs=1024 count=32 2>/dev/null
"$BIN/flatten-ctl" tar stream -f "$TMPDIR/artifact-g2.tar" \
    "payload-g2.bin:$TMPDIR/payload-g2.bin"
G2_VISIBLE=0
for _ in $(seq 1 30); do
    "$BIN/manifest-ctl" store $CFG --no-progress "$TMPDIR/artifact-g2.tar" >/dev/null
    if find "$STORE_ROOT/manifest/G2" -type f -print -quit 2>/dev/null | grep -q .; then
        G2_VISIBLE=1
        break
    fi
    sleep 0.1
done
if [ "$G2_VISIBLE" -eq 1 ]; then
    ok "SIGHUP refresh admits new writes to G2 without restart"
else
    fail "SIGHUP refresh did not move admission to G2"
fi

# ============================================================
echo ""
echo "=== Test 1: cache_listen serves the older G1 manifest after G2 rollout ==="
# Fetch the same manifest object two ways: via the store gRPC (manifest-ctl
# get-manifest) and via store-ctl's cache wire (cache-ctl object get). Both
# read the identical stored object, so the bytes must match.
"$BIN/manifest-ctl" get-manifest $CFG --output "$TMPDIR/m-grpc.bin" "$MKEY"
H_GRPC=$(sha256sum "$TMPDIR/m-grpc.bin" | awk '{print $1}')
H_WIRE=$("$BIN/cache-ctl" object get --endpoint "$STORE_CACHE_EP" --namespace manifest --hash "$MKEY" | sha256sum | awk '{print $1}')
assert_eq "$H_GRPC" "$H_WIRE" "cache_listen manifest bytes match the store gRPC"

# ============================================================
echo ""
echo "=== Test 2: blob namespace (0x03) is wired end-to-end ==="
# No blob has been written. cache-ctl defines a cache MISS as exit status 1
# plus the exact MISS diagnostic. Any transport/backend/protocol error is a
# different result and must fail this selected product case.
ABSENT="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
set +e
BLOB_OUT=$("$BIN/cache-ctl" object get --endpoint "$STORE_CACHE_EP" --namespace blob --hash "$ABSENT" 2>&1)
BLOB_STATUS=$?
set -e
if [ "$BLOB_STATUS" -eq 1 ] && [ "$BLOB_OUT" = "MISS" ]; then
    ok "blob namespace accepted; absent key returned the expected MISS"
else
    fail "blob namespace did not return an explicit MISS (status=$BLOB_STATUS output=$BLOB_OUT)"
fi

# ============================================================
echo ""
echo "=== Test 3: read-only — writes are rejected ==="
# cache_listen is a read path; ObjectPut must be refused (writes go through
# the store gRPC). Require both a failing status and the product's rejection
# diagnostic so an unrelated command/transport failure cannot masquerade as PASS.
set +e
PUT_OUT=$("$BIN/cache-ctl" object put --endpoint "$STORE_CACHE_EP" --namespace chunk --hash "$ABSENT" --value /dev/null 2>&1)
PUT_STATUS=$?
set -e
if [ "$PUT_STATUS" -ne 0 ] && echo "$PUT_OUT" | grep -qi "not supported"; then
    ok "object put rejected (writes not supported)"
else
    fail "object put did not produce the expected read-only rejection (status=$PUT_STATUS output=$PUT_OUT)"
fi

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
