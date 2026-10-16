#!/usr/bin/env bash
# e2e-requires: []
# store-ctl(obs backend) ↔ real S3-compatible OBS round-trip.
#
# This case is opt-in by selection. Once selected, missing credentials,
# bucket/endpoint configuration, host prerequisites, or prepared products are failures.
# Required:
#   OBS_BUCKET, OBS_ENDPOINT, OBS_AK, OBS_SK
# Optional:
#   OBS_REGION, OBS_PREFIX

set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"

BIN="${BIN:?BIN is required}"

fail_case() {
    echo "FAIL: storage.obs: $*" >&2
    exit 1
}

[ -n "${OBS_BUCKET:-}" ] || fail_case "OBS_BUCKET is required"
[ -n "${OBS_ENDPOINT:-}" ] || fail_case "OBS_ENDPOINT is required"
[ -n "${OBS_AK:-}" ] || fail_case "OBS_AK is required"
[ -n "${OBS_SK:-}" ] || fail_case "OBS_SK is required"

for tool in python3 openssl dd sha256sum awk grep date rm mktemp; do
    require_command "$tool"
done
for binary in store-ctl manifest-ctl flatten-ctl; do
    require_binary "$binary"
done

WORK="$(mktemp -d /tmp/e2e-obs-XXXXXX)"
STORE_PID=""
cleanup() {
    local status=$?
    trap - EXIT
    if [ -n "$STORE_PID" ]; then
        kill "$STORE_PID" 2>/dev/null || true
        wait "$STORE_PID" 2>/dev/null || true
    fi
    if [ -n "${E2E_KEEP:-}" ]; then
        echo "kept work dir: $WORK"
    else
        rm -rf "$WORK"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

OBS_PREFIX="${OBS_PREFIX:-store-ctl-e2e/$(date +%s)-$$/}"
free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}
STORE_PORT=$(free_port)
KEY=$(openssl rand -hex 32)

cat > "$WORK/store-ctl.yaml" <<EOF
listen: 127.0.0.1:${STORE_PORT}
backend: obs
obs:
  bucket: ${OBS_BUCKET}
  prefix: ${OBS_PREFIX}
  endpoint: ${OBS_ENDPOINT}
  access_key: \${OBS_AK}
  secret_key: \${OBS_SK}
EOF
if [ -n "${OBS_REGION:-}" ]; then echo "  region: ${OBS_REGION}" >> "$WORK/store-ctl.yaml"; fi
echo "  verify_content_key: true" >> "$WORK/store-ctl.yaml"
echo "  op_timeout: 15s"          >> "$WORK/store-ctl.yaml"

echo "==> store-ctl init --generation G1"
OBS_AK="$OBS_AK" OBS_SK="$OBS_SK" \
    "$BIN/store-ctl" init --config "$WORK/store-ctl.yaml" --generation G1

cat > "$WORK/accelerator.yaml" <<EOF
manifest:
  key: "${KEY}"
store:
  endpoint: 127.0.0.1:${STORE_PORT}
  pool: 2
  timeout: 30s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

echo "==> store-ctl: backend=obs bucket=${OBS_BUCKET} prefix=${OBS_PREFIX}"
OBS_AK="$OBS_AK" OBS_SK="$OBS_SK" \
    "$BIN/store-ctl" serve --config "$WORK/store-ctl.yaml" \
    >"$WORK/store.log" 2>&1 &
STORE_PID=$!

READY=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
    if (echo >/dev/tcp/127.0.0.1/${STORE_PORT}) 2>/dev/null; then
        READY=1
        break
    fi
    kill -0 "$STORE_PID" 2>/dev/null || break
    sleep 0.3
done
if [ "$READY" -ne 1 ]; then
    echo "==> store-ctl did not become ready:" >&2
    cat "$WORK/store.log" >&2
    fail_case "store-ctl OBS backend failed to start"
fi

dd if=/dev/urandom of="$WORK/payload.bin" bs=4096 count=64 status=none
ORIG_HASH=$(sha256sum "$WORK/payload.bin" | awk '{print $1}')

"$BIN/flatten-ctl" tar stream -f "$WORK/payload.tar" "image:$WORK/payload.bin"

echo "==> manifest-ctl store"
MKEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$WORK/payload.tar")
[ ${#MKEY} -eq 64 ] || fail_case "bad manifest key length ${#MKEY}"
echo "    manifest key: $MKEY"

echo "==> manifest-ctl load (round-trip via OBS)"
"$BIN/manifest-ctl" load --manifest-config "$WORK/accelerator.yaml" \
    --output "$WORK/restored.tar" --no-progress "$MKEY"
"$BIN/flatten-ctl" tar extract -f "$WORK/restored.tar" --dense "image:$WORK/restored.bin"
RESTORED_HASH=$(sha256sum "$WORK/restored.bin" | awk '{print $1}')
[ "$ORIG_HASH" = "$RESTORED_HASH" ] || fail_case "restored hash mismatch: orig=$ORIG_HASH restored=$RESTORED_HASH"
echo "    PASS: round-trip hash matches"

echo "==> dedup: store same payload again, expect 0 new chunks"
OUT=$("$BIN/manifest-ctl" store --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$WORK/payload.tar" 2>&1)
STORED=$(echo "$OUT" | grep "chunks:" | grep -oP 'stored=\K[0-9]+' || echo "?")
[ "$STORED" = "0" ] || fail_case "expected 0 stored chunks on dedup, got '$STORED'"
echo "    PASS: dedup hit (0 new chunks on second store)"

echo "==> manifest verify (round-trip every chunk via OBS)"
"$BIN/manifest-ctl" verify --manifest-config "$WORK/accelerator.yaml" \
    --no-progress "$MKEY"

kill "$STORE_PID" 2>/dev/null || true
wait "$STORE_PID" 2>/dev/null || true
STORE_PID=""
echo "==> cleanup: store-ctl purge --all --confirm"
if OBS_AK="$OBS_AK" OBS_SK="$OBS_SK" \
        "$BIN/store-ctl" purge --config "$WORK/store-ctl.yaml" \
        --all --confirm 2>&1; then
    echo "    PASS: per-run prefix purged"
else
    echo "    WARN: cleanup failed; residual prefix: ${OBS_PREFIX}" >&2
fi

echo
echo "==> storage.obs: OK (bucket=$OBS_BUCKET prefix=$OBS_PREFIX)"
