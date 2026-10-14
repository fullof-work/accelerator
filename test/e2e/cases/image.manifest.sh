#!/bin/bash
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
ACCELERATOR_LIB="$E2E_LIB/accelerator"

# E2E test for manifest-ctl and flatten-ctl using local Docker archives.

IMAGE_A="${IMAGE_A:-}"
IMAGE_B="${IMAGE_B:-}"

BIN="${BIN:?BIN is required}"

die() { echo "  ERROR: $*" >&2; exit 1; }

# Check the assembled tree and host tools before starting any service.
for tool in python3 openssl unzip stat sha256sum awk grep sed dd od tr truncate \
    flock mktemp mkdir rm cat head sleep dirname readlink env; do
    command -v "$tool" >/dev/null || die "required tool not found: $tool"
done
for binary in store-ctl manifest-ctl flatten-ctl; do
    [ -x "$BIN/$binary" ] || die "required executable not found: $BIN/$binary (set BIN to the assembled platform binaries)"
done
BIN="$(cd "$BIN" && pwd)"
for helper in manifest_fixture.py port_lease.sh; do
    [ -r "$ACCELERATOR_LIB/$helper" ] || die "required prepared accelerator helper missing: $ACCELERATOR_LIB/$helper"
done
SUDO=()
if [ "$EUID" -ne 0 ]; then
    command -v sudo >/dev/null || die "sudo is required to preserve image ownership during flatten export"
    sudo -n true || die "flatten export requires root or noninteractive sudo"
    SUDO=(sudo -n)
fi
if [ -n "$IMAGE_A" ] || [ -n "$IMAGE_B" ]; then
    command -v docker >/dev/null || die "Docker is required only for explicit IMAGE_A/IMAGE_B overrides"
fi

TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/acc-manifest-e2e-XXXXXX")

PASS=0
FAIL=0
STORE_PID=""

cleanup() {
    local status=$? attempt
    trap - EXIT
    trap '' INT TERM
    if [ -n "$STORE_PID" ]; then
        kill -TERM "$STORE_PID" 2>/dev/null || true
        # Allow three seconds for graceful shutdown, then reap after KILL.
        for ((attempt = 0; attempt < 30; attempt++)); do
            kill -0 "$STORE_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$STORE_PID" 2>/dev/null; then
            kill -KILL "$STORE_PID" 2>/dev/null || true
        fi
        wait "$STORE_PID" 2>/dev/null || true
    fi
    # An interrupted privileged flatten may leave root-owned scratch entries.
    if ! rm -rf -- "$TMPDIR" 2>/dev/null; then
        "${SUDO[@]}" rm -rf -- "$TMPDIR" || { [ "$status" -ne 0 ] || status=1; }
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

E2E_PORT_LEASE_FILE="$TMPDIR/ports"
# shellcheck source=/dev/null
source "$ACCELERATOR_LIB/port_lease.sh"
KEY=$(openssl rand -hex 32)
mkdir -p "$TMPDIR/home" "$TMPDIR/docker-config" "$TMPDIR/flatten"

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

# Explicit overrides use only the local daemon's cached images. A clean
# environment prevents loading caller HOME, Docker contexts or credentials.
cached_archive() {
    local name=$1 img=$2 output=$3
    local docker_cmd=(env -i "PATH=$PATH" "HOME=$TMPDIR/home" "DOCKER_CONFIG=$TMPDIR/docker-config" docker)
    if ! "${docker_cmd[@]}" image inspect "$img" >/dev/null 2>&1; then
        die "$name=$img is not cached in the local Docker daemon, or the daemon is unavailable; pre-cache it explicitly (no pull is performed)"
    fi
    if ! "${docker_cmd[@]}" save "$img" > "$output"; then
        die "could not save cached $name=$img"
    fi
}

flatten_archive() {
    # Only the owned scratch/config paths cross sudo; the CLI links its generator.
    "${SUDO[@]}" env -i PATH=/usr/bin:/bin "HOME=$TMPDIR/home" \
        "DOCKER_CONFIG=$TMPDIR/docker-config" "TMPDIR=$TMPDIR/flatten" \
        "$BIN/flatten-ctl" export --output "$2" --no-progress < "$1" 2>&1
}

echo ""
echo "=== Prepare local image archives ==="
if [ -z "$IMAGE_A" ] || [ -z "$IMAGE_B" ]; then
    if [ -n "${MANIFEST_FIXTURE_DIR:-}" ]; then
        for variant in a b; do
            [ -s "$MANIFEST_FIXTURE_DIR/image-$variant.tar" ] || die "prepared manifest fixture is missing: $variant"
            cp "$MANIFEST_FIXTURE_DIR/image-$variant.tar" "$TMPDIR/image-$variant.tar"
        done
    else
        [ "${KUASAR_ARTIFACT_E2E:-0}" != 1 ] || die "MANIFEST_FIXTURE_DIR is required for artifact E2E"
        python3 "$ACCELERATOR_LIB/manifest_fixture.py" "$TMPDIR"
    fi
fi
[ -z "$IMAGE_A" ] || cached_archive IMAGE_A "$IMAGE_A" "$TMPDIR/image-a.tar"
[ -z "$IMAGE_B" ] || cached_archive IMAGE_B "$IMAGE_B" "$TMPDIR/image-b.tar"
for archive in "$TMPDIR/image-a.tar" "$TMPDIR/image-b.tar"; do
    [ -s "$archive" ] || die "required image archive missing or empty: $archive"
done
IMAGE_A="${IMAGE_A:-local fixture A}"
IMAGE_B="${IMAGE_B:-local fixture B}"

# ============================================================
# Spin up store-ctl sidecar. manifest-ctl no longer touches the
# filesystem directly — every chunk / manifest I/O goes through
# this daemon's gRPC endpoint.
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
"$BIN/store-ctl" serve --config "$TMPDIR/store-ctl.yaml" >"$TMPDIR/store.log" 2>&1 &
STORE_PID=$!
# Bounded service readiness; the former 500ms probe did not fail on timeout
# and subsequent RPCs waited longer. Do not make that probe a new hard deadline.
STORE_READY=0
for ((attempt = 0; attempt < 50; attempt++)); do
    kill -0 "$STORE_PID" 2>/dev/null || break
    if (echo >/dev/tcp/127.0.0.1/"$STORE_PORT") 2>/dev/null; then
        STORE_READY=1
        break
    fi
    sleep 0.1
done
if [ "$STORE_READY" -ne 1 ]; then
    echo "  ERROR: store-ctl did not become ready on 127.0.0.1:$STORE_PORT within 5 seconds" >&2
    cat "$TMPDIR/store.log" >&2
    exit 1
fi
echo "  store-ctl listen=127.0.0.1:$STORE_PORT root=$STORE_ROOT"

# Write manifest-ctl config (YAML, points at store-ctl's gRPC endpoint)
cat > "$TMPDIR/accelerator.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 10s
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

# Manifest config no longer accepts CLI flag overrides (--chunk-mode,
# --crypto-chunk, etc. were removed). Pre-render the alternate configs
# the test cases below need, and pass --manifest-config explicitly per case.
cat > "$TMPDIR/accelerator-fixed.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 10s
chunker:
  mode: fixed
  fixed:
    size: 512KiB
crypto:
  chunk: aes
  manifest: aes
EOF

cat > "$TMPDIR/accelerator-fixed-64k.yaml" <<EOF
manifest:
  key: "$KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 2
  timeout: 10s
chunker:
  mode: fixed
  fixed:
    size: 64KiB
crypto:
  chunk: aes
  manifest: aes
EOF

COMMON=(--manifest-config "$TMPDIR/accelerator.yaml")

# ============================================================
echo ""
echo "=== Test 1: Flatten $IMAGE_A ==="
# flatten-ctl export preserves image file ownership, which needs root/CAP_CHOWN.
# Escalate just this business operation so the platform runner need not wrap the
# entire case in sudo.
flatten_archive "$TMPDIR/image-a.tar" "$TMPDIR/image-a.erofs"
SIZE=$(stat --printf="%s" "$TMPDIR/image-a.erofs" 2>/dev/null || stat -f "%z" "$TMPDIR/image-a.erofs")
if [ "$SIZE" -gt 0 ]; then
    ok "flatten produced $SIZE bytes"
else
    fail "flatten produced empty output"
fi

# Trailing ZIP must contain config.json (standard unzip works because
# ZIP EOCD scan tolerates the EROFS prefix). Note: unzip exits 1 with
# a warning about the prefix data but still produces a valid listing —
# capture into a variable to neutralise pipefail interaction.
ZIP_LIST=$(unzip -l "$TMPDIR/image-a.erofs" 2>/dev/null || true)
if echo "$ZIP_LIST" | grep -q '\bconfig\.json\b'; then
    ok "trailing ZIP contains config.json"
else
    fail "trailing ZIP missing or no config.json entry"
fi

# `flatten-ctl info --json` should report a valid erofs_size and a
# non-null Architecture (Docker images we test with always carry it).
INFO_JSON=$("$BIN/flatten-ctl" info --json "$TMPDIR/image-a.erofs")
EROFS_SIZE=$(echo "$INFO_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["erofs_size"])')
ARCH=$(echo "$INFO_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["config"].get("Architecture",""))')
if [ "$EROFS_SIZE" -gt 0 ] && [ "$EROFS_SIZE" -lt "$SIZE" ]; then
    ok "info --json: erofs_size=$EROFS_SIZE < total file size $SIZE (ZIP appended after EROFS)"
else
    fail "info --json: erofs_size=$EROFS_SIZE inconsistent with file size $SIZE"
fi
if [ -n "$ARCH" ]; then
    ok "info --json: Architecture=$ARCH"
else
    fail "info --json: Architecture is empty"
fi

# flatten now emits a tarstream artifact (tar header + EROFS payload + trailing ZIP,
# ff88f5f), so the EROFS no longer starts at offset 0 — a bare truncate won't expose
# it. Extract the payload (entry "image") and verify the EROFS magic at offset 1024
# of the pure erofs.
"$BIN/flatten-ctl" tar extract -f "$TMPDIR/image-a.erofs" --dense --no-chown "image:$TMPDIR/image-a.pure.erofs"
MAGIC_HEX=$(dd if="$TMPDIR/image-a.pure.erofs" bs=1 count=4 skip=1024 2>/dev/null | od -An -tx1 | tr -d ' \n')
if [ "$MAGIC_HEX" = "e2e1f5e0" ]; then
    ok "extracted EROFS payload retains magic at offset 1024"
else
    fail "EROFS magic missing in extracted payload (got: $MAGIC_HEX)"
fi

# ============================================================
echo ""
echo "=== Test 2: Store + roundtrip ==="
# store ingests the image, uploads its manifest, and prints the hex
# manifest content key on stdout (the summary goes to stderr).
MKEY_A=$("$BIN/manifest-ctl" store "${COMMON[@]}" --no-progress "$TMPDIR/image-a.erofs")
echo "  image-a manifest key: $MKEY_A"
"$BIN/manifest-ctl" load "${COMMON[@]}" --output "$TMPDIR/image-a-restored.erofs" --no-progress "$MKEY_A"

H1=$(sha256sum "$TMPDIR/image-a.erofs" | awk '{print $1}')
H2=$(sha256sum "$TMPDIR/image-a-restored.erofs" | awk '{print $1}')
assert_eq "$H1" "$H2" "store → load roundtrip matches"

# ============================================================
echo ""
echo "=== Test 3: Dedup (same image stored twice) ==="
OUTPUT=$("$BIN/manifest-ctl" store "${COMMON[@]}" --no-progress "$TMPDIR/image-a.erofs" 2>&1)
STORED=$(echo "$OUTPUT" | grep "chunks:" | grep -oP 'stored=\K[0-9]+')
if [ "$STORED" = "0" ]; then
    ok "100% dedup on second store (0 new chunks)"
else
    fail "expected 0 stored chunks on second store, got $STORED"
fi

# ============================================================
echo ""
echo "=== Test 4: Info (via manifest:// fetch) ==="
OUTPUT=$("$BIN/manifest-ctl" info "${COMMON[@]}" "manifest://$MKEY_A" 2>&1)
if echo "$OUTPUT" | grep -q "chunk mode"; then
    ok "info shows chunk mode"
else
    fail "info missing chunk mode"
fi
if echo "$OUTPUT" | grep -q "chunk count"; then
    ok "info shows chunk count"
else
    fail "info missing chunk count"
fi

# ============================================================
echo ""
echo "=== Test 5: Verify ==="
OUTPUT=$("$BIN/manifest-ctl" verify "${COMMON[@]}" --no-progress "$MKEY_A" 2>&1)
if echo "$OUTPUT" | grep -q "failed: 0"; then
    ok "verify passed with 0 failures"
else
    fail "verify reported failures: $OUTPUT"
fi

# ============================================================
echo ""
echo "=== Test 6: get-manifest (idempotent blob retrieval + materialize) ==="
# store already uploaded the manifest; get-manifest pulls the raw blob
# back by key. Fetching twice must yield byte-identical blobs. The
# first copy doubles as the on-disk manifest used by the diff tests.
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/image-a.manifest" "$MKEY_A"
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/image-a-again.manifest" "$MKEY_A"
HM1=$(sha256sum "$TMPDIR/image-a.manifest" | awk '{print $1}')
HM2=$(sha256sum "$TMPDIR/image-a-again.manifest" | awk '{print $1}')
assert_eq "$HM1" "$HM2" "get-manifest twice yields byte-identical blob"

# ============================================================
echo ""
echo "=== Test 7: get-manifest | info - (stdin manifest inspection) ==="
# load takes a key, not a manifest blob, so the old get-manifest|load
# pipe no longer applies. Pipe the fetched blob into info's stdin path
# instead to prove it is a well-formed manifest.
OUTPUT=$("$BIN/manifest-ctl" get-manifest "${COMMON[@]}" "$MKEY_A" | "$BIN/manifest-ctl" info -)
if echo "$OUTPUT" | grep -q "chunk count"; then
    ok "get-manifest | info - parses the piped manifest blob"
else
    fail "info - failed to parse piped manifest blob"
fi

# ============================================================
echo ""
echo "=== Test 8: Cross-image diff ($IMAGE_A vs $IMAGE_B) ==="
flatten_archive "$TMPDIR/image-b.tar" "$TMPDIR/image-b.erofs"
MKEY_B=$("$BIN/manifest-ctl" store "${COMMON[@]}" --no-progress "$TMPDIR/image-b.erofs")
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/image-b.manifest" "$MKEY_B"
OUTPUT=$("$BIN/manifest-ctl" diff "$TMPDIR/image-a.manifest" "$TMPDIR/image-b.manifest" 2>&1)
echo "  $OUTPUT" | head -5
if echo "$OUTPUT" | grep -q "shared"; then
    ok "diff shows shared chunks"
else
    fail "diff missing shared info"
fi

# ============================================================
echo ""
echo "=== Test 9: Fixed chunking vs CDC ==="
MKEY_FIXED=$("$BIN/manifest-ctl" store --manifest-config "$TMPDIR/accelerator-fixed.yaml" --no-progress "$TMPDIR/image-a.erofs")
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/image-a-fixed.manifest" "$MKEY_FIXED"
OUTPUT=$("$BIN/manifest-ctl" diff "$TMPDIR/image-a.manifest" "$TMPDIR/image-a-fixed.manifest" 2>&1)
echo "  $OUTPUT" | head -5
ok "CDC vs fixed diff completed"

# ============================================================
echo ""
echo "=== Test 10: stdin store + load roundtrip ==="
# store reads the image from stdin (no positional input) and prints the
# manifest key; load reconstructs by key. This is the one-step model —
# no separate put-manifest stage.
# Exercise a pipe (streaming input), not a seekable file redirected to stdin.
# shellcheck disable=SC2002
MKEY_PIPE=$(cat "$TMPDIR/image-a.erofs" | "$BIN/manifest-ctl" store "${COMMON[@]}" --no-progress)
echo "  stdin-store manifest key: $MKEY_PIPE"
"$BIN/manifest-ctl" load "${COMMON[@]}" --output "$TMPDIR/image-a-fullpipe.erofs" --no-progress "$MKEY_PIPE"

H5=$(sha256sum "$TMPDIR/image-a-fullpipe.erofs" | awk '{print $1}')
assert_eq "$H1" "$H5" "stdin store + load roundtrip matches original"

# ============================================================
echo ""
echo "=== Test 11: Sparse file roundtrip (zero-block optimization) ==="
# 8 MiB sparse file: 1 MiB random data + 7 MiB zeros. Verify the
# all-zero optimization path: most chunks are IsZero, manifest size is
# small (compressed key table), and round-trip reconstructs identical
# bytes without ever encrypting/storing/fetching the zero chunks.
SPARSE="$TMPDIR/sparse.bin"
dd if=/dev/urandom of="$SPARSE" bs=1M count=1 status=none
dd if=/dev/zero    of="$SPARSE" bs=1M count=7 seek=1 conv=notrunc status=none
SPARSE_HASH=$(sha256sum "$SPARSE" | awk '{print $1}')

# The store consumes platform tarstream artifacts, not bare files (ff88f5f), so
# package the sparse file first (flatten-ctl tar stream, pure Go). The chunker still
# detects the all-zero content chunks (IsZero) during ingest.
"$BIN/flatten-ctl" tar stream -f "$SPARSE.tar" "image:$SPARSE"
# Use fixed chunking so we can predict counts: 8 MiB / 64 KiB = 128 chunks total.
MKEY_SPARSE=$("$BIN/manifest-ctl" store --manifest-config "$TMPDIR/accelerator-fixed-64k.yaml" --no-progress \
    "$SPARSE.tar" 2>"$TMPDIR/sparse-store.stderr")

# Parse the "chunks: stored=S dedup=D zero=W" summary line.
SUMMARY=$(grep -E '^chunks:' "$TMPDIR/sparse-store.stderr" | head -1)
ZERO_W=$(echo "$SUMMARY" | sed -E 's/.*zero=([0-9]+).*/\1/')
STORED_S=$(echo "$SUMMARY" | sed -E 's/.*stored=([0-9]+).*/\1/')
echo "  sparse store summary: $SUMMARY"
if [ -z "$ZERO_W" ] || [ "$ZERO_W" -lt 100 ]; then
    fail "expected ≥100 zero chunks for 7/8 MiB sparse file (got: '$ZERO_W')"
else
    ok "sparse ingest: $ZERO_W zero chunks detected, $STORED_S stored"
fi

# Materialize the manifest blob; info must report the same zero count.
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/sparse.manifest" "$MKEY_SPARSE"
"$BIN/manifest-ctl" info "$TMPDIR/sparse.manifest" > "$TMPDIR/sparse-info.txt"
INFO_ZERO=$(grep -E '^zero chunks:' "$TMPDIR/sparse-info.txt" | sed -E 's/zero chunks:\s+([0-9]+).*/\1/')
assert_eq "$ZERO_W" "$INFO_ZERO" "info reports same zero count as store summary"

# Round-trip: load the artifact, extract its payload (--dense materializes the
# zero chunks), and compare SHA256 to the original.
"$BIN/manifest-ctl" load "${COMMON[@]}" --no-progress --output "$TMPDIR/sparse.rt.tar" "$MKEY_SPARSE"
"$BIN/flatten-ctl" tar extract -f "$TMPDIR/sparse.rt.tar" --dense --no-chown "image:$TMPDIR/sparse.rt"
RT_HASH=$(sha256sum "$TMPDIR/sparse.rt" | awk '{print $1}')
assert_eq "$SPARSE_HASH" "$RT_HASH" "sparse file payload roundtrip matches"

# Manifest size sanity: with 128 chunks total but only ~16 non-zero
# (1 MiB / 64 KiB), the sealed key table holds ~16*32 bytes of key
# material instead of 128*32 — manifest is significantly smaller.
MANIFEST_SIZE=$(stat -c %s "$TMPDIR/sparse.manifest" 2>/dev/null || stat -f %z "$TMPDIR/sparse.manifest")
echo "  sparse manifest size: $MANIFEST_SIZE bytes (with key-table compression)"
# 128 * 56 (entry) + 64 (header) + 16 * 32 (compressed keys) + AEAD overhead ≈ 7800
# Without compression would be 128 * 32 = 4096 instead of 512 in keys,
# so ~3.5 KiB delta. We assert manifest < 12 KiB as a loose bound.
if [ "$MANIFEST_SIZE" -gt 12288 ]; then
    fail "sparse manifest unexpectedly large: $MANIFEST_SIZE bytes"
else
    ok "sparse manifest size within bound (≤12 KiB)"
fi

# ============================================================
echo ""
echo "=== Test 12: Sparse file with holes (envelope-borne hole metadata) ==="
# 8 MiB sparse file: 1 MiB random + 7 MiB hole. truncate creates a
# real filesystem hole (not zero-fill); dd at offset 0 writes the
# leading data without touching the trailing hole region.
HOLED="$TMPDIR/holed.img"
truncate -s 8M "$HOLED"
dd if=/dev/urandom of="$HOLED" bs=1M count=1 conv=notrunc status=none
HOLED_HASH=$(sha256sum "$HOLED" | awk '{print $1}')

# Sanity: the source actually has a sparse tail (block count × 512 < apparent size).
HOLED_BLOCKS=$(stat -c '%b' "$HOLED")
HOLED_BSIZE=$(stat -c '%B' "$HOLED")
HOLED_ALLOC=$((HOLED_BLOCKS * HOLED_BSIZE))
echo "  source: apparent=8MiB allocated=$HOLED_ALLOC bytes"

# Package the sparse file: flatten-ctl tar stream captures the filesystem hole
# (SEEK_HOLE) into the artifact envelope, so the store records it as authoritative
# hole metadata from the envelope — no content scan. Fixed chunking for
# predictable counts.
"$BIN/flatten-ctl" tar stream -f "$HOLED.tar" "image:$HOLED"
MKEY_HOLED=$("$BIN/manifest-ctl" store --manifest-config "$TMPDIR/accelerator-fixed-64k.yaml" --no-progress \
    "$HOLED.tar" 2>"$TMPDIR/holed-store.stderr")

# info is the source of truth for hole extents: the store summary
# carries no holes line, so we materialize the manifest and read its
# holes count back — proving the store recorded the envelope hole at
# ingest (the tar stream carried it from SEEK_HOLE, no content scan).
"$BIN/manifest-ctl" get-manifest "${COMMON[@]}" --output "$TMPDIR/holed.manifest" "$MKEY_HOLED"
"$BIN/manifest-ctl" info "$TMPDIR/holed.manifest" > "$TMPDIR/holed-info.txt"
INFO_HOLES=$(grep -E '^holes:' "$TMPDIR/holed-info.txt" | sed -E 's/holes:\s+([0-9]+).*/\1/')
if [ "$INFO_HOLES" -lt 1 ]; then
    fail "info shows holes=$INFO_HOLES, expected ≥1 (envelope hole not recorded)"
else
    ok "store recorded $INFO_HOLES envelope hole extent(s) (via info)"
fi

# Round-trip: load the artifact and extract its payload. load emits a tarstream
# artifact carrying the hole in its envelope, and the extractor chooses
# materialization: --dense fills zeros (bytes-equal to the source, since a hole
# reads as zeros), the default punches a sparse file. The sparse-output (punch)
# behavior is covered by flatten-ctl's own extract tests; here we assert the
# byte-exact payload roundtrip.
"$BIN/manifest-ctl" load "${COMMON[@]}" --no-progress --output "$TMPDIR/holed.rt.tar" "$MKEY_HOLED"
"$BIN/flatten-ctl" tar extract -f "$TMPDIR/holed.rt.tar" --dense --no-chown "image:$TMPDIR/holed.rt"
RT_HASH=$(sha256sum "$TMPDIR/holed.rt" | awk '{print $1}')
assert_eq "$HOLED_HASH" "$RT_HASH" "sparse-hole payload roundtrip matches (holes materialized as zeros)"

# ============================================================
echo ""
echo "========================================="
echo "Results: $PASS passed, $FAIL failed"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
