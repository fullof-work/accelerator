#!/usr/bin/env bash

set -euo pipefail

# Archive fixtures target x86_64 even when this gate runs on an ARM host.
# Validators run natively; payload fixtures are inspected, never executed.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "test-release: $*" >&2
  exit 1
}

# shellcheck source=scripts/release-materials.sh
source "$ROOT/scripts/release-materials.sh"

bash "$ROOT/scripts/test-release-materials.sh"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-selected-go-materials.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-go-environment.py"
bash "$ROOT/scripts/test-release-license-traversal.sh"
bash "$ROOT/scripts/test-release-cleanup.sh"
GOWORK=off go test -race "$ROOT/scripts/release-archive-validator.go" "$ROOT/scripts/release-archive-validator_test.go"
bash "$ROOT/deps/test-common.sh"
bash "$ROOT/scripts/test-release-native-materials.sh"
bash "$ROOT/scripts/test-release-rpm-enumeration.sh"
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/scripts/test-release-validator-environment.py"

init_fixture_repo() {
  local directory="$1"
  shift
  git -C "$directory" init -q
  git -C "$directory" config --local user.name "Chen Xiaohui"
  git -C "$directory" config --local user.email "graych@gmail.com"
  git -C "$directory" add -- "$@"
  git -C "$directory" commit -q -m "test: create release source fixture"
  git -C "$directory" rev-parse HEAD
}

mkdir -p "$TMP/git-source" \
  "$TMP/material-hash/share/licenses/hash-test/LICENSES" \
  "$TMP/material-hash/share/sources/hash-test"
printf 'fixture license\n' > "$TMP/git-source/LICENSE"
fixture_git_sha="$(init_fixture_repo "$TMP/git-source" LICENSE)"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = "git:$fixture_git_sha" ] \
  || fail "untagged source was recorded as a component release"
git -C "$TMP/git-source" tag v1.2.3 "$fixture_git_sha"
[ "$(release_materials_git_version "$TMP/git-source" v1.2.3 "$fixture_git_sha")" = v1.2.3 ] \
  || fail "matching source tag was not retained"
if (release_materials_git_version "$TMP/git-source" v1.2.3 \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "source version resolver accepted a tag for another commit"
fi
[ "$(release_materials_resolve_git_source "$TMP/git-source" "$fixture_git_sha" fixture)" = "$fixture_git_sha" ] \
  || fail "clean source worktree did not resolve to its selected commit"
if (release_materials_resolve_git_source "$TMP/git-source" \
  0000000000000000000000000000000000000000 fixture >/dev/null 2>&1); then
  fail "source resolver accepted a commit that differs from the selected commit"
fi
printf 'untracked source\n' > "$TMP/git-source/untracked.go"
if (release_materials_resolve_git_source "$TMP/git-source" "" fixture >/dev/null 2>&1); then
  fail "source resolver accepted a dirty source worktree"
fi
printf 'LICENSE.generated\n' > "$TMP/git-source/.git/info/exclude"
printf 'ignored material\n' > "$TMP/git-source/LICENSE.generated"
if (
  release_materials_init "$TMP/ignored-material/stage" "$TMP/ignored-material/work" fixture
  release_materials_copy_licenses "$TMP/git-source" project >/dev/null 2>&1
); then
  fail "license collection accepted material absent from the selected commit"
fi
printf 'nested license manifest\n' \
  > "$TMP/material-hash/share/licenses/hash-test/LICENSES/MATERIALS.sha256"
printf 'generated inventory\n' \
  > "$TMP/material-hash/share/sources/hash-test/MATERIALS.sha256"
release_materials_hash_tree "$TMP/material-hash" hash-test "$TMP/material-hash-actual"
grep -Fq 'share/licenses/hash-test/LICENSES/MATERIALS.sha256' "$TMP/material-hash-actual" \
  || fail "license file named MATERIALS.sha256 was omitted from the material inventory"
if grep -Fq 'share/sources/hash-test/MATERIALS.sha256' "$TMP/material-hash-actual"; then
  fail "generated material inventory included itself"
fi

material_root="$TMP/material-validation"
material_unit=validation
mkdir -p "$material_root/bin"
printf 'payload\n' > "$material_root/bin/tool"
mkdir -p "$material_root/share/licenses/$material_unit/project" \
  "$material_root/share/sources/$material_unit" "$TMP/material-validation-work"
printf 'fixture license\n' > "$material_root/share/licenses/$material_unit/project/LICENSE"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\tshare/licenses/%s/project\n' \
    "$material_unit"
} > "$material_root/share/sources/$material_unit/SOURCES.tsv"
printf 'payload\trecord\tname\tversion_or_value\tchecksum\n' \
  > "$material_root/share/sources/$material_unit/GO-BUILD-INFO.tsv"
printf 'module\tversion\tchecksum\n' \
  > "$material_root/share/sources/$material_unit/GO-MODULES.tsv"
release_materials_hash_tree "$material_root" "$material_unit" \
  "$material_root/share/sources/$material_unit/MATERIALS.sha256"
find "$material_root/share" -type d -exec chmod 0755 {} +
find "$material_root/share" -type f -exec chmod 0644 {} +
(
  WORK="$TMP/material-validation-work"
  release_materials_validate "$material_root" "$material_unit"
)

cp -a "$material_root" "$TMP/material-unsafe-license-path"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\t../../../etc\n'
} > "$TMP/material-unsafe-license-path/share/sources/$material_unit/SOURCES.tsv"
release_materials_hash_tree "$TMP/material-unsafe-license-path" "$material_unit" \
  "$TMP/material-unsafe-license-path/share/sources/$material_unit/MATERIALS.sha256"
chmod 0644 "$TMP/material-unsafe-license-path/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-unsafe-license-path" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a license directory outside its unit"
fi

cp -a "$material_root" "$TMP/material-invalid-record"
{
  printf 'payload\tname\tversion\tsource\tintegrity\tlicense_directory\n'
  printf 'bin/tool\tfixture\tv1.0.0\thttps://example.invalid/source.tar.gz\tsha256:fixture\n'
} > "$TMP/material-invalid-record/share/sources/$material_unit/SOURCES.tsv"
release_materials_hash_tree "$TMP/material-invalid-record" "$material_unit" \
  "$TMP/material-invalid-record/share/sources/$material_unit/MATERIALS.sha256"
chmod 0644 "$TMP/material-invalid-record/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-invalid-record" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a SOURCES.tsv row with fewer than six fields"
fi

cp -a "$material_root" "$TMP/material-unsafe-parent-mode"
chmod 0777 "$TMP/material-unsafe-parent-mode/share"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-unsafe-parent-mode" "$material_unit" \
    >/dev/null 2>&1
); then
  fail "release material validator accepted an unsafe parent directory mode"
fi

cp -a "$material_root" "$TMP/material-empty-license"
rm "$TMP/material-empty-license/share/licenses/$material_unit/project/LICENSE"
release_materials_hash_tree "$TMP/material-empty-license" "$material_unit" \
  "$TMP/material-empty-license/share/sources/$material_unit/MATERIALS.sha256"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-empty-license" "$material_unit" >/dev/null 2>&1
); then
  fail "release material validator accepted an empty license directory"
fi
if (
  release_materials_require_source "$material_root" "$material_unit" bin/tool fixture v2.0.0 \
    >/dev/null 2>&1
); then
  fail "release material validator accepted a different release version"
fi
if (
  release_materials_require_go "$material_root" "$material_unit" bin/tool >/dev/null 2>&1
); then
  fail "release material validator accepted missing Go build records"
fi
cp -a "$material_root" "$TMP/material-missing-payload"
rm "$TMP/material-missing-payload/bin/tool"
if (
  WORK="$TMP/material-validation-work"
  release_materials_validate "$TMP/material-missing-payload" "$material_unit" >/dev/null 2>&1
); then
  fail "release material validator accepted a record for an unshipped payload"
fi

bash "$ROOT/scripts/test-preview-line.sh"
bash "$ROOT/scripts/test-delete-preview.sh"

# The component passes every input unchanged to the fixed trusted helper.
mkdir -p "$TMP/source-entry/trusted/platform/release"
cat > "$TMP/source-entry/trusted/platform/release/producer-inputs.py" <<'PYENTRY'
import os, sys
from pathlib import Path
unit = os.environ['ENTRY_UNIT']
prefix = unit + '-' if unit in ('runtime', 'vmlinux') else ''
assert sys.argv[1:] == ['admit', unit, 'release/v1.2.x', '1' * 40,
                       prefix + 'v1.2.3', os.environ['GITHUB_REPOSITORY'], 'producer-source.tar']
if os.environ.get('ENTRY_REJECT'): raise SystemExit(23)
Path('producer-source.tar').write_text('admitted fixture')
PYENTRY
(
  cd "$TMP/source-entry"
  export GITHUB_REPOSITORY=kuasar-sandbox/accelerator ENTRY_UNIT=accelerator
  bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 accelerator
  [ -f producer-source.tar ] || fail "source admission was not preserved"
  rm producer-source.tar
  status=0
  ENTRY_REJECT=1 bash "$ROOT/scripts/validate-release-source.sh" release/v1.2.x 1111111111111111111111111111111111111111 v1.2.3 accelerator || status=$?
  [ "$status" -eq 23 ] || fail "source admission failure was not preserved"
  [ ! -e producer-source.tar ] || fail "rejected admission produced an artifact"
)
rm "$TMP/source-entry/trusted/platform/release/producer-inputs.py"
if (cd "$TMP/source-entry"; bash "$ROOT/scripts/validate-release-source.sh" main 1111111111111111111111111111111111111111 v1.2.3 connector) >/dev/null 2>&1; then
  fail "source validator accepted a missing trusted helper"
fi
bash -n "$ROOT/scripts/delete-preview.sh" "$ROOT/scripts/validate-release-source.sh"
grep -Fqx 'run-name: Release ${{ inputs.version }} @${{ inputs.source_sha }}' \
  "$ROOT/.github/workflows/release.yml" \
  || fail "release run identity does not pin source_sha"
workflow="$ROOT/.github/workflows/release.yml"
grep -Fq 'kuasar-preview-binding' "$ROOT/scripts/publish-release.sh" \
  || fail "Preview publisher does not record its build binding"
for workflow in release.yml delete-preview.yml; do
  [ "$(grep -Fc 'group: component-mutation-${{ github.repository }}-${{ inputs.version }}' \
    "$ROOT/.github/workflows/$workflow")" -eq 1 ] \
    || fail "$workflow does not hold exactly one full-workflow mutation lock"
done
grep -Fq 'kuasar-release-source' "$ROOT/scripts/publish-release.sh" \
  || fail "publisher does not record Stable source provenance"
grep -Fq 'reconcile_main_latest' "$ROOT/scripts/publish-release.sh" \
  || fail "publisher does not reconcile component main Latest by source commit"
RECONCILE_WORKFLOW="$ROOT/.github/workflows/reconcile-latest.yml"
grep -Fq 'group: component-latest-reconciliation-${{ github.repository }}' \
  "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation is not serialized across component versions"
grep -Fq 'workflow_run:' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation is not triggered after release completion"
grep -Fq 'schedule:' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation has no automatic recovery schedule"
grep -Fq 'publish-release.sh reconcile' "$RECONCILE_WORKFLOW" \
  || fail "Latest reconciliation does not use the idempotent entrypoint"
if grep -R -Fq 'queue: max' "$ROOT/.github/workflows"; then
  fail "workflows use the unsupported concurrency queue key"
fi

expected_cases=(
  test/e2e/cases/image.manifest.sh
  test/e2e/cases/storage.cache-membership.sh
  test/e2e/cases/storage.cache-probe.sh
  test/e2e/cases/storage.cache.sh
  test/e2e/cases/storage.obs.sh
  test/e2e/cases/storage.service-drain.sh
  test/e2e/cases/storage.socket-activation.sh
  test/e2e/cases/storage.store-cache.sh
  test/e2e/cases/storage.tiered-cache.sh
)
mapfile -t actual_cases < <(git -C "$ROOT" ls-files -- 'test/e2e/cases/*.sh' | sort)
mapfile -t sorted_expected_cases < <(printf '%s\n' "${expected_cases[@]}" | sort)
[ "${#actual_cases[@]}" -eq "${#sorted_expected_cases[@]}" ] \
  || fail "unexpected number of accelerator product E2E cases"
for i in "${!sorted_expected_cases[@]}"; do
  [ "${actual_cases[$i]}" = "${sorted_expected_cases[$i]}" ] \
    || fail "accelerator E2E case set differs from the expected prepared cases"
done
for case_path in "${expected_cases[@]}"; do
  [ "$(git -C "$ROOT" ls-files -s -- "$case_path" | awk '{print $1}')" = 100644 ] \
    || fail "$case_path must be a non-executable platform case file (100644)"
done
for retired in test/e2e/e2e_manifest.sh test/e2e/e2e_obs.sh test/e2e/run_all.sh; do
  if git -C "$ROOT" ls-files --error-unmatch -- "$retired" >/dev/null 2>&1; then
    fail "$retired is retired and must not remain in the source tree"
  fi
done

mkdir -p "$TMP/bin" "$TMP/src" "$TMP/rocksdb"
fixture_root="$TMP/project"
mkdir -p "$fixture_root/scripts" "$fixture_root/LICENSES"
install -m 0644 "$ROOT/LICENSE" "$fixture_root/LICENSE"
printf 'fixture nested project notice\n' > "$fixture_root/LICENSES/NOTICE.txt"
printf 'fixture project attribution\n' > "$fixture_root/NOTICE"
printf '/bin/\n/build/\n' > "$fixture_root/.gitignore"
install -m 0755 "$ROOT/scripts/release.sh" "$fixture_root/scripts/release.sh"
install -m 0755 "$ROOT/scripts/release-materials.sh" "$fixture_root/scripts/release-materials.sh"
install -m 0644 "$ROOT/scripts/release-native-materials.sh" "$fixture_root/scripts/release-native-materials.sh"
# Synthetic link inputs exercise the normal material collection without a real RocksDB build.
cat >> "$fixture_root/scripts/release-native-materials.sh" <<'EOF'
release_native_system_input() {
  local input="$1" label="system/$(basename "$1")"
  case "$(basename "$input")" in libstdc++.a|libgcc.a) ;; *) fail "unexpected fixture system input" ;; esac
  mkdir -p "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label"
  printf 'synthetic fixture compiler-runtime notice\n' \
    > "$RELEASE_MATERIALS_STAGE/share/licenses/$RELEASE_MATERIALS_UNIT/$label/LICENSE"
  release_materials_record_source "$2" "system:$(basename "$input")" 1.0 \
    deb-source:fixture@1.0 "sha256:$(sha256sum "$input" | awk '{print $1}');package:fixture" "$label"
}
EOF
install -m 0644 "$ROOT/scripts/release-archive-validator.go" "$fixture_root/scripts/release-archive-validator.go"
install -m 0755 "$ROOT/scripts/publish-release.sh" "$fixture_root/scripts/publish-release.sh"
printf 'module github.com/kuasar-sandbox/accelerator\n\ngo 1.24\n' > "$fixture_root/go.mod"
for binary in manifest-ctl store-ctl cache-ctl; do
  mkdir -p "$fixture_root/cmd/$binary"
  printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/$binary/main.go"
done
mkdir -p "$fixture_root/test"
cp -a "$ROOT/test/scripts" "$fixture_root/test/scripts"
cat > "$fixture_root/Makefile" <<'EOF'
.PHONY: build
build:
	mkdir -p bin/x86_64 build/src/rocksdb build/x86_64/rocksdb/lib build/test-system
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/manifest-ctl ./cmd/manifest-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/store-ctl ./cmd/store-ctl
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=true -o bin/x86_64/cache-ctl ./cmd/cache-ctl
	for notice in AUTHORS COPYING LICENSE.Apache LICENSE.leveldb; do printf 'fixture RocksDB %s\n' "$$notice" > "build/src/rocksdb/$$notice"; done
	printf 'fresh synthetic RocksDB archive\n' > build/x86_64/rocksdb/lib/librocksdb.a
	printf 'synthetic stdc++ archive\n' > build/test-system/libstdc++.a
	printf 'synthetic gcc archive\n' > build/test-system/libgcc.a
	@map="$(CURDIR)/build/x86_64/cache-ctl.map"; \
	  printf 'LOAD %s\n' "$(CURDIR)/build/x86_64/rocksdb/lib/librocksdb.a" \
	    "$(CURDIR)/build/test-system/libstdc++.a" "$(CURDIR)/build/test-system/libgcc.a" > "$$map"
EOF
fixture_project_sha="$(init_fixture_repo "$fixture_root" LICENSE LICENSES NOTICE .gitignore scripts go.mod cmd test/scripts Makefile)"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/go-fixture" ./cmd/manifest-ctl)
release_materials_require_go_revision "$TMP/go-fixture" "$fixture_project_sha"
printf '// dirty fixture\n' >> "$fixture_root/cmd/manifest-ctl/main.go"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -o "$TMP/dirty-go-fixture" ./cmd/manifest-ctl)
if (release_materials_require_go_revision "$TMP/dirty-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary built from dirty source"
fi
printf 'package main\nfunc main() {}\n' > "$fixture_root/cmd/manifest-ctl/main.go"
if (release_materials_require_go_revision "$TMP/go-fixture" \
  0000000000000000000000000000000000000000 >/dev/null 2>&1); then
  fail "release accepted a binary built from another commit"
fi
GO111MODULE=off go build -o "$TMP/unstamped-go-fixture" "$fixture_root/cmd/manifest-ctl/main.go"
if (release_materials_require_go_revision "$TMP/unstamped-go-fixture" "$fixture_project_sha" >/dev/null 2>&1); then
  fail "release accepted a binary without source stamping"
fi
GOWORK=off make --no-print-directory -C "$fixture_root" build
for binary in manifest-ctl store-ctl cache-ctl; do
  install -m 0755 "$fixture_root/bin/x86_64/$binary" "$TMP/bin/$binary"
done

mkdir -p "$TMP/no-rocksdb-bin"
install -m 0755 "$TMP/bin/manifest-ctl" "$TMP/no-rocksdb-bin/manifest-ctl"
install -m 0755 "$TMP/bin/store-ctl" "$TMP/no-rocksdb-bin/store-ctl"
(cd "$fixture_root" && GOWORK=off GOOS=linux GOARCH=amd64 go build -buildvcs=true -tags no_rocksdb \
  -o "$TMP/no-rocksdb-bin/cache-ctl" ./cmd/cache-ctl)
if SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/no-rocksdb-bin" \
  "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 \
    "$TMP/no-rocksdb-bundle" > "$TMP/prebuilt-rejection.log" 2>&1; then
  fail "packager accepted a payload without RocksDB support"
fi
grep -Fq 'official accelerator release must include RocksDB support' "$TMP/prebuilt-rejection.log" \
  || fail "prebuilt rejection failed for an unrelated reason"
cp -a "$fixture_root/build/src/rocksdb/." "$TMP/rocksdb/"
sha256sum "$TMP/rocksdb/"{AUTHORS,COPYING,LICENSE.Apache,LICENSE.leveldb} \
  > "$TMP/rocksdb-before.sha256"

SOURCE_DATE_EPOCH=1700000000 RELEASE_BIN_DIR="$TMP/bin" \
  RELEASE_ROCKSDB_SOURCE_DIR="$TMP/rocksdb" \
  "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/bundle"
sha256sum -c "$TMP/rocksdb-before.sha256" >/dev/null \
  || fail "release packaging changed foreign source material"
"$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/bundle"
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  "$fixture_project_sha" main
"$ROOT/scripts/test-publisher.sh" "$fixture_root/scripts/publish-release.sh" \
  "$TMP/bundle" kuasar-sandbox/accelerator v1.2.3 \
  "$fixture_project_sha" release/v1.2.x

archive="$TMP/bundle/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
go_toolchain="$(GOWORK=off go -C "$fixture_root" env GOVERSION)"
for path in ./bin/manifest-ctl ./bin/store-ctl ./bin/cache-ctl \
  ./test/scripts/bench_cache.sh \
  ./share/licenses/accelerator/project/LICENSE \
  ./share/licenses/accelerator/rocksdb/AUTHORS \
  ./share/licenses/accelerator/rocksdb/COPYING \
  ./share/licenses/accelerator/rocksdb/LICENSE.Apache \
  ./share/licenses/accelerator/rocksdb/LICENSE.leveldb \
  ./share/licenses/accelerator/go-toolchain/"$go_toolchain"/LICENSE \
  ./share/sources/accelerator/SOURCES.tsv \
  ./share/sources/accelerator/GO-BUILD-INFO.tsv \
  ./share/sources/accelerator/GO-MODULES.tsv \
  ./share/sources/accelerator/MATERIALS.sha256; do
  tar -tzf "$archive" | grep -Fx "$path" >/dev/null || fail "archive is missing $path"
done
tar -xOf "$archive" ./share/sources/accelerator/SOURCES.tsv \
  | grep -Fq $'\tGo toolchain\t'"$go_toolchain"$'\t' \
  || fail "archive does not associate its Go toolchain with license material"
if tar -tzf "$archive" | grep -E '^\./(docs|test/e2e)(/|$)' >/dev/null; then
  fail "component archive contains documentation or E2E sources"
fi
if tar -tzf "$archive" | grep -E '(^|/)release\.json$|(^|/)release/[^/]+\.json$' >/dev/null; then
  fail "archive contains release metadata JSON"
fi

(umask 077; SOURCE_DATE_EPOCH=1700000000 \
  "$fixture_root/scripts/release.sh" package v1.2.3 x86_64 "$TMP/reproducible")
cmp -s "$archive" "$TMP/reproducible/assets/accelerator-v1.2.3-linux-x86_64.tar.gz" \
  || fail "identical inputs did not produce an identical archive"

for target in darwin/amd64 linux/arm64; do
  target_command=manifest-ctl
  [ "$target" != darwin/amd64 ] || target_command=store-ctl
  (cd "$fixture_root" && GOWORK=off CGO_ENABLED=0 GOOS="${target%/*}" GOARCH="${target#*/}" \
    go build -buildvcs=true -o "$TMP/target-${target//\//-}" "./cmd/$target_command")
done
for mutation in extra-binary extra-script extra-directory extra-source extra-source-directory duplicate no-rocksdb wrong-os wrong-arch; do
  candidate="$TMP/exact-contract-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$mutation" in
    extra-binary) install -m 0755 "$TMP/go-fixture" "$candidate/root/bin/unexpected-tool" ;;
    extra-script) install -m 0755 "$TMP/go-fixture" "$candidate/root/test/scripts/unexpected-helper" ;;
    extra-directory) mkdir "$candidate/root/unexpected-directory" ;;
    extra-source) printf 'uncontracted metadata\n' > "$candidate/root/share/sources/accelerator/unexpected" ;;
    extra-source-directory) mkdir "$candidate/root/share/sources/accelerator/nested" ;;
    no-rocksdb) install -m 0755 "$TMP/no-rocksdb-bin/cache-ctl" "$candidate/root/bin/cache-ctl" ;;
    wrong-os) install -m 0755 "$TMP/target-darwin-amd64" "$candidate/root/bin/store-ctl" ;;
    wrong-arch) install -m 0755 "$TMP/target-linux-arm64" "$candidate/root/bin/manifest-ctl" ;;
  esac
  if [ "$mutation" = duplicate ]; then
    tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -cf "$candidate/duplicate.tar" -C "$candidate/root" .
    tar --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -rf "$candidate/duplicate.tar" -C "$candidate/root" ./bin/manifest-ctl
    gzip -c "$candidate/duplicate.tar" > "$candidate/assets/$(basename "$archive")"
  else
    tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
      -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  fi
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated checksums"
  fi
  case "$mutation" in
    extra-*) expected='unexpected member' ;;
    duplicate) expected='duplicate member' ;;
    no-rocksdb) expected='must include RocksDB support' ;;
    wrong-*) expected='must target linux/amd64' ;;
  esac
  grep -Fq "$expected" "$candidate/result.log" || fail "$mutation failed for an unrelated reason"
done

for payload in manifest-ctl store-ctl cache-ctl; do
  candidate="$TMP/wrong-main-$payload"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  other=manifest-ctl
  [ "$payload" != manifest-ctl ] || other=store-ctl
  install -m 0755 "$candidate/root/bin/$other" "$candidate/root/bin/$payload"
  release_materials_require_go_revision "$candidate/root/bin/$payload" "$fixture_project_sha"
  release_materials_init "$candidate/metadata-stage" "$candidate/materials" accelerator
  for binary in manifest-ctl store-ctl cache-ctl; do
    release_materials_add_go_binary "$candidate/root/bin/$binary" "bin/$binary"
  done
  {
    printf 'payload\trecord\tname\tversion_or_value\tchecksum\n'
    LC_ALL=C sort -u "$candidate/materials/go-build-info"
  } > "$candidate/root/share/sources/accelerator/GO-BUILD-INFO.tsv"
  release_materials_hash_tree "$candidate/root" accelerator \
    "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted another same-commit main package as $payload"
  fi
  grep -Fq 'must have its expected main package' "$candidate/result.log" \
    || fail "$payload identity failed for an unrelated reason"
done

for native in rocksdb-static-library system:libstdc++.a system:libgcc.a; do
  candidate="$TMP/missing-native-${native//:/-}"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  inventory="$candidate/root/share/sources/accelerator/SOURCES.tsv"
  awk -F '\t' -v name="$native" '$2 != name' "$inventory" > "$candidate/changed.tsv"
  mv "$candidate/changed.tsv" "$inventory"
  release_materials_hash_tree "$candidate/root" accelerator \
    "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted missing $native with regenerated checksums"
  fi
  case "$native" in
    rocksdb-static-library) expected='RocksDB static-library digest' ;;
    system:*) expected='unclaimed release license material' ;;
  esac
  grep -Fq "$expected" "$candidate/result.log" \
    || { cat "$candidate/result.log" >&2; fail "$native failed for an unrelated reason"; }
done

for mutation in unknown-kind unknown-payload unknown-toolchain \
  native-version native-source native-integrity native-package native-license; do
  candidate="$TMP/source-contract-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  table="$candidate/root/share/sources/accelerator/SOURCES.tsv"
  awk -F '\t' -v OFS='\t' -v mutation="$mutation" '
    { print }
    NR == 2 && mutation ~ /^unknown-/ {
      $1="bin/cache-ctl"; $2="unrecognized"; $6="share/licenses/accelerator/project"
      if (mutation == "unknown-payload") { $1="bin/manifest-ctl"; $2="rocksdb" }
      if (mutation == "unknown-toolchain") { $1="test/scripts/proc_analyze.py"; $2="Go toolchain" }
      print
    }
  ' "$table" > "$candidate/changed-sources"
  if [[ "$mutation" == native-* ]]; then
    awk -F '\t' -v OFS='\t' -v mutation="$mutation" '
      $2 == "system:libgcc.a" {
        if (mutation == "native-version") $3="different-version"
        if (mutation == "native-source") $4="https://example.invalid/unverified-native"
        if (mutation == "native-integrity") $5="not-an-input-digest"
        if (mutation == "native-package") sub(/;package:.*/, ";package:another-package", $5)
        if (mutation == "native-license") $6="share/licenses/accelerator/system/libstdc++.a"
      }
      { print }
    ' "$table" > "$candidate/changed-sources"
  fi
  install -m 0644 "$candidate/changed-sources" "$table"
  release_materials_hash_tree "$candidate/root" accelerator "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated material/archive checksums"
  fi
  grep -Fq 'unrecognized or inconsistent source inventory record' "$candidate/result.log" \
    || fail "$mutation failed for an unrelated reason"
done

for notice in AUTHORS COPYING LICENSE.Apache LICENSE.leveldb; do
  candidate="$TMP/missing-rocks-notice-$notice"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  rm "$candidate/root/share/licenses/accelerator/rocksdb/$notice"
  release_materials_hash_tree "$candidate/root" accelerator \
    "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted missing RocksDB $notice with regenerated checksums"
  fi
  grep -Fq "missing member \"./share/licenses/accelerator/rocksdb/$notice\"" "$candidate/result.log" \
    || fail "missing RocksDB notice failed for an unrelated reason"
done

# The archive name is the requested release target; an untagged source record
# identifies the actual commit and does not pretend that target tag exists.
tar -xOf "$archive" ./share/sources/accelerator/SOURCES.tsv | \
  awk -F '\t' -v sha="$fixture_project_sha" \
    '$2 == "accelerator" && $3 == "git:" sha {found=1} END {exit !found}' \
  || fail "pre-tag project source was recorded as an existing release"
for column in 3 4 5; do
  candidate="$TMP/project-source-$column"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  inventory="$candidate/root/share/sources/accelerator/SOURCES.tsv"
  awk -F '\t' -v OFS='\t' -v column="$column" \
    '$2 == "accelerator" {$column="not-the-selected-source"} {print}' \
    "$inventory" > "$candidate/changed.tsv"
  mv "$candidate/changed.tsv" "$inventory"
  release_materials_hash_tree "$candidate/root" accelerator \
    "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" >/dev/null 2>&1; then
    fail "validator accepted project provenance column $column with regenerated checksums"
  fi
done

cp -a "$TMP/bundle" "$TMP/tampered"
printf 'tampered\n' >> "$TMP/tampered/assets/accelerator-v1.2.3-linux-x86_64.tar.gz"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/tampered" >/dev/null 2>&1; then
  fail "validator accepted a tampered archive"
fi

cp -a "$TMP/bundle" "$TMP/extra"
touch "$TMP/extra/assets/release.json"
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/extra" >/dev/null 2>&1; then
  fail "validator accepted an extra asset"
fi

mkdir -p "$TMP/material-stage"
tar -xzf "$archive" -C "$TMP/material-stage"
printf 'not the packaged license\n' > "$TMP/material-stage/share/licenses/accelerator/rocksdb/COPYING"
cp -a "$TMP/bundle" "$TMP/material-tampered"
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime='@1700000000' \
  --pax-option=delete=atime,delete=ctime -czf \
  "$TMP/material-tampered/assets/accelerator-v1.2.3-linux-x86_64.tar.gz" \
  -C "$TMP/material-stage" .
(cd "$TMP/material-tampered/assets" \
  && sha256sum accelerator-v1.2.3-linux-x86_64.tar.gz > SHA256SUMS)
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 \
  "$TMP/material-tampered" >/dev/null 2>&1; then
  fail "validator accepted license material that disagrees with its inventory"
fi

if RELEASE_BIN_DIR="$TMP/bin" "$fixture_root/scripts/release.sh" package 01.2.3 x86_64 \
  "$TMP/invalid-version" >/dev/null 2>&1; then
  fail "packager accepted an invalid version"
fi
if RELEASE_BIN_DIR="$TMP/bin" "$fixture_root/scripts/release.sh" package v1.2.3 aarch64 \
  "$TMP/invalid-arch" >/dev/null 2>&1; then
  fail "packager accepted x86_64 payloads as aarch64"
fi

for mutation in setuid setgid writable-directory writable-binary; do
  candidate="$TMP/unsafe-mode-$mutation"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  case "$mutation" in
    setuid) chmod 4755 "$candidate/root/bin/manifest-ctl" ;;
    setgid) chmod 2755 "$candidate/root/bin/manifest-ctl" ;;
    writable-directory) chmod 0777 "$candidate/root/bin" ;;
    writable-binary) chmod 0777 "$candidate/root/bin/manifest-ctl" ;;
  esac
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" > "$candidate/result.log" 2>&1; then
    fail "validator accepted $mutation with regenerated checksums"
  fi
  grep -Fq 'unsafe type, mode or ownership' "$candidate/result.log" \
    || fail "$mutation was rejected for an unrelated reason"
done

for column in 4 5; do
  candidate="$TMP/rocksdb-source-$column"
  cp -a "$TMP/bundle" "$candidate"
  mkdir "$candidate/root"
  tar -xzf "$archive" -C "$candidate/root"
  inventory="$candidate/root/share/sources/accelerator/SOURCES.tsv"
  awk -F '\t' -v OFS='\t' -v column="$column" \
    '$2 == "rocksdb" {$column="not-the-pinned-rocksdb-source"} {print}' \
    "$inventory" > "$candidate/changed.tsv"
  mv "$candidate/changed.tsv" "$inventory"
  release_materials_hash_tree "$candidate/root" accelerator \
    "$candidate/root/share/sources/accelerator/MATERIALS.sha256"
  tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@1700000000 \
    -czf "$candidate/assets/$(basename "$archive")" -C "$candidate/root" .
  (cd "$candidate/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
  if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$candidate" >/dev/null 2>&1; then
    fail "validator accepted a changed RocksDB provenance column $column"
  fi
done

cp -a "$TMP/bundle" "$TMP/nonroot-owner"
mkdir "$TMP/nonroot-owner/root"
tar -xzf "$archive" -C "$TMP/nonroot-owner/root"
tar --sort=name --owner=1234 --group=0 --numeric-owner --mtime=@1700000000 \
  -czf "$TMP/nonroot-owner/assets/$(basename "$archive")" -C "$TMP/nonroot-owner/root" .
(cd "$TMP/nonroot-owner/assets" && sha256sum "$(basename "$archive")" > SHA256SUMS)
if "$fixture_root/scripts/release.sh" validate v1.2.3 x86_64 "$TMP/nonroot-owner" >/dev/null 2>&1; then
  fail "validator accepted non-root numeric ownership with regenerated checksums"
fi

# Standalone validation must not need source checkouts, module downloads or a build.
mkdir -p "$TMP/standalone-tools" "$TMP/standalone-bin"
cp -a "$fixture_root/scripts" "$TMP/standalone-tools/scripts"
for command in git curl wget cargo make gcc; do
  printf '#!/bin/sh\nexit 97\n' > "$TMP/standalone-bin/$command"
  chmod 0755 "$TMP/standalone-bin/$command"
done
env PATH="$TMP/standalone-bin:$PATH" GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  SOURCE_SHA="$fixture_project_sha" "$TMP/standalone-tools/scripts/release.sh" validate \
  v1.2.3 x86_64 "$TMP/bundle"
echo "test-release: standalone validation without checkouts/downloads/build PASS"

echo "test-release: PASS"
