[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

# Accelerator CI requirements and checks

The [platform CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md)
owns shared runner routing, profiles, workflow entry points and rollout status.
This guide records accelerator build requirements and the evidence provided by
its checks.

## Build and ABI requirements

The release build uses real RocksDB through the existing recipe;
`NO_ROCKSDB=1` is not a release substitute. Go and native compilers come from the
build environment, retaining the existing toolchain selection semantics.
CI uses the admitted Workbench image selected once for the run, pinned separately
for native x86_64 and aarch64 runners. Ordinary builds, tests and packaging use an
ordinary UID with a read-only root, dropped capabilities and private source/cache
directories. `KUASAR_BUILD_JOBS` supplies the same task CPU budget to Go and the
RocksDB CMake build; local native recipes without a budget still use `nproc`.
Normal Go RocksDB tests retain the upstream binding's compression-library link
flags and require Snappy, LZ4, Zstandard and zlib development libraries. This does
not enable compression in the existing native RocksDB recipe.

The accelerator [ABI check](../scripts/ci-check-abi.py) requires `manifest-ctl`
and `store-ctl` to remain static. `cache-ctl` retains normal CGO/glibc with static
RocksDB, libstdc++ and libgcc; required GLIBC versions may not exceed the declared
2.38 baseline. ABI failures stop packaging. Native recipes, checksums, licenses,
source inventories and link-map validation remain required. The publisher uses a
standalone archive validator compiled from its trusted source in a separate
Workbench invocation with cache restore/save disabled; publishing executes the
validator without compiling it again.

## Prepared product E2E and local evidence

The [accelerator E2E guide](../test/e2e/README.md) lists the prepared-product,
EROFS, Redis, scratch-storage and privilege prerequisites and each accelerator
`storage.*.sh` / `image.*.sh` product case. Product E2E is executed only through
the platform-owned prepared-workspace runner; accelerator no longer owns a
`run_all.sh` or an owner-suite entry point. Local filesystem/cache and
S3-compatible fixtures demonstrate local behavior; they do not establish real
cloud coverage.

For ordinary non-credentialed validation, prepare a workspace from the exact
prebuilt platform release and explicitly select the five non-OBS accelerator
cases:

```bash
RUNNER=/path/to/platform/test/e2e/e2e
RELEASE_DIR=/path/to/prebuilt/platform-release
WORK=/tmp/kuasar-e2e

"$RUNNER" prepare --release-dir "$RELEASE_DIR" --workdir "$WORK"
"$RUNNER" run --workdir "$WORK" \
  --include storage.cache.sh \
  --include storage.tiered-cache.sh \
  --include storage.cache-membership.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

Credentialed OBS remains opt-in and is selected by case ID, not by an
`OBS_E2E` marker. With `OBS_BUCKET`, `OBS_ENDPOINT`, `OBS_AK` and `OBS_SK`
explicitly configured, run the same prepared workspace with:

```bash
"$RUNNER" run --workdir "$WORK" --include storage.obs.sh
```

A selected cloud case that cannot satisfy its endpoint or credential
prerequisites fails; an excluded cloud case is not a pass. CI uses the same
platform runner and exact admitted case IDs.

Offline checks from the accelerator checkout:

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # use the actual platform path
python3 scripts/test-native-build-budget.py
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

The workflow checks parse the component's actual YAML and exercise its allocation,
workspace and ABI contracts. Release tests use synthetic binaries/link maps and
mocked API responses. Script tests exercise fixtures and lifecycle boundaries;
they do not complete the numbered real E2E assertions. Passing these checks is
not evidence of a real RocksDB build, prepared product E2E or cloud qualification.

## Streaming flatten regression coverage

Build the selected guest-runtime native recipe before running the flatten and
remote suites, and set `MKFS_EROFS_PATH` to that exact `mkfs.erofs`. Plan tests
require the reported v1 capability; a system mkfs without it is not a substitute.
The [flatten guide](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md)
defines input, source lifetime, native plan and artifact completion contracts.

| Requirement | Implementation | Maintained checks and evidence scope |
|---|---|---|
| Fixed input identity and complete validation | `pkg/flatten/input.go`, `layer_stream.go`; `pkg/remote/stream.go` | Layer media/size/digest/DiffID/trailer/Close and cancellation cases; actual local Registry → umoci → Plan → pipe → current artifact reader. |
| Layer filesystem semantics | `pkg/flatten/layer_apply_linux.go` | Actual umoci late whiteout/opaque, replacements, implicit parents, links, attributes and empty-layer cases; permission failures remain errors. |
| Native layout and sequential emission | Guest native patch series; `pkg/flatten/plan_linux.go` | Raw/Plan identity and extracted contents; corruption, bounds, source mutation, metadata-only repeated RunAt and final-tail failure. Native exact-read injection runs in guest-runtime's existing `test-erofs` target. |
| Archive and entry-point equivalence | `pkg/flatten/archive_linux.go`, `raw_linux.go` | Seekable archive, pipe spool and directory comparisons; truncated/concatenated archive and index-budget rejection. Guest CLI tests cover file, stdout and upload/error paths. |
| Persistent cache publication and ownership | `pkg/remote/prepare_linux.go`, `prefetch_linux.go`, cache/GC | Unverified EOF is not a hit; explicit byte budget, bypass, current-layer priority, duplicates, concurrent GC leases and joined cancellation. Conversion-policy tests retain explicit old-artifact reading while rejecting stale conversion hits. |
| Stable selection and owned cleanup | `pkg/flatten/prepared_linux.go`, `selection_linux.go` | Atime/excluded-child tolerance, included metadata changes, actual ENOSPC during apply/output, bind/output aliases, previous-file/UID/GID/rwx/access-ACL preservation, inherited directory GID, privileged empty-root chmod, EMFILE cleanup, reserved-file and directory substitution rejection, read-only private-tree cleanup without following external symlinks, and open/deleted FD checks. |

The last row includes opt-in real mount tests. Compile the test executable as an
ordinary user, then run only those cases with mount privilege in a new private
mount namespace. Set `TMPDIR` to a private test directory and `MKFS_EROFS_PATH`
to the selected native tool; all mounts are on test-owned paths and are removed
before their directories. The small tmpfs limits create real ENOSPC without
filling the host filesystem.

```bash
CGO_ENABLED=0 go test -c -o /path/to/private/flatten-mount.test ./pkg/flatten
sudo unshare --mount --propagation private env \
  FLATTEN_TEST_MOUNTS=1 TMPDIR=/path/to/private/scratch \
  MKFS_EROFS_PATH=/path/to/selected/mkfs.erofs \
  /path/to/private/flatten-mount.test -test.v -test.run '^TestMounted'
```

These tests establish local contracts. Actual runtime bundle/Guest consumption,
Builder Phase A/B, static package validation, Guest ext4/Host COW peaks and
matched end-to-end performance require the existing prepared-product and Builder
test paths with the exact source/artifact set. Record receiving artifact bytes
separately from scratch; include index/decoder memory and owned teardown. Package
tests or these mount fixtures do not replace real VM evidence.

### Matched flatten measurements

`test/scripts/bench_flatten.py` uses prebuilt native producers, a real stdout
pipe and the current artifact reader. It alternates variant order for three
trials by default, records executable hashes, sampled process-tree RSS, allocated
scratch blocks/inodes and deleted-open files, and waits for the owned producer
session, including children in different process groups, before cleaning scratch.
The Linux harness temporarily acts as a child subreaper and waits only adopted
members of that owned session, so their CPU/I/O is included in child rusage;
unrelated children are not reaped. Records from older harnesses may omit orphan
helpers' CPU/I/O. After all writers stop, cleanup restores deletion access only
within owned scratch, including failure/cancellation paths. A cleanup error is
attached to the original failure instead of replacing it. Receiving artifact P and its optional fsync are
reported separately. `post_eof_teardown_seconds` measures the interval from producer
completion through owned process/thread cleanup to the start of consumer validation;
`receiver_validation_seconds` measures the consumer validation interval.
`accepted_seconds` retains the full interval including both. Older records without
`post_eof_teardown_seconds` include both intervals in `receiver_validation_seconds`;
they cannot be split retrospectively. Optional `--progress-timing` timestamps existing CLI progress
lines and the first available stdout bytes for phase attribution; preparation
ends before the artifact writer can emit its header. Child rusage I/O counters may be unavailable on some hosts;
sampled RSS and block peaks have the recorded sampling resolution.

Allocated storage values in the historical `scratch_blocks`, `deleted_open_blocks` and `artifact_blocks` fields are **bytes** (`st_blocks * 512`), not block counts. Their names and numeric meaning are retained for compatibility. `metric_units` now labels these fields and process RSS as bytes; it labels `child_io_input_512blocks` and `child_io_output_512blocks` as counts of 512-byte blocks. The same allocation units apply to individual samples and reported peaks.

The aggregate `results.json` is published only after all rounds succeed and their artifact identities match. Rejected comparisons retain per-round diagnostics without a success aggregate.

The current consumer's `info --json` config must equal the fixed projected JSON
provided by `--expected-config` before acceptance or caching a verified artifact
identity. Use JSON `null` only when the fixture intentionally has no appended
config. Records include the expected config hash, saved consumer output, and the
resolved paths and SHA-256 hashes of both consumer and fsck. Older records without
these checks or tool hashes remain incomplete evidence; do not infer them from
the producer hash or artifact identity.

Walking a wide tree for block/inode samples can perturb the pipe receiver and
source I/O. Procfs stat parsing preserves arbitrary process-name bytes, including names of unrelated processes. Process enumeration follows procfs parent PIDs and includes owned-session
members after reparenting, checking the leader start time; it does not require
the optional per-task `children` interface. A resource pass without samples, or whose samples never observe a live owned process, fails
instead of publishing zero peaks. The receiver retries interrupted and positive short writes before recording artifact bytes; a write without progress or a storage error fails the run. Scratch traversal uses bound directory descriptors and no-follow entry stats, so directory-to-symlink replacement cannot redirect its samples outside scratch. Proc references are bound before both pathname classification and stat, preventing descriptor reuse from mixing two objects. Deleted-open accounting includes a retained, removed scratch root and uses inode link count,
not the procfs filename suffix. Process `cwd` and `root` references are also inspected: a removed directory they retain contributes its inode and allocated bytes once, including in the historical `deleted_open_*` fields. A scratch inode retained only by a memory mapping must also be accounted for; an unmeasured mapping invalidates the resource sample. Failed teardown aborts the progress reader and
retains scratch instead of waiting indefinitely for stderr. Pair the resource pass with `--timing-only` under the same resource,
storage and cache conditions to measure that effect. Timing-only results keep
identity/content checks, child CPU/I/O accounting and owned teardown, but record
sampled peaks and the sampling interval as null. Retain both results; a timing
pass cannot establish a memory, block, inode or open-file bound.

Compare the previous implementation, the selected correct applier with old mkfs,
and Plan on the same CPU/memory/storage/cache conditions. For the middle control,
apply `test/fixtures/flatten-old-layout.patch` only to an isolated copy of the
selected accelerator source, and build it with the same selected guest CLI and
the old maintained mkfs including the exact-read fix. Record all source commits
and the patch digest with the build receipts. This patch is a measurement fixture;
normal product builds use the Plan path.

Run in the existing Workbench with Python 3.11 or newer. Input, scratch parent and
output root must be disjoint; the output root must not exist. A Registry input
must include its fixed SHA-256 digest. Supply a fixed expected-content JSON keyed
by relative path, including `.` for the root directory: each entry includes `type`, `mode`, `uid`, `gid`, and `mtime_ns` (0 under the fixed output timestamp policy), plus file
`size`/`sha256`, symlink `target` or special-file `device` and numeric `kind` (`stat.S_IFMT(st_mode)`, distinguishing
FIFO, socket, character and block device). For each non-directory
hard-link group (including symlinks and special files) in the selected tree, the lexicographically first relative path
is canonical; other members include `hardlink_to` naming that path. The first occurrence of
each artifact identity is extracted with the selected native fsck, and its actual
contents and metadata must match. This extra extraction is outside measurement. After verification, only the owned
extraction tree has directory access restored for removal; external symlink
targets remain untouched. Scratch permission or I/O errors, including denied inspection of an owned process's open files, fail the measurement; directories removed by the producer and processes or file descriptors that disappear during sampling are tolerated.

```bash
python3 test/scripts/bench_flatten.py \
  --variant baseline,baseline/flatten-ctl,baseline/mkfs.erofs \
  --variant correct-applier,control/flatten-ctl,control/mkfs.erofs \
  --variant plan,plan/flatten-ctl,plan/mkfs.erofs \
  --input fixtures/image.tar --expected fixtures/expected.json \
  --expected-config fixtures/runtime-config.json \
  --consumer plan/flatten-ctl --fsck tools/fsck.erofs \
  --scratch-parent scratch --output-root results --trials 3 --fsync
```

Normal-corpus artifact identities must match across variants. Cover low/high
repetition, layer overwrites, small files, constrained network and explicit cache
as separate fixed fixtures. Record persistent cache occupancy/lifetime separately
when supplying `--config`; it is never temporary scratch. End-to-end elapsed time
includes overlapped work; do not add overlapping stage durations. These host
measurements do not establish Guest ext4 or Host COW peaks. Measure those through
the real Guest/Builder path, including the receiver and owned teardown.
