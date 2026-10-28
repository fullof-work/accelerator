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


Block hashing uses the admitted 1 MiB read buffer and a fixed digest array, with
at most two workers bounded by `GOMAXPROCS`. All workers join before buffer reuse
or cancellation cleanup. Deduplication and layout allocation remain sequential.
An adjacent block can reuse a full SHA-256 only after comparing all 4096 bytes;
worker count does not change block identities or output order.

## Prepared product E2E and local evidence

The [accelerator E2E guide](../test/e2e/README.md) lists the prepared-product,
EROFS, Redis, scratch-storage and privilege prerequisites and each accelerator
`basic.*.sh` / `storage.*.sh` / `image.*.sh` product case. Product E2E is executed only through
the platform-owned prepared-workspace runner; accelerator no longer owns a
`run_all.sh` or an owner-suite entry point. Local filesystem/cache and
S3-compatible fixtures demonstrate local behavior; they do not establish real
cloud coverage.

For ordinary non-credentialed validation, prepare a workspace from the exact
prebuilt platform release and explicitly select the non-OBS accelerator storage/image
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
  --include storage.cache-probe.sh \
  --include storage.service-drain.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

`basic.accelerator-activation.sh` separately validates service-manager lifecycle and named listener ownership. Run it with `--include basic.accelerator-activation.sh` on a systemd-capable environment; its mandatory `["systemd"]` declaration routes formal CI to the existing system executor. The actual Store/Cache requests verify recovery, while storage protocol/algorithm cases keep their storage classification and clean runtime. See the E2E guide for owned resources and prerequisites.

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

Production planning and Plan test fixtures use the fixed external `github.com/kuasar-sandbox/go-erofs` module without resolving or executing mkfs. Independent content validation uses host fsck/dump. Go planner tests cover an invalid native-tool path; Plan corruption, bounds and success-tail regressions remain active. The [flatten guide](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md) defines input, source lifetime, plan and artifact completion contracts.

| Requirement | Implementation | Maintained checks and evidence scope |
|---|---|---|
| Fixed input identity and complete validation | `pkg/flatten/input.go`, `layer_stream.go`; `pkg/remote/stream.go` | Layer media/size/digest/DiffID/trailer/Close and cancellation cases; actual local Registry → umoci → Plan → pipe → current artifact reader. |
| Content-based layer compression | `internal/layercompression`; streaming layers, Docker-save adapter and legacy `remote.Pull` | One detector and decoder implementation accepts plain/gzip/zstd bytes independently of supported media-type labels, including leading zstd skippable frames. Checks cover mismatched labels, complete identities/trailers, sniff-time cancellation, and actual Registry `Prepare` without cache, on cache miss/hit, and against archive output. |
| Layer filesystem semantics | `pkg/flatten/layer_apply_linux.go` | Actual umoci late whiteout/opaque, replacements, implicit parents, links, attributes and empty-layer cases; permission failures remain errors. |
| Go layout and sequential emission | Fixed go-erofs module; `go_planner_linux.go`, `plan_linux.go` | Actual content/metadata, selected hard links, padded-block sharing, independent fsck extraction, deterministic repeated builds, controlled allocation/Plan budgets, cancellation and cleanup. Existing Plan corruption, bounds, source mutation, metadata-only repeated RunAt and final-tail checks remain. Historical native Raw/Plan and exact-read controls remain in their original revisions; current tests exercise the Go generator. Go/native output identity is not required. |
| Archive and entry-point equivalence | `pkg/flatten/archive_linux.go`, `raw_linux.go` | Seekable archive, pipe spool and directory comparisons; truncated/concatenated archive and index-budget rejection. Guest CLI tests cover file, stdout and upload/error paths. |
| Persistent cache publication and ownership | `pkg/remote/prepare_linux.go`, `prefetch_linux.go`, cache/GC | Unverified EOF is not a hit; explicit byte budget, bypass, current-layer priority, duplicates, concurrent GC leases and joined cancellation. Conversion-policy tests retain explicit old-artifact reading while rejecting stale conversion hits. |
| Stable selection and owned cleanup | `pkg/flatten/prepared_linux.go`, `selection_linux.go` | Atime/excluded-child tolerance, included metadata changes, actual ENOSPC during apply/output, bind/output aliases, previous-file/UID/GID/rwx/access-ACL preservation, inherited directory GID, privileged empty-root chmod, fail-fast sticky-directory replacement permissions, EMFILE cleanup, reserved-file and directory substitution rejection, read-only private-tree cleanup without following external symlinks, and open/deleted FD checks. |

`LayerLimits.MetadataBytes` and the three `PlanLimits` fields are opt-in
admission quotas. Zero no longer imposes the former 128 MiB layer estimate or
512/512/128 MiB planning/plan/index defaults. Internally, zero-valued plan quotas
normalize to checked integer/allocation ceilings, without allocating those
amounts. The former one-million-source cap is replaced by the addressable source
slice bound; both producer and reader still enforce byte, overflow and format
checks. Explicit positive quotas retain the existing conservative accounting and
phase shares, including cumulative selection charges and table-growth overlap.
No duplicate umoci path table or new selection-lifetime model is introduced.

Regressions cross the former 6 GiB unique-block and source-index thresholds,
check opt-in rejection, and exercise source-count/charge overflow and corrupt
plans. Source mutation, path, digest, decoder, archive and artifact-integrity
checks remain unchanged. Builder guest/execution resources remain the outer
memory boundary; standalone callers must bound their own execution environment.
These estimates are not total RSS limits, and zero quotas do not guarantee that
arbitrarily large input will complete. The fixed cross-architecture golden
includes the presence carrier introduced by the tarstream format change.

All payload records remain in first-seen source order and are streamed to the
existing KERPLAN v1 files. No full payload blob or raw intermediate is created.
Conversion lookup and writeback use `umoci-go-erofs-no-xattr-v1`; older conversion
results miss, while explicitly selected old artifacts remain readable.

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

For a linked Go generator, use `--variant NAME,CLI` without a native tool. `--allow-layout-differences` permits different artifact identities across generators only after the existing content, metadata, hard-link and runtime-config checks; repeated builds of each generator must still be byte-identical. `identity-comparison.json` records the trial count and repeated-identity check for each variant. A one-trial resource pass remains valid for resource measurements but explicitly reports `not_checked_single_trial`; it is not determinism evidence. Use at least two trials to compare repeated output, and retain the three alternating trials required for performance acceptance.

`test/scripts/bench_flatten.py` uses prebuilt producers, a real stdout
pipe and the current artifact reader. It alternates variant order for three
trials by default, records executable hashes, sampled process-tree RSS, allocated
scratch blocks/inodes and deleted-open files, and waits for the owned producer
session, including children in different process groups, before cleaning scratch.
The Linux harness temporarily acts as a child subreaper and waits only adopted
members of that owned session, so their CPU/I/O is included in child rusage;
unrelated children are not reaped. Termination and adopted-child waits use pidfds after rechecking process identity, with repeated session enumeration; numeric process-group IDs are not signal targets. The measurement host must support pidfd signaling and waits, checked before launching a producer. Records from older harnesses may omit orphan
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

The aggregate `results.json` is published only after all rounds succeed and the selected identity policy passes. Repeated builds of each generator must match; cross-generator identities may differ only with the explicit semantic-comparison option. Rejected comparisons retain per-round diagnostics without a success aggregate.

The current consumer's `info --json` config must equal the fixed projected JSON
provided by `--expected-config` before acceptance or caching a verified artifact
identity. Use JSON `null` only when the fixture intentionally has no appended
config. Records include the expected config hash, saved consumer output, and the
resolved paths and SHA-256 hashes of both consumer and fsck. Older records without
these checks or tool hashes remain incomplete evidence; do not infer them from
the producer hash or artifact identity.

Each round records the resolved producer config path, SHA-256 and file identity (`--config`, then `FLATTEN_CONFIG`; null means defaults). It checks the config again before publishing a successful result; changed, replaced or removed inputs invalidate the round. Config contents are not copied into evidence. Inputs must still remain immutable; these checks do not create a snapshot or capture external registry, proxy or CA state. Older records without this config evidence cannot establish unchanged pull/cache settings.

Walking a wide tree for block/inode samples can perturb the pipe receiver and
source I/O. Procfs stat parsing preserves arbitrary process-name bytes, including names of unrelated processes. Process enumeration follows procfs parent PIDs and includes owned-session
members after reparenting, checking the leader start time; it does not require
the optional per-task `children` interface. A resource pass without samples, or whose samples never observe a live owned process, fails
instead of publishing zero peaks. The receiver retries interrupted and positive short writes before recording artifact bytes; a write without progress or a storage error fails the run. Scratch traversal uses bound directory descriptors and no-follow entry stats, so directory-to-symlink replacement cannot redirect its samples outside scratch. Proc references are bound before both pathname classification and stat, preventing descriptor reuse from mixing two objects. Resource scans retain each observed process's start time and session. Per-process reference contributions and errors are accepted only after rechecking the group and task identities across the complete scan, including re-enumeration of the full live-thread identity set; changed sets, exited or replaced tasks contribute no reference sample. These checks do not make procfs atomic or capture every reference shorter than the sampling interval. Scratch traversal and stopped-tree cleanup use an explicit depth-first stack of bound directory handles, opening each child relative to its parent and checking its identity once. Backtracking and exception paths close the scans and handles. Directory-opening work grows linearly with the tree size; retained handles grow with the active ancestor depth. Resource exhaustion is an error that invalidates the round, not a promise to handle arbitrary depth with a fixed FD budget. Regressions check operation growth and handle release on success, scan failure and actual EMFILE; the timeout is only a hang guard. Each live task is inspected for private FD/cwd/root tables; shared inodes are deduplicated and shared RSS is counted once per thread group. A zombie group leader remains observable and stoppable while a sibling is live; its recorded start time and session are rechecked around sibling reads to reject PID reuse. Deleted-open accounting includes a retained, removed scratch root and uses inode link count,
not the procfs filename suffix. Process `cwd` and `root` references are also inspected: a removed directory they retain contributes its inode and allocated bytes once, including in the historical `deleted_open_*` fields. A scratch inode retained only by a memory mapping must also be accounted for; an unmeasured mapping invalidates the resource sample. Failed teardown aborts the progress reader and
retains scratch instead of waiting indefinitely for stderr. Pair the resource pass with `--timing-only` under the same resource,
storage and cache conditions to measure that effect. Timing-only results keep
identity/content checks, child CPU/I/O accounting and owned teardown, but record
sampled peaks and the sampling interval as null. Retain both results; a timing
pass cannot establish a memory, block, inode or open-file bound.

For the historical #161 comparison, use the previous implementation, the
correct applier with old mkfs, and native Plan under the same conditions. Apply
`test/fixtures/flatten-old-layout.patch` only to an isolated copy of its recorded
native accelerator baseline, with the matching guest CLI and maintained mkfs
including the exact-read fix. Retain source commits and the patch digest. This
historical fixture is not a patch for the current Go planner.

For the Go generator comparison, use the maintained native Plan implementation
with the same correct applier, reads and transport as the control. The example
below compares complete outputs semantically and checks repeated identities
within each generator.

Run in the existing Workbench with Python 3.11 or newer. Input, scratch parent and
output root must be disjoint; the output root must not exist. A Registry input
must include its fixed SHA-256 digest. Supply a fixed expected-content JSON keyed
by relative path, including `.` for the root directory: each entry includes `type`, `mode`, `uid`, `gid`, and `mtime_ns` (0 under the fixed output timestamp policy), plus file
`size`/`sha256`, symlink `target` or special-file `device` and numeric `kind` (`stat.S_IFMT(st_mode)`, distinguishing
FIFO, socket, character and block device). For each non-directory
hard-link group (including symlinks and special files) in the selected tree, the lexicographically first relative path
is canonical; other members include `hardlink_to` naming that path. Content enumeration
is iterative on Python 3.11 and newer and reports directory scan errors; an unreadable
subtree invalidates verification instead of being treated as empty. The first occurrence of
each artifact identity is extracted with the selected native fsck, and its actual
contents and metadata must match. This extra extraction is outside measurement. After verification, only the owned
extraction tree has directory access restored for removal; external symlink
targets remain untouched. Scratch permission or I/O errors, including denied inspection of an owned process's open files, fail the measurement; directories removed by the producer and processes or file descriptors that disappear during sampling are tolerated.

```bash
python3 test/scripts/bench_flatten.py \
  --variant native,native/flatten-ctl,native/mkfs.erofs \
  --variant go,go/flatten-ctl --allow-layout-differences \
  --input fixtures/image.tar --expected fixtures/expected.json \
  --expected-config fixtures/runtime-config.json \
  --consumer go/flatten-ctl --fsck tools/fsck.erofs \
  --scratch-parent scratch --output-root results --trials 3 --fsync
```

The historical native controls require matching normal-corpus identities.
Go/native comparisons allow different layouts after independent content,
metadata, link and runtime-config checks. Cover low/high repetition, layer
overwrites, small files, constrained network and explicit cache
as separate fixed fixtures. Record persistent cache occupancy/lifetime separately
when supplying `--config`; it is never temporary scratch. End-to-end elapsed time
includes overlapped work; do not add overlapping stage durations. These host
measurements do not establish Guest ext4 or Host COW peaks. Measure those through
the real Guest/Builder path, including the receiver and owned teardown.

### Go generator measurements and accepted tradeoff

The linked Go generator removes the separate runtime mkfs executable. It does
not promise a universal speedup. The [#178 candidate comparison](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177535650)
and [accepted tradeoff](https://github.com/kuasar-sandbox/accelerator/issues/178#issuecomment-6177892609)
record the measured costs as well as the gains. All other semantic, failure,
determinism, static-package and Guest/Builder acceptance requirements remain.

The comparison used 4 CPUs, 6 GiB memory, fixed inputs, three alternating pairs,
Go 1.26.8, and a real output pipe. The x86 host supports AVX2 but not SHA-NI or
AVX-512. The native control uses the same correct umoci applier and maintained
native Plan/read/transport behavior. Timing and sampled-resource passes are
separate. These are fixture measurements, not bounds for arbitrary images.

| Fixture | Native EOF / accepted + fsync (s) | Go EOF / accepted + fsync (s) | Accepted + fsync change | Producer/owned-child CPU change |
| --- | --- | --- | --- | --- |
| Directory, 4 GiB low repetition | 57.198 / 61.353 | 61.919 / 62.821 | +2.4% | +22.1% |
| Directory, 1 GiB low repetition | 16.814 / 18.470 | 15.463 / 18.980 | +2.8% | +37.4% |
| Directory, 4 GiB high repetition | 11.028 / 11.072 | 1.225 / 1.257 | −88.6% | −86.8% |
| Directory, 20,000 small files | 1.092 / 1.662 | 1.194 / 1.802 | +8.4% | +12.9% |
| Registry, three 1 GiB overwrite layers, initial matrix | 45.460 / 48.926 | 53.600 / 55.011 | +12.4% | +24.5% |
| Same overwrite fixture, writeback/cache-controlled follow-up | 41.426 / 47.384 | 42.205 / 47.591 | +0.4% | +14.3% |
| Registry, 1 GiB, 32 MiB/s server | 47.988 / 50.995 | 45.092 / 51.339 | +0.7% | −2.8% |
| Registry, 1 GiB, warm explicit layer cache | 21.549 / 25.795 | 20.884 / 27.599 | +7.0% | +10.0% |

Each cell is a median of complete per-round measurements. Accepted + fsync
includes receiver acceptance and receiver fsync; EOF alone does not. The initial
overwrite comparison showed variance before planning. The bounded follow-up
warms the same fixed inputs and waits for prior writeback to settle without
changing resources or flushing global caches. Both results are retained.
Unavailable producer rusage I/O counters do not mean zero I/O; follow-up cgroup
device counters include the Registry, receiver, verification and cleanup.

For low-repetition 4 GiB input, sampled RSS increased from about 137 to 210 MiB,
and planner/index scratch from about 64 to 68 MiB. The source directory and
received artifact are outside that scratch figure. The artifact grew from
4,294,975,488 to 4,299,173,888 bytes (about 0.098%). Registry fixtures include the
private 1 GiB final rootfs: producer scratch is about 1.016 GiB for both versions,
with about 1 MiB extra in Go. Host measurements do not establish the fixed Guest
ext4 or Host COW limits, which require separate real VM evidence.

The measured planner uses two hashing workers and external go-erofs module
`v0.0.0-20261014160540-827da6f90114`. The 4 GiB low-repetition timing series uses
a diagnostic build with VCS stamping disabled; the normal Makefile build has
the same planner and independently passes content/resource checks with the same
artifact digest. The normal CLI grows by 18,909 bytes while the runtime removes
the separate approximately 3.9 MB mkfs payload. These candidate records do not
substitute for final acceptance identified by actual merged sources and formal
artifact hashes. The ARM Guest/Builder hardware exception remains explicitly
unverified; native ARM tests and static packages are separate evidence.
