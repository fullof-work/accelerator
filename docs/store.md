[English](store.md) | [简体中文](store_zh.md)

# store — persistent-storage read/write proxy

`store-ctl` provides a unified persistent-storage data plane for `manifest-ctl` and cache origins. Object data may reside on a local/shared filesystem or in S3-compatible object storage. The generation list can independently come from the main configuration, a regular file or a separate S3 object.

<a id="1-核心模型"></a>
## 1. Core model

There is one representation of generations:

```go
[]store.Generation // oldest -> newest
```

The last entry receives new writes; reads search backward from that entry. The list is neither sorted by name nor automatically deduplicated. Names must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. Empty lists, duplicate entries, `.`, `..`, path separators, control characters and excessively long lists are rejected.

`AdmitWrite` / `AdmitWriteFor` return:

```go
type WriteAdmission struct {
    Generation store.Generation
    Salt       [32]byte
}
```

This contains routing and cryptographic-derivation information, not credentials. An empty `AdmitWrite` request chooses the last entry in the current list. `AdmitWriteFor(generation)` requests a specific entry that is still present. One manifest ingest performs one admission RPC; all chunk Puts and the final manifest Put reuse that admission. An admission remains writable while its generation is in the current list. Once removed, that generation accepts no Put and no longer participates in Get.

Generation salt has one public implementation:

```go
salt, err := store.SaltForGeneration(generation)
```

The Store server, offline Manifest Bundle writer and tests all call it rather than duplicating the derivation-domain constant. It first calls `ValidateGeneration`. Every valid string, including `NONE`, is an ordinary generation; there is no reserved value or special branch.

<a id="2-配置"></a>
## 2. Configuration

<a id="21-fs-数据后端"></a>
### 2.1 Filesystem data backend

```yaml
listen: 127.0.0.1:7100
backend: fs
stats_interval: 30s
cache_listen: ""
fs:
  root: /var/store
  verify_content_key: true
  direct_io: false
```

`direct_io` defaults to `false` and affects only object Get. Generation files and the main configuration always use ordinary I/O.

<a id="22-s3-数据后端"></a>
### 2.2 S3 data backend

```yaml
listen: 127.0.0.1:7100
backend: s3
s3:
  endpoint: https://example-s3-endpoint
  region: us-east-1
  bucket: kuasar-store
  path_style: true
  prefix: production
  access_key: ${S3_ACCESS_KEY}
  secret_key: ${S3_SECRET_KEY}
  verify_content_key: true
  max_inflight: 64
  op_timeout: 10s
  max_object_size_bytes: 16777216
  tls:
    ca_cert: ""
    insecure_skip_verify: false
```

Static AK/SK values must be supplied together. When both are empty, the AWS SDK default credential chain is used. String fields support `${VAR}` expansion. `region` defaults to `us-east-1`; `path_style` defaults to `true`.

`tls.ca_cert` is a path to a PEM CA bundle (may hold several certificates) appended to the system trust store — the way to trust an endpoint (or intercepting proxy) whose CA is not in the system store. `tls.insecure_skip_verify: true` skips TLS certificate verification entirely; it is insecure — traffic, including credentials, can be intercepted — and is meant for testing only, not production. The two are mutually exclusive. Both apply to object traffic for the configured endpoint only; the SDK credential chain's identity requests (instance role, web identity, SSO) always use the system trust store, so on a fully intercepted network they need the CA in the system store or static AK/SK.

### 2.3 Generation source

Exactly one source must be selected under `generations`.

A list in the main configuration:

```yaml
generations:
  config:
    - G1
    - G2
    - G3
```

A file:

```yaml
generations:
  refresh_interval: 5s
  file:
    path: /var/store-meta/generations
```

A separate S3 object:

```yaml
generations:
  refresh_interval: 5s
  s3:
    endpoint: https://example-s3-endpoint
    region: us-east-1
    bucket: kuasar-store-meta
    key: production/__meta/generations
    path_style: true
    access_key: ${GENERATION_S3_ACCESS_KEY}
    secret_key: ${GENERATION_S3_SECRET_KEY}
    tls:
      ca_cert: ""
      insecure_skip_verify: false
```

`generations.s3.tls` is honored independently of the data backend's; when the `generations` section is omitted, the legacy backend-local S3 location inherits the data backend's setting.

File and S3 sources use line-oriented text in oldest-to-newest order. They refresh periodically according to `refresh_interval`; `SIGHUP` also triggers an immediate refresh. For a config source, `SIGHUP` rereads the main YAML but does not dynamically change the backend, listener, credentials or other configuration. A new list replaces the current immutable slice only after complete loading, parsing and validation. A failed refresh is logged and retains the last valid list. Startup is rejected when no valid list is available.

Omitting `generations` preserves the legacy deployment locations:

- FS: `<fs.root>/__meta/generations`
- S3: `<s3.prefix>/__meta/generations`

Object-data permissions and generation-source permissions can be separate. For example, a data node can have read-only generation credentials and object read/write credentials, without permission to modify the generation object.

### 2.4 `verify_content_key`

When `true` (the default), the server hashes the incoming upload stream with SHA-256; its digest must equal the request key. Verification failure removes only a file still owned by the current writer. Deduplication against an existing target still uses only optional size or existence: it does not reread and hash the existing object.

When `false`, uploaded content is not hash-verified:

- With a size: the actual upload size must equal it; an existing target with the same size is a dedup hit.
- Without a size: the existence of a regular file/object is a dedup hit; a new upload has no expected-size check.

Consequently, incorrect content of the same size cannot be identified. Without a size, leftover or incomplete files may also count as existing. This is an explicit tradeoff when disabling content verification.

<a id="3-rpc-与-backend"></a>
## 3. RPC and Backend

Store gRPC provides:

```protobuf
rpc AdmitWrite(AdmitWriteRequest) returns (AdmitWriteResponse);
rpc Get(GetRequest) returns (stream GetResponse);
rpc Put(stream PutRequest) returns (PutResponse);
```

An empty `AdmitWriteRequest.generation` keeps the existing behavior of choosing the newest writable generation. A nonempty invalid name returns `InvalidArgument`; a valid name absent from the current list observed by this request returns `FailedPrecondition`. An empty server generation list returns `Unavailable`. A successful response always returns the canonical `WriteAdmission` for that generation. The official Go client retains `AdmitWrite(ctx)` and additionally provides `AdmitWriteFor(ctx, generation)`. Put wire format and the Backend interface are unchanged.

`PutHeader` contains the partition, 32-byte key, generation and a presence-aware `optional uint64 size`. Zero means a known empty object; only absence means unknown size. The official Go client receives a complete `[]byte`, so it always sends `uint64(len(data))`. The server checks overflow before converting to `int64`.

A data backend operates only on the one explicitly supplied generation:

```go
type Backend interface {
    Get(ctx context.Context, generation store.Generation,
        partition store.Partition, key store.ContentKey) (bool, []byte, error)

    Exists(ctx context.Context, generation store.Generation,
        partition store.Partition, key store.ContentKey,
        expectedSize *int64) (bool, error)

    OpenPut(generation store.Generation, partition store.Partition,
        key store.ContentKey, expectedSize *int64) (store.PutHandle, error)
}
```

`OpenPut` copies the optional size and binds the generation, partition and key. At Commit, the handle does not reread the list or reselect a generation or destination. A Commit key different from the bound key is rejected.

Server Put proceeds as follows:

1. Confirm that the admitted generation remains in the current list observed by this request.
2. Parse optional size and call `Exists`.
3. On a hit, immediately return `is_new=false` without receiving the payload.
4. On a miss, call `OpenPut` and count actual bytes.
5. When size is supplied, Abort immediately on overflow and require exact equality at EOF.
6. Perform the configured streaming digest verification and Commit.

FS `Exists` accepts only regular files. Known size must match exactly; without size, regular-file existence suffices. NotExist is a miss. Symlinks, directories, special files, permission errors, EIO, ESTALE and similar conditions are errors. S3 performs HEAD using the caller's context. NotFound is a miss; other remote errors propagate unchanged. Known size is compared with Content-Length.

gRPC Get and embedded `cache_listen` use the same reverse-lookup entry point. Each request reads the generation slice once:

```go
for i := len(generations) - 1; i >= 0; i-- {
    found, data, err := backend.Get(ctx, generations[i], partition, key)
    if err != nil || found {
        return found, data, err
    }
}
```

### 3.1 Read attempt and write boundary

Store `Get` performs one business read attempt. Failed streams are canceled/released, and a later call starts a complete new object read; confirmed absence remains different from transport/access failure. The existing endpoint and operation timeout bound that attempt, while caller context owns any later retry/backoff.

This read contract never authorizes replay of `Put`, Fill, ingestion, publication or snapshot capture. A lost response does not prove that a write was not applied. Direct CLI reads return their single-attempt error to the command owner. `pkg/readerr` markers are applied only where Store knows a semantic boundary such as confirmed absence or integrity/format failure; opaque access, network EOF/partial stream and backend-local cancellation keep their original causes.

## 4. FS direct-final-write

The object path is:

```text
<root>/<partition>/<generation>/<aa>/<bb>/<content-key>
```

`OpenPut` creates this final path directly:

1. Create parent directories and inspect the target with Lstat.
2. If the target is valid under optional-size rules, return a no-op handle. Its Write accepts and discards data; Commit returns `isNew=false`.
3. When size is known and an existing regular file has the wrong size, record its identity. Lstat again before deleting; delete only if the path still identifies that file, otherwise recheck.
4. Create the final file with `O_WRONLY | O_CREAT | O_EXCL` and `O_NOFOLLOW` protection. `EEXIST` indicates a race; return to target inspection.

The direct-write handle records the final path, created-file identity, generation, partition, key, optional size and actual byte count. Write writes directly to the final descriptor and prohibits overflow when size is known.

Abort, digest mismatch, length errors, and Write/Sync/Close failures first check whether the path still identifies the file created by the handle; only its owner may delete it. Successful Commit performs file Sync, Close and parent-directory synchronization, then confirms identity again. If another writer has replaced the path:

- If the replacement satisfies optional-size/existence rules, the loser returns `isNew=false`.
- If the replacement is invalid, missing or cannot be confirmed, the loser returns an error.
- The loser never deletes another writer's file.

This concurrency decision needs no process-wide lock and supports multiple writers competing for the same content key.

<a id="41-可见性与故障边界"></a>
### 4.1 Visibility and failure boundaries

Direct-final-write does not provide crash-atomic publication or an equivalent guarantee:

- The final path is visible during writes; its size may temporarily be incomplete.
- The official client always sends size, so incomplete size normally prevents a subsequent Put dedup hit.
- Manifest ingest completes all chunk Puts before publishing the manifest. Normal readers do not request the new chunks before their reference is published.
- A later same-size Put can recognize and replace a wrong-size file left by a crash.
- Size alone cannot distinguish a leftover file that reached full size but was not yet persisted.
- Without size, an incomplete file can count as existing.

These boundaries follow from writing directly to the final path without adding a separate object-completion marker.

## 5. FS Direct I/O

On Linux with `direct_io: true`, object Get opens an `O_DIRECT` descriptor using `golang.org/x/sys/unix` and first attempts `statx(STATX_DIOALIGN)` to obtain memory/offset alignment. Buffer addresses, offsets and read lengths meet that alignment; returned data is trimmed to the actual file length. The implementation handles empty files, small files, unaligned lengths, short reads and EOF.

If the filesystem does not report usable alignment, or open/read returns `EINVAL`, `EOPNOTSUPP` or another unsupported condition, Get returns an explicit error rather than silently falling back to buffered I/O. Non-Linux builds likewise return an explicit unsupported error. Decide whether to enable this on the target NFS/SFS Turbo environment using its benchmark:

```bash
go test -run '^$' -bench 'BenchmarkGet(Buffer|Direct)' -benchmem ./pkg/store/fs
```

<a id="6-admin-命令"></a>
## 6. Administrative commands

```bash
store-ctl init    --config FILE --generation G1
store-ctl rollout --config FILE --generation G2
store-ctl info    --config FILE
store-ctl purge   --config FILE --generation G1
store-ctl purge   --config FILE --all --confirm
store-ctl serve   --config FILE
```

- `info` reads the oldest-to-newest list from the generation source and shows its last entry as the current write generation. Explicit generation admin helpers collect object statistics.
- `init`/`rollout` modify file or S3 sources. A config source is explicitly read-only and must be updated by the configuration system.
- `purge --generation` rejects the last entry, removes the generation from a writable source, then deletes FS/S3 data. S3 source updates use ETag `If-Match` CAS, rereading and retrying within a bound on conflicts.
- `purge --all` is offline maintenance and still requires `--confirm`: stop every `store-ctl serve` process and quiesce all writers first. File/S3 sources are removed. For a config source, only object data is removed, not the main YAML. A running server treats a missing source as refresh failure and deliberately retains its last valid list; therefore this command is not an online write-revocation mechanism.

A write-generation rollout does not retire existing data:

```text
[G1] -> [G1, G2]              new default writes use G2; G1 remains readable
[G1, G2] -> [G2]              only after G1 is no longer needed by retained data
```

After the first refresh, new default admissions use G2, listed G1 admissions may
still complete, and Get reads G2 then G1. An explicit admission can still select
G1 while it is listed. Finishing old writes is therefore neither a read-retention
policy nor evidence that G1 can be deleted. Existing images, templates, snapshots
and their parent chains can continue to reference objects in G1 indefinitely.

Before removing G1, the deployment owner must establish that no retained artifact
or parent reference depends on objects available only through that generation.
Where an alternative source is used, verify that the original references and
required content remain resolvable from that durable source; a cache hit or a
newer write generation is not such evidence. Without this proof, retain G1 in the
read list. The server does not discover live application references or migrate
objects when rolling out a generation.

Coordinate all readers and writers and allow in-flight operations to complete
before physical deletion. A refreshed server stops looking in a removed
generation even if its files still exist; a stale server or in-flight request may
still hold the previous list. Each request captures one list, and refresh is not
a fleet-wide synchronization barrier. `purge --generation` combines list removal
and destructive data deletion: it is not a reference-aware garbage collector.
Backups and retirement decisions belong to the operator. `purge --all` additionally
requires retiring or replacing every affected reference and an intentional data
loss or recovery plan, not merely stopping the processes. Valid list changes do
not require a `store-ctl serve` restart; this does not make unsafe deletion safe.

<a id="7-s3-数据面"></a>
## 7. S3 data plane

The object layout matches FS:

```text
<prefix>/<partition>/<generation>/<aa>/<bb>/<content-key>
```

PutHandle buffers within a memory bound and writes the fixed key with one PutObject. `max_inflight` limits concurrent S3 calls; an empty `op_timeout` leaves timeout control solely to the caller's context; `max_object_size_bytes` limits object size. The generation source may use an entirely different S3 endpoint, bucket, key and credentials from the data backend.

<a id="8-cache_listen-与运维"></a>
## 8. `cache_listen` and operations

A nonempty `cache_listen` starts a read-only cache wire server. ObjectGet accesses chunks/manifests/blobs through the Server's same cross-generation read helper. ObjectPut and shard operations are rejected. This is a pass-through entry point without L1; deploy cache-ctl tiered when local caching is needed.

`stats_interval` defaults to 30s and emits get/put/admit rates, bandwidth, dedup ratio, latency, inflight counts and errors. `0`/`off` disables it. `STORE_CTL_DEBUG=1` enables slow-S3-operation tracing; `STORE_CTL_SLOW` sets its threshold.

`store-ctl serve --config FILE [--drain-timeout DURATION]` closes admission and drains admitted requests on the first SIGTERM/SIGINT. The second exit signal cancels in-flight work; further signals are idempotent. SIGHUP retains generation refresh while running and stops triggering it during shutdown. Store adds no management port or separate readiness framework.

Drain covers every Get stream Send and Put reception, verification, Commit and SendAndClose. gRPC and optional cache_listen share an exit barrier: neither entrance finishing first can close the shared backend early. Executing, admitted queued and pending wire requests retain complete responses; local reader shutdown does not cancel handlers as though the peer disconnected. After foreground work ends, join generation refresh, stats and other actual users, then close App-owned backends/clients. Borrowed providers/loggers are not closed. Forced cancellation preserves existing Put Abort semantics; a commit followed by a lost response remains uncertain.

`--drain-timeout` defaults to `0`, adding no internal overall limit. A positive value is one graceful budget shared by all entrances from the first soft stop; a negative value is rejected before resource creation. It adds no YAML/env setting. Expiry can cancel work and close transports, but Run must still join uncancellable FS/provider calls before closing resources and returning. Final process termination belongs to the manager or SDK host; this budget is not a hard Run return deadline.

Ordinary bind releases entrances on exit; the next instance initializes and binds after the old instance exits. Drain does not migrate connections or change the accepted eventual-consistency and revocable-object deduplication constraints of FS. Rollback changes no format. Low-level clients remain single attempt; transport errors never become fabricated success or miss.

Validation:

```bash
CGO_ENABLED=0 go test ./pkg/store/... ./pkg/manifest/... ./cmd/store-ctl
CGO_ENABLED=1 go test -race ./pkg/store/... ./pkg/manifest/... ./cmd/store-ctl
make store-ctl
make manifest-ctl
make vet
make test-e2e-scripts
```

The commands above are source/unit/helper validation. Product Store/cache E2E is executed from the prepared platform workspace through the common runner and the `storage.*.sh` cases; see [`../test/e2e/README.md`](../test/e2e/README.md). It consumes prebuilt products and does not compile product or helper source during E2E execution.

The race-detector command requires CGO and a working C toolchain. Disabling CGO is valid for the ordinary test command above, not for `go test -race`.

## 9. Reusable Store application entry point

`app/store.Run(ctx, cfg, opts)` owns the complete Store serving lifecycle. `store-ctl serve` is only one caller; a custom executable may link the same App directly and does not exec or require the official service binary. `Run` does not read `STORE_CONFIG`, register process signals, call `os.Exit`/`log.Fatal`, or mutate a process-global logger. The caller owns configuration loading, signals and exit policy.

A minimal custom executable that deliberately reuses the repository's YAML adapter is:

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    storeapp "github.com/kuasar-sandbox/accelerator/app/store"
    storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
)

func main() {
    if len(os.Args) != 2 { log.Fatal("usage: custom-store CONFIG") }
    cfg, err := storeconfig.LoadConfig(os.Args[1], true)
    if err != nil { log.Fatal(err) }
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    if err := storeapp.Run(ctx, *cfg, storeapp.Options{}); err != nil { log.Fatal(err) }
}
```

A deployment can point systemd at that binary directly:

```ini
[Service]
ExecStart=/opt/kuasar/bin/custom-store /etc/kuasar/store.yaml
Restart=on-failure
KillSignal=SIGTERM
```

Direct Go construction of `storeapp.Config` is equally supported. Dynamic generation or credential providers are explicit `Options` bindings documented by the Go API; they are process-local bindings and are not written back into YAML. A custom caller that wants reload must own its reload trigger and provider callback just as `store-ctl` owns SIGHUP adaptation.

`Options` adds zero-value-compatible `Drain <-chan struct{}` and `DrainTimeout time.Duration`. A nil Drain preserves the old SDK cancellation usage shown above; closing Drain starts irreversible soft shutdown. Context cancellation always cancels in-flight work, immediately escalates drain, and takes precedence when simultaneous. Drain during initialization prevents subsequent started publication or serving. A host can pass `Options{Drain: drain, DrainTimeout: 30*time.Second}`, adapt its first exit request to `close(drain)` and later requests to its own cancel function; the SDK does not register signals. See §8 for the budget and resource ordering.

Validation of the reusable entry point should compile the custom module independently and exercise real Store protocol requests with the official service binary absent from `PATH`.

## 9. See also

- [manifest.md](manifest.md): manifest ingest/fetch and encryption.
- [cache.md](cache.md): tiered cache, origins and wire protocol.
- Repository-root `README.md` / `Makefile`: build and full-test entry points.

Store read-attempt and non-replay boundaries are specified in [§3.1](#31-read-attempt-and-write-boundary).
