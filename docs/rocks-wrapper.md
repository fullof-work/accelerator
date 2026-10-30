[English](rocks-wrapper.md) | [简体中文](rocks-wrapper_zh.md)

# Cache RocksDB wrapper

The private binding is in `pkg/cache/rocks/internal/native`. It implements only
the cache store's fixed four column families, opening, pinned reads, writes,
flush, compaction, properties and frequency filtering. The normal
`deps/build-rocksdb.sh` owns the RocksDB version and build; this wrapper adds no
native dependency recipe.

## Public engine boundary and ownership

RocksDB 9.7.4's C API exposes pinned reads but not `FilterBlobByKey`. Its opaque
options cannot accept a custom C++ filter through a public conversion API.
The small C++ bridge therefore owns the DB directly using public headers. It
does not reinterpret C handles, include internal headers, register factories
globally, or provide a general Go object model.

| Object | Lifetime |
| --- | --- |
| DB and four CF handles | One native owner. Partial open failure destroys all returned handles and DB. Handles are destroyed before DB close. |
| Options and table configuration | Local to Open. The DB copies options. Shared table factory, LRU and Bloom policy references survive through DB ownership and release automatically. |
| ReadOptions / WriteOptions | Initialized once by the native owner and shared by concurrent synchronous operations. Data CFs use WAL-free writes; default CF retains WAL. There is no native-object `sync.Pool`. |
| PinnableSlice | One Get. The Go bridge copies to the existing BlobPool before destroying the pin, including panic cleanup. No native view escapes to networking. |
| Compaction filter | One owner per DB; borrowed by all three data CFs. It outlives DB close/destruction and background callbacks. |
| Go callback | One `runtime/cgo.Handle`, deleted after failed Open or completed DB destruction. C++ retains the integer only. Callback panics preserve the key and are logged. |
| Errors and properties | Copied to Go, then freed in the same bridge call. |

The store's lifecycle lock drains complete requests before teardown. Close then
joins the sketch's background workers and persists the final sketch. Before
closing a writable DB, the native owner explicitly waits for each data CF to
Flush. It preserves the first flush error while still closing the DB and
releasing every handle; final sketch-persistence errors also reach the caller.
Read-only Close skips Flush. Concurrent Close calls wait for the same result.
Late requests return a closed-store error; missing keys remain ordinary misses.

## Configuration and persistence

- CF names remain `default`, `chunk`, `manifest`, `blob`; unknown partitions
  still fall back to chunk. Object keys are 32 bytes; shard keys append byte
  `0x00`. Values, shard prefixes and empty-value-as-miss behavior are unchanged.
- `__freq_sketch__` remains in default CF with the existing version-2 encoding.
  Only the active CMS is restored; the previous generation and the access-count
  and successful-write byte progress are not persisted. Default CF is never
  filtered.
- `disk_bytes` defaults to 1 TiB and scales the block cache with `mem_ratio`
  (default 0.01, minimum cache 64 MiB). It also supplies the successful-value byte
  budget per sketch generation for soft aging, not a limit on physical disk
  usage or resident data.
  All three data CFs contribute, including shard prefixes and repeated writes;
  failed writes, reads and default-CF persistence add no bytes. Count-, byte- and
  explicitly configured timer-triggered rollovers clear both progress counters.
  One write rolls at most once. Natural compaction performs eviction; this adds
  no idle maintenance or forced compaction and is not a disk quota.
- Block size defaults to 64 KiB, Bloom bits to 15; L0 index/filter pinning and
  index/filter caching remain enabled. The serialized Bloom description now
  includes its parameters instead of the old C wrapper's generic name.
- Direct reads still default to true. Direct flush/compaction I/O is unchanged.
- `write_buffer_bytes` (default 256 MiB) and four write buffers still apply only
  to default CF. Data CFs retain the engine defaults. Background compactions
  default to 8 and background flushes to 2.
- Data CFs retain BlobDB, the 4 KiB inline threshold, 256 MiB blob files and GC.
  Compression remains off. Chunk, manifest and blob writes always disable WAL;
  default/sketch retains its WAL and sync settings. These are internal choices,
  not additional configuration options.
- Read-only opening remains `OpenForReadOnly` with WAL files tolerated. It may
  replay WAL into memory; it is not a secondary instance. It creates no sketch.
- Inline filtering and `FilterBlobByKey` consult the same sketch and make only
  keep/remove decisions. Snapshot-ignoring semantics are unchanged. Necessary
  GC relocation may still read surviving blobs.

Normal Close flushes surviving data-CF writes and reports failures. An abnormal
process exit may lose unflushed cache values, yielding ordinary misses that can
be refilled. Default-CF checkpoints remain WAL-backed, but per-write sync is not
enabled: a successful Fill or unflushed checkpoint does not promise synchronous
persistence through power loss. `freq.disable_eviction` changes only filtering,
not this durability policy.

## Validation commands

Use `make test`, `make test-no-rocksdb`, `make test-release` and the normal
architecture/release targets. For focused native tests after `make deps-rocksdb`:

```sh
export CGO_CPPFLAGS="-I$PWD/build/$(uname -m)/rocksdb/include"
export CGO_LDFLAGS="-L$PWD/build/$(uname -m)/rocksdb/lib -lrocksdb -lstdc++ -lm -lpthread -ldl"
go test -race -count=3 ./pkg/cache/rocks/...
GOEXPERIMENT=cgocheck2 go test ./pkg/cache/rocks/...
bash test/scripts/test_rocks_native.sh
```

The last command needs Valgrind and exercises the actual bridge/library without
the Go runtime, including locked/partial opens and repeated open/close. The
Go tests cover callback lifetime, panic recovery, concurrent shutdown, corrupt
reads, readonly/reopen, BlobGC and inline/blob eviction. Recovery tests separate
flushed prefixes from unflushed tails across normal Close and SIGKILL, check
WAL recovery of the latest sketch checkpoint, and verify that flush errors are
returned without retaining the DB lock. A private opt-in engine
counter proves cold-blob filtering avoids value reads; statistics remain
disabled in production.
