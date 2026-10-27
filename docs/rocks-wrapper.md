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
| ReadOptions / WriteOptions | Immutable members of the native owner; concurrent synchronous operations share them. There is no native-object `sync.Pool`. |
| PinnableSlice | One Get. The Go bridge copies to the existing BlobPool before destroying the pin, including panic cleanup. No native view escapes to networking. |
| Compaction filter | One owner per DB; borrowed by all three data CFs. It outlives DB close/destruction and background callbacks. |
| Go callback | One `runtime/cgo.Handle`, deleted after failed Open or completed DB destruction. C++ retains the integer only. Callback panics preserve the key and are logged. |
| Errors and properties | Copied to Go, then freed in the same bridge call. |

The store's lifecycle lock drains complete requests before teardown. Close then
joins the sketch's background workers, persists the final sketch, and closes
the native owner. Concurrent Close calls wait for the same result. Late requests
return a closed-store error; missing keys remain ordinary cache misses.

## Preserved configuration and data

- CF names remain `default`, `chunk`, `manifest`, `blob`; unknown partitions
  still fall back to chunk. Object keys are 32 bytes; shard keys append byte
  `0x00`. Values, shard prefixes and empty-value-as-miss behavior are unchanged.
- `__freq_sketch__` remains in default CF with the existing version-2 encoding,
  counters, threshold, decay and persistence rules. Default CF is never filtered.
- `disk_bytes` defaults to 1 TiB and scales the block cache with `mem_ratio`
  (default 0.01, minimum cache 64 MiB). It is not a disk quota.
- Block size defaults to 64 KiB, Bloom bits to 15; L0 index/filter pinning and
  index/filter caching remain enabled. The serialized Bloom description now
  includes its parameters instead of the old C wrapper's generic name.
- Direct reads still default to true. Direct flush/compaction I/O is unchanged.
- `write_buffer_bytes` (default 256 MiB) and four write buffers still apply only
  to default CF. Data CFs retain the engine defaults. Background compactions
  default to 8 and background flushes to 2.
- Data CFs retain BlobDB, the 4 KiB inline threshold, 256 MiB blob files and GC.
  Compression remains off; default WAL and sync settings are unchanged.
- Read-only opening remains `OpenForReadOnly` with WAL files tolerated. It may
  replay WAL into memory; it is not a secondary instance. It creates no sketch.
- Inline filtering and `FilterBlobByKey` consult the same sketch and make only
  keep/remove decisions. Snapshot-ignoring semantics are unchanged. Necessary
  GC relocation may still read surviving blobs.

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
reads, readonly/reopen, BlobGC and inline/blob eviction. A private opt-in engine
counter proves cold-blob filtering avoids value reads; statistics remain
disabled in production.
