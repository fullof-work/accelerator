//go:build !no_rocksdb

#include "bridge.h"
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <rocksdb/statistics.h>

namespace {
using namespace rocksdb;

// These indices match the fixed CF descriptors below and native/db.go.
constexpr int kChunkCF = 1;
constexpr int kManifestCF = 2;
constexpr int kBlobCF = 3;

char* error(const Status& s) {
  return s.ok() ? nullptr : ::strdup(s.ToString().c_str());
}

// No value is inspected or rewritten. Both paths consult the same Go sketch.
// The inherited IgnoreSnapshots() == true matches the former C filter.
class SketchFilter final : public CompactionFilter {
 public:
  explicit SketchFilter(uintptr_t callback) : callback_(callback) {}
  const char* Name() const override { return "cache/rocks/sketchFilter"; }
  bool Filter(int, const Slice& key, const Slice&, std::string*, bool*) const override {
    return cache_rocks_evict(callback_, const_cast<char*>(key.data()), key.size()) != 0;
  }
  Decision FilterBlobByKey(int, const Slice& key, std::string*, std::string*) const override {
    return cache_rocks_evict(callback_, const_cast<char*>(key.data()), key.size()) ? Decision::kRemove : Decision::kKeep;
  }
 private:
  uintptr_t callback_;
};
}  // namespace

// This is our own handle, never a redefinition/cast of a RocksDB C handle.
// Destruction also handles a partially successful Open. The filter must outlive
// DB destruction, which joins RocksDB's background jobs and their callbacks.
struct cache_rocks {
  std::unique_ptr<SketchFilter> filter;
  std::shared_ptr<rocksdb::Statistics> statistics;
  rocksdb::DB* db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> cfs;
  const rocksdb::ReadOptions read;
  const rocksdb::WriteOptions write;
  rocksdb::WriteOptions data_write;
  bool read_only = false;

  cache_rocks() {
    // Cache values can be refetched after a crash. Sketch metadata still uses
    // the original WAL-backed options above.
    data_write.disableWAL = true;
  }

  ~cache_rocks() {
    for (auto* cf : cfs) delete cf;
    delete db;
  }
};

struct cache_pin {
  rocksdb::PinnableSlice value;
};

extern "C" char* cache_rocks_open(const char* path, cache_rocks_config cfg,
                                  uintptr_t callback, int readonly, cache_rocks** out) {
  *out = nullptr;
  auto owner = std::make_unique<cache_rocks>();
  owner->read_only = readonly != 0;
  if (callback) owner->filter = std::make_unique<SketchFilter>(callback);

  rocksdb::BlockBasedTableOptions table;
  table.block_cache = rocksdb::NewLRUCache(cfg.cache_bytes);
  table.block_size = cfg.block_bytes;
  table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(cfg.bloom_bits, true));
  table.pin_l0_filter_and_index_blocks_in_cache = true;
  table.cache_index_and_filter_blocks = true;

  rocksdb::Options opts;
  if (cfg.track_blob_io) {
    owner->statistics = rocksdb::CreateDBStatistics();
    opts.statistics = owner->statistics;
  }
  opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
  opts.create_if_missing = true;
  opts.create_missing_column_families = true;
  opts.compression = rocksdb::kNoCompression;
  opts.use_direct_reads = cfg.direct_reads;
  opts.write_buffer_size = cfg.write_buffer_bytes;
  opts.max_write_buffer_number = 4;
  opts.max_background_compactions = cfg.background_compactions;
  opts.max_background_flushes = 2;

  // Preserve the existing per-CF scope: only default uses opts' write buffers.
  rocksdb::ColumnFamilyOptions data;
  data.table_factory = opts.table_factory;
  data.compression = rocksdb::kNoCompression;
  data.enable_blob_files = true;
  data.min_blob_size = 4 << 10;
  data.blob_file_size = 256 << 20;
  data.enable_blob_garbage_collection = true;
  data.compaction_filter = owner->filter.get();
  std::vector<rocksdb::ColumnFamilyDescriptor> cfs{
      {"default", opts}, {"chunk", data}, {"manifest", data}, {"blob", data}};
  auto status = readonly
      ? rocksdb::DB::OpenForReadOnly(opts, path, cfs, &owner->cfs, &owner->db, false)
      : rocksdb::DB::Open(opts, path, cfs, &owner->cfs, &owner->db);
  if (!status.ok()) return error(status);
  *out = owner.release();
  return nullptr;
}

extern "C" char* cache_rocks_close(cache_rocks* owner) {
  // All callers and pins have drained at the Go store's lifecycle gate.
  // WAL-free writes need an explicit flush whose failure reaches the caller;
  // relying on the DB destructor alone can silently lose unflushed values.
  std::string first_error;
  if (!owner->read_only) {
    rocksdb::FlushOptions flush;
    flush.wait = true;
    for (int cf : {kChunkCF, kManifestCF, kBlobCF}) {
      auto status = owner->db->Flush(flush, owner->cfs[cf]);
      if (!status.ok() && first_error.empty()) {
        first_error = "flush " + owner->cfs[cf]->GetName() + ": " + status.ToString();
      }
    }
  }
  // Even a failed flush must release the DB lock and every native resource.
  // Handles precede DB::Close; filter/callback survive both Close and delete.
  for (auto* cf : owner->cfs) delete cf;
  owner->cfs.clear();
  auto status = owner->db->Close();
  if (!status.ok() && first_error.empty()) first_error = status.ToString();
  delete owner;
  return first_error.empty() ? nullptr : ::strdup(first_error.c_str());
}

extern "C" cache_read cache_rocks_get(cache_rocks* owner, int cf, const char* key,
                                       size_t key_size) {
  cache_read result{};
  auto pin = std::make_unique<cache_pin>();
  auto status = owner->db->Get(owner->read, owner->cfs[cf],
                               rocksdb::Slice(key, key_size), &pin->value);
  if (status.IsNotFound()) return result;
  if (!status.ok()) {
    result.error = error(status);
    return result;
  }
  result.data = pin->value.data();
  result.size = pin->value.size();
  result.pin = pin.release();
  return result;
}

extern "C" void cache_rocks_unpin(cache_pin* pin) { delete pin; }

extern "C" char* cache_rocks_put(cache_rocks* owner, int cf, const char* key,
                                 size_t key_size, const char* value, size_t value_size) {
  const auto& write = (cf == kChunkCF || cf == kManifestCF || cf == kBlobCF)
      ? owner->data_write : owner->write;
  return error(owner->db->Put(write, owner->cfs[cf],
      rocksdb::Slice(key, key_size), rocksdb::Slice(value, value_size)));
}

extern "C" char* cache_rocks_flush(cache_rocks* owner, int cf) {
  return error(owner->db->Flush(rocksdb::FlushOptions(), owner->cfs[cf]));
}

extern "C" char* cache_rocks_compact(cache_rocks* owner, int cf) {
  return error(owner->db->CompactRange(rocksdb::CompactRangeOptions(),
                                      owner->cfs[cf], nullptr, nullptr));
}

extern "C" char* cache_rocks_property(cache_rocks* owner, int cf, const char* name) {
  std::string value;
  owner->db->GetProperty(owner->cfs[cf], name, &value);
  return ::strdup(value.c_str());
}

extern "C" uint64_t cache_rocks_blob_read_bytes(cache_rocks* owner) {
  return owner->statistics ? owner->statistics->getTickerCount(
      rocksdb::BLOB_DB_BLOB_FILE_BYTES_READ) : 0;
}
