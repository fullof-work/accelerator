// Standalone native ownership check, suitable for Valgrind. No Go runtime is
// involved; Go callback lifetime and pointer rules are covered by race/cgocheck.
#include "../../pkg/cache/rocks/internal/native/bridge.h"
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <rocksdb/db.h>

extern "C" int cache_rocks_evict(uintptr_t, char* key, size_t size) {
  return size && key[0] == 'c';
}

static void check(char* error) {
  if (error) {
    fprintf(stderr, "%s\n", error);
    std::free(error);
    std::abort();
  }
}

int main(int argc, char** argv) {
  assert(argc == 2);
  const std::string root = argv[1];  // runner-owned temporary directory
  cache_rocks_config cfg{};
  cfg.cache_bytes = 64 << 20;
  cfg.block_bytes = 64 << 10;
  cfg.write_buffer_bytes = 256 << 20;
  cfg.bloom_bits = 15;
  cfg.background_compactions = 8;
  cfg.track_blob_io = true;
  const std::string path = root + "/db";
  for (int round = 0; round < 8; ++round) {
    cache_rocks* db = nullptr;
    check(cache_rocks_open(path.c_str(), cfg, 1, 0, &db));
    cache_rocks* failed = nullptr;
    char* error = cache_rocks_open(path.c_str(), cfg, 1, 0, &failed);
    assert(error && !failed);
    std::free(error);
    for (int cf = 1; cf < 4; ++cf) {
      for (size_t size : {128, 8192}) {
        std::string value(size, 'v');
        for (const char* key : {"cold", "hot"}) {
          check(cache_rocks_put(db, cf, key, std::strlen(key), value.data(), value.size()));
          auto got = cache_rocks_get(db, cf, key, std::strlen(key));
          check(got.error);
          assert(got.pin && got.size == size && std::memcmp(got.data, value.data(), got.size) == 0);
          cache_rocks_unpin(got.pin);
        }
        check(cache_rocks_flush(db, cf));
        check(cache_rocks_compact(db, cf));
        auto got = cache_rocks_get(db, cf, "cold", 4);
        check(got.error);
        assert(!got.pin && !got.size);
        char* property = cache_rocks_property(db, cf, "rocksdb.estimate-num-keys");
        assert(property);
        std::free(property);
      }
    }
    check(cache_rocks_close(db));
    check(cache_rocks_open(path.c_str(), cfg, 0, 1, &db));
    check(cache_rocks_close(db));
  }

  // A DB containing an unexpected CF must fail without leaking partial handles.
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* extra = nullptr;
  const std::string other = root + "/extra-cf";
  assert(rocksdb::DB::Open(options, other, &extra).ok());
  rocksdb::ColumnFamilyHandle* cf = nullptr;
  assert(extra->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "unexpected", &cf).ok());
  delete cf;
  delete extra;
  for (int i = 0; i < 8; ++i) {
    cache_rocks* failed = nullptr;
    char* error = cache_rocks_open(other.c_str(), cfg, 1, 0, &failed);
    assert(error && !failed);
    std::free(error);
  }
}
