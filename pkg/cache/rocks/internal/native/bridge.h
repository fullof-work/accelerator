#ifndef ACCELERATOR_ROCKS_BRIDGE_H
#define ACCELERATOR_ROCKS_BRIDGE_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct cache_rocks cache_rocks;
typedef struct cache_pin cache_pin;
typedef struct {
  uint64_t cache_bytes, block_bytes, write_buffer_bytes;
  double bloom_bits;
  int background_compactions, direct_reads, track_blob_io;
} cache_rocks_config;

// Errors and property strings are malloc-owned; the caller frees them.
// CF indexes are fixed: default=0, chunk=1, manifest=2, blob=3.
char* cache_rocks_open(const char*, cache_rocks_config, uintptr_t, int, cache_rocks**);
char* cache_rocks_close(cache_rocks*);
typedef struct {
  cache_pin* pin;
  const char* data;
  size_t size;
  char* error;
} cache_read;
cache_read cache_rocks_get(cache_rocks*, int, const char*, size_t);
void cache_rocks_unpin(cache_pin*);
char* cache_rocks_put(cache_rocks*, int, const char*, size_t, const char*, size_t);
char* cache_rocks_flush(cache_rocks*, int);
char* cache_rocks_compact(cache_rocks*, int);
char* cache_rocks_property(cache_rocks*, int, const char*);

uint64_t cache_rocks_blob_read_bytes(cache_rocks*);

// Implemented by Go. Only an integer cgo.Handle crosses the retained boundary.
int cache_rocks_evict(uintptr_t, char*, size_t);
#ifdef __cplusplus
}
#endif
#endif
