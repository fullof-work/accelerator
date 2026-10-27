//go:build !no_rocksdb

// Package native is the private cache store bridge to RocksDB's public C++ API.
// The C API cannot install FilterBlobByKey without depending on opaque layouts,
// so one narrow owner handles the DB, fixed CFs, options and sketch callback.
// Callers must exclude Close from every operation, including Read's copy.
package native

/*
#cgo CXXFLAGS: -std=c++17 -fno-rtti
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"errors"
	"log"
	"runtime/cgo"
	"unsafe"
)

const (
	Default = iota
	Chunk
	Manifest
	Blob
)

// Config contains resolved settings, with precisely the historical CF scope.
type Config struct {
	// TrackBlobIO enables the engine counter only for validation. Production leaves statistics disabled.
	TrackBlobIO                              bool
	CacheBytes, BlockBytes, WriteBufferBytes uint64
	BloomBits, BackgroundCompactions         int
	DirectReads                              bool
}

type DB struct {
	ptr      *C.cache_rocks
	callback cgo.Handle
}

// Open keeps evict alive until Close has joined all RocksDB callbacks. No Go
// pointer is retained by C++; failure deletes the handle just like Close.
func Open(path string, cfg Config, readOnly bool, evict func([]byte) bool) (*DB, error) {
	db := &DB{}
	if evict != nil {
		db.callback = cgo.NewHandle(evict)
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	c := C.cache_rocks_config{
		track_blob_io:          flag(cfg.TrackBlobIO),
		cache_bytes:            C.uint64_t(cfg.CacheBytes),
		block_bytes:            C.uint64_t(cfg.BlockBytes),
		write_buffer_bytes:     C.uint64_t(cfg.WriteBufferBytes),
		bloom_bits:             C.int(cfg.BloomBits),
		background_compactions: C.int(cfg.BackgroundCompactions),
		direct_reads:           flag(cfg.DirectReads),
	}
	if err := takeError(C.cache_rocks_open(cpath, c, C.uintptr_t(db.callback), flag(readOnly), &db.ptr)); err != nil {
		if db.callback != 0 {
			db.callback.Delete()
		}
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	err := takeError(C.cache_rocks_close(db.ptr))
	db.ptr = nil
	if db.callback != 0 {
		db.callback.Delete()
		db.callback = 0
	}
	return err
}

// Read copies the pinned result into a caller-owned buffer synchronously.
// alloc must return a buffer of the requested length and must not retain any
// native view. Pins never survive this call, even if alloc panics.
func (db *DB) Read(cf int, key []byte, alloc func(int) []byte) (bool, error) {
	// Return one native POD by value. Out-parameters would force separate Go
	// heap allocations for the pin, pointer and size on every read.
	result := C.cache_rocks_get(db.ptr, C.int(cf), bytesPtr(key), C.size_t(len(key)))
	if err := takeError(result.error); err != nil {
		return false, err
	}
	if result.pin == nil {
		return false, nil
	}
	defer C.cache_rocks_unpin(result.pin)
	buf := alloc(int(result.size))
	if len(buf) != int(result.size) {
		panic("rocks: invalid read buffer length")
	}
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(result.data)), int(result.size)))
	return true, nil
}

func (db *DB) Put(cf int, key, value []byte) error {
	return takeError(C.cache_rocks_put(db.ptr, C.int(cf), bytesPtr(key), C.size_t(len(key)), bytesPtr(value), C.size_t(len(value))))
}

// BlobReadBytes exposes the engine counter for key-only filtering validation.
func (db *DB) BlobReadBytes() uint64 { return uint64(C.cache_rocks_blob_read_bytes(db.ptr)) }

func (db *DB) Flush(cf int) error   { return takeError(C.cache_rocks_flush(db.ptr, C.int(cf))) }
func (db *DB) Compact(cf int) error { return takeError(C.cache_rocks_compact(db.ptr, C.int(cf))) }
func (db *DB) Property(cf int, name string) string {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	value := C.cache_rocks_property(db.ptr, C.int(cf), cname)
	defer C.free(unsafe.Pointer(value))
	return C.GoString(value)
}

func flag(v bool) C.int {
	if v {
		return 1
	}
	return 0
}
func bytesPtr(v []byte) *C.char { return (*C.char)(unsafe.Pointer(unsafe.SliceData(v))) }
func takeError(p *C.char) error {
	if p == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(p))
	return errors.New(C.GoString(p))
}

//export cache_rocks_evict
func cache_rocks_evict(handle C.uintptr_t, key *C.char, size C.size_t) (remove C.int) {
	// A panic cannot unwind through a C++ background thread. Preserve the key
	// and report the failure; never turn a callback failure into data eviction.
	defer func() {
		if p := recover(); p != nil {
			remove = 0
			log.Printf("rocks: compaction callback panic (key preserved): %v", p)
		}
	}()
	evict := cgo.Handle(handle).Value().(func([]byte) bool)
	if evict(unsafe.Slice((*byte)(unsafe.Pointer(key)), int(size))) {
		return 1
	}
	return 0
}
