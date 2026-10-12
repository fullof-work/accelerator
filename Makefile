# accelerator — content acceleration (manifest / cache / store).
#
# Builds three CLIs:
#   manifest-ctl, store-ctl  — pure Go (CGO_ENABLED=0)
#   cache-ctl                — CGO, statically links librocksdb.a
#
# The public packages downstream repos import are CGO-free; only cache-ctl pulls
# RocksDB. pkg/remote uses go-containerregistry, but it is pure Go and isolated
# from the manifest/cache/store client paths.

SHELL := /bin/bash

.PHONY: all build manifest-ctl store-ctl cache-ctl deps-rocksdb test test-no-rocksdb vet bench perf-cache perf-cache-remote dedup-report release test-release clean help
.PHONY: test-e2e-scripts

# ---------------------------------------------------------------------------
# Architecture selection (identical block across all kuasar-sandbox repos)
# ---------------------------------------------------------------------------
HOST_ARCH   := $(shell uname -m)
TARGET_ARCH ?= $(HOST_ARCH)
ifeq ($(TARGET_ARCH),amd64)
  override TARGET_ARCH := x86_64
endif
ifeq ($(TARGET_ARCH),arm64)
  override TARGET_ARCH := aarch64
endif
ifeq ($(TARGET_ARCH),x86_64)
  GO_ARCH := amd64
else ifeq ($(TARGET_ARCH),aarch64)
  GO_ARCH := arm64
else
  $(error unsupported TARGET_ARCH=$(TARGET_ARCH); supported: x86_64, aarch64)
endif

# ---------------------------------------------------------------------------
# Build settings
# ---------------------------------------------------------------------------
GO             := go
GO_BUILD_FLAGS := -trimpath
BINDIR         := bin/$(TARGET_ARCH)
BUILD_DIR      := build/$(TARGET_ARCH)
ROCKS_PREFIX   := $(abspath $(BUILD_DIR)/rocksdb)

# Use the same target compiler for the RocksDB archive and the CGO link.
# Keep these settings local to the product recipes; host tools remain native.
ifeq ($(HOST_ARCH),$(TARGET_ARCH))
  CROSS_PREFIX ?=
else
  CROSS_PREFIX ?= $(TARGET_ARCH)-linux-gnu-
endif
TARGET_CC := $(if $(CROSS_PREFIX),$(CROSS_PREFIX)gcc,$(if $(filter default,$(origin CC)),gcc,$(CC)))
TARGET_CXX := $(if $(CROSS_PREFIX),$(CROSS_PREFIX)g++,$(if $(filter default,$(origin CXX)),g++,$(CXX)))

# CGO flags for cache-ctl: static link librocksdb.a + libstdc++; glibc stays
# dynamic. RocksDB is built without compression so the link line stays minimal.
CGO_CFLAGS         := -I$(ROCKS_PREFIX)/include
CGO_LDFLAGS_STATIC := -L$(ROCKS_PREFIX)/lib -Wl,-Bstatic -lrocksdb -lstdc++ -Wl,-Bdynamic -lm -lpthread -ldl
GOLDFLAGS_STATIC   := -linkmode=external -extldflags "-static-libstdc++ -static-libgcc -Wl,-Map,$(abspath $(BUILD_DIR)/cache-ctl.map)"

# NO_ROCKSDB=1 builds cache-ctl with RocksDB support compiled out:
# no librocksdb prerequisite, CGO_ENABLED=0, -tags no_rocksdb. The output
# artifact is the same $(BINDIR)/cache-ctl — same program, different
# compile-time feature set.
NO_ROCKSDB ?= 0

ifeq ($(NO_ROCKSDB),1)
CACHE_CTL_DEPS  :=
CACHE_CTL_CGO   := 0
CACHE_CTL_TAGS  := no_rocksdb
CACHE_CTL_BUILD = GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=$(CACHE_CTL_CGO) \
    $(GO) build $(GO_BUILD_FLAGS) -tags $(CACHE_CTL_TAGS) -o $(BINDIR)/cache-ctl ./cmd/cache-ctl
else
CACHE_CTL_DEPS  := deps-rocksdb
CACHE_CTL_CGO   := 1
# Let our target-specific CGO_LDFLAGS own the complete RocksDB link. The
# binding's default flags otherwise append host/shared compression and C++ libs.
CACHE_CTL_TAGS  := grocksdb_no_link
CACHE_CTL_BUILD = TMPDIR="$(abspath $(BUILD_DIR))" GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=$(CACHE_CTL_CGO) \
    CC="$(TARGET_CC)" \
    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS_STATIC)" \
    $(GO) build $(GO_BUILD_FLAGS) -tags $(CACHE_CTL_TAGS) -ldflags '$(GOLDFLAGS_STATIC)' -o $(BINDIR)/cache-ctl ./cmd/cache-ctl
endif

# Native-only symlink: bin/<name> -> $(TARGET_ARCH)/<name>. $(1) = basename.
define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
all: build

build: manifest-ctl store-ctl cache-ctl

manifest-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/manifest-ctl ./cmd/manifest-ctl
	$(call link_bin,manifest-ctl)

store-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/store-ctl ./cmd/store-ctl
	$(call link_bin,store-ctl)

# cache-ctl: the artifact target. NO_ROCKSDB=1 compiles RocksDB out.
cache-ctl: $(CACHE_CTL_DEPS)
	@mkdir -p $(BINDIR)
	$(CACHE_CTL_BUILD)
	$(call link_bin,cache-ctl)

# Build librocksdb.a locally (no compression). ~minutes cold.
deps-rocksdb: $(ROCKS_PREFIX)/lib/librocksdb.a
$(ROCKS_PREFIX)/lib/librocksdb.a:
	TARGET_ARCH="$(TARGET_ARCH)" \
	CROSS_PREFIX="$(CROSS_PREFIX)" \
	CC="$(TARGET_CC)" CXX="$(TARGET_CXX)" \
	BUILD_DIR="$(abspath $(BUILD_DIR))" \
	BINDIR="$(abspath $(BINDIR))" \
	TARBALL_CACHE="$(abspath build/tarball)" \
		bash deps/build-rocksdb.sh

# Compile/vet/test the no_rocksdb surface. Needs no librocksdb.
test-no-rocksdb:
	CGO_ENABLED=0 $(GO) build -tags no_rocksdb ./...
	CGO_ENABLED=0 $(GO) vet -tags no_rocksdb ./...
	CGO_ENABLED=0 $(GO) test -tags no_rocksdb ./pkg/cache/rocks

# Unit tests. cache/rocks tests need librocksdb (dynamic link is fine here).
test: deps-rocksdb test-e2e-scripts
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-native-build-budget.py
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="-L$(ROCKS_PREFIX)/lib -lrocksdb -lstdc++ -lm -lpthread -ldl" \
		$(GO) test ./...
	$(MAKE) test-no-rocksdb
	bash test/scripts/bench_cache_test.sh

# vet the CGO-free client surface (no librocksdb needed).
vet:
	CGO_ENABLED=0 $(GO) vet ./pkg/sparse/... ./pkg/tarstream/... ./pkg/manifest/... ./pkg/store/client/... ./pkg/cache/client/... ./pkg/flatten/... ./pkg/image/... ./pkg/remote/... ./pkg/tar/... ./cmd/manifest-ctl ./cmd/store-ctl

clean:
	rm -rf bin build

# ---------------------------------------------------------------------------
# Tests + benchmarks + perf
# ---------------------------------------------------------------------------
# Perf tools use the assembled platform binary set because they also exercise
# flatten-ctl. Integration E2E is owned by the prepared platform runner.
E2E_BIN ?= $(abspath ../kuasar-sandbox/bin/$(TARGET_ARCH))

bench: deps-rocksdb
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="-L$(ROCKS_PREFIX)/lib -lrocksdb -lstdc++ -lm -lpthread -ldl" \
		$(GO) test -bench=. -benchmem -run=^$$ ./...

perf-cache:
	BIN=$(E2E_BIN) bash test/scripts/bench_cache.sh

perf-cache-remote:
	BIN=$(E2E_BIN) bash test/scripts/bench_cache_remote.sh

dedup-report:
	BIN=$(E2E_BIN) bash test/scripts/dedup_report.sh

test-e2e-scripts:
	bash test/scripts/test_port_lease.sh
	PYTHONDONTWRITEBYTECODE=1 python3 test/scripts/test_e2e_manifest.py
	PYTHONDONTWRITEBYTECODE=1 python3 test/scripts/test_e2e_cache.py
	PYTHONDONTWRITEBYTECODE=1 python3 test/scripts/test_bench_flatten.py

VERSION ?= v0.1.0

release: build
	rm -rf build/release-bundle
	SOURCE_DATE_EPOCH="$$(git show -s --format=%ct HEAD)" \
		bash scripts/release.sh package "$(VERSION)" "$(TARGET_ARCH)" build/release-bundle

test-release:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
	bash scripts/test-release.sh

help:
	@echo "accelerator. Targets:"
	@echo "  build         build manifest-ctl + store-ctl + cache-ctl"
	@echo "  manifest-ctl  pure Go"
	@echo "  store-ctl     pure Go"
	@echo "  cache-ctl     CGO + librocksdb (auto deps-rocksdb); NO_ROCKSDB=1 to compile RocksDB out"
	@echo "  deps-rocksdb  build local librocksdb.a"
	@echo "  test          unit tests (needs librocksdb for cache/rocks)"
	@echo "  vet           vet the CGO-free client surface"
	@echo "  release       build a validated component release bundle"
	@echo "  test-release  test component release packaging"
	@echo "  clean         remove bin build"
	@echo "  TARGET_ARCH   x86_64 (default) | aarch64"
