#!/usr/bin/env bash
# Run after make deps-rocksdb. All outputs are temporary; the source/library
# version and recipe are the same ones used by cache-ctl.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
prefix="$root/build/$(uname -m)/rocksdb"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
"${CXX:-g++}" -std=c++17 -fno-rtti -g -O1 -I"$prefix/include" \
  "$root/test/native/rocks_wrapper_test.cc" \
  "$root/pkg/cache/rocks/internal/native/bridge.cc" \
  "$prefix/lib/librocksdb.a" -lpthread -ldl -o "$scratch/check"
valgrind --quiet --leak-check=full --show-leak-kinds=definite,indirect \
  --errors-for-leak-kinds=definite,indirect --error-exitcode=1 \
  "$scratch/check" "$scratch"
