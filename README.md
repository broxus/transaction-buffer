# transaction-buffer

## Prebuilt RocksDB

`weedb` builds RocksDB from source by default. To reuse a prebuilt library,
set `ROCKSDB_LIB_DIR` to the directory containing `librocksdb.so` or
`librocksdb.a` before running Cargo.

This crate uses `librocksdb-sys 0.17.3+10.4.2`, so the prebuilt library and
headers must be RocksDB `10.4.2`. The Rust bindings are generated from the
RocksDB headers and must match the linked library.

```bash
export ROCKSDB_LIB_DIR=/path/to/rocksdb/build
export ROCKSDB_INCLUDE_DIR=/path/to/rocksdb/include

# Use this for a static librocksdb.a. Omit it for librocksdb.so.
export ROCKSDB_STATIC=1

# Keep artifacts in the repository if CARGO_TARGET_DIR points to an
# unavailable shared location in the shell environment.
CARGO_TARGET_DIR=target cargo build
```

To build a compatible copy manually:

```bash
sudo apt install clang lld libjemalloc-dev libgflags-dev libzstd-dev liblz4-dev

git clone https://github.com/facebook/rocksdb.git /path/to/rocksdb
cd /path/to/rocksdb
git checkout v10.4.2

cmake -S . -B build \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_CXX_FLAGS=-Wno-nontrivial-memcall \
  -DWITH_LZ4=ON \
  -DWITH_ZSTD=ON \
  -DWITH_JEMALLOC=ON
cmake --build build --parallel
```

The build produces `build/librocksdb.a` and `build/librocksdb.so`. Use the
first with `ROCKSDB_STATIC=1`; for faster debug links, omit that variable and
use the shared library instead.

## Logging (tracing)

`transaction-buffer` uses `tracing`.

Optional Cargo feature:

- `compact-logs` reduces TTY log noise
  - removes extra span fields emitted by this library

Example:

```bash
cargo test --features compact-logs
```
