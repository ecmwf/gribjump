# gribjump-sys

Low-level Rust bindings to ECMWF's [GribJump](https://github.com/ecmwf/gribjump) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe API, use the [`gribjump`](https://crates.io/crates/gribjump) crate instead.

## Cargo build features

These flags control what the underlying C++ GribJump library is compiled with.

### Build strategy (mutually exclusive)

- `vendored` - Build GribJump and its dependencies (FDB, metkit, eckit,
  ecCodes) from source.
- `system` - Link against system-installed GribJump.

Note: neither is enabled by default on `gribjump-sys` itself. End users should
depend on the higher-level [`gribjump`](https://crates.io/crates/gribjump)
crate, which defaults to `vendored`. If you depend on `gribjump-sys` directly
you must select one explicitly.

### Core (enabled by default)

- `local-extract` - Local extraction and server-side functionality
  (`ENABLE_GRIBJUMP_LOCAL_EXTRACT=ON`).

## Environment variables

- `GRIBJUMP_DIR` - Location of a system GribJump install (`system` builds).
- `CMAKE_PREFIX_PATH` - Additional CMake search paths.
- `DOCS_RS` - When set, the build script becomes a no-op (for docs.rs).

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
