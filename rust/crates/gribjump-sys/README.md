# gribjump-sys

Low-level Rust bindings to ECMWF's [GribJump](https://github.com/ecmwf/gribjump) C++ library.

This crate provides raw FFI bindings using [cxx](https://cxx.rs/). For a safe, ergonomic API, use the [`gribjump`](https://crates.io/crates/gribjump) crate instead.

## Features

- `vendored` (default) - Build GribJump and dependencies from source
- `system` - Link against system-installed GribJump
- `local-extract` - Local extraction and serverside functionality

## License

Apache-2.0
