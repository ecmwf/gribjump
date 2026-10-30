# GribJump Rust bindings

Rust bindings for ECMWF's [GribJump](https://github.com/ecmwf/gribjump) C++
library.

## Overview

GribJump extracts subsets of values from GRIB fields stored in an
[FDB](https://github.com/ecmwf/fdb) without decoding whole messages. These
crates make it usable from Rust: the C++ library is built from source (or
located on the system) at build time, and its extraction, axes and scan
operations are exposed through a safe API.

## Installation

Add the safe wrapper to your `Cargo.toml`:

```toml
[dependencies]
gribjump = "0.13"
```

Building requires CMake and a C++17 compiler. To link against an existing
GribJump installation instead of building from source, use the `system`
feature:

```toml
[dependencies]
gribjump = { version = "0.13", default-features = false, features = ["system"] }
```

## Crates

- **gribjump**: the safe API. Range extraction from FDB requests, files and
  paths, axes queries and index scanning, with GribJump, FDB and eckit
  exceptions surfaced as typed Rust errors.
- **gribjump-sys**: the low-level FFI layer using [CXX](https://cxx.rs/).
  Builds the GribJump C++ library from source (`vendored`) or links against a
  system installation (`system`), and exposes the raw bridge. Its Cargo
  features control which parts of the C++ library are compiled.

Both are versioned to track the GribJump release they build, and depend on
the [`fdb`](https://crates.io/crates/fdb), [`metkit`](https://crates.io/crates/metkit)
and [`eckit`](https://crates.io/crates/eckit) crates for the C++ libraries
underneath.

Inside a checkout of this repository, a `vendored` build compiles the C++
sources in the tree rather than cloning the release tag, so changes to `src/`
take effect without a network round-trip.

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
