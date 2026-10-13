# gribjump

Safe Rust wrapper for ECMWF's [GribJump](https://github.com/ecmwf/gribjump) library.

GribJump extracts subsets of values from GRIB fields stored in an
[FDB](https://github.com/ecmwf/fdb) without decoding whole messages.

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
gribjump = "0.13"
```

The default `vendored` feature builds GribJump and its dependencies (FDB,
metkit, eckit, ecCodes) from source, which requires CMake and a C++17
compiler.

## Usage

`GribJump::new()` takes no configuration. FDB reads its own from
`FDB5_CONFIG` or `FDB5_CONFIG_FILE` in the environment; see the upstream
FDB docs.

```rust,no_run
use gribjump::{ExtractionRequest, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gj = GribJump::new()?;

    // Value index ranges to extract: start inclusive, end exclusive.
    let ranges = vec![Range::new(0, 100)?, Range::new(500, 600)?];

    // One request per field; the grid hash is checked against the field's grid.
    let request = ExtractionRequest::new(
        "class=od,expver=0001,stream=oper,date=20230508,time=1200,step=1,param=151130",
        ranges,
        "33c7d6025995e1b4913811e77d38ec50",
    );

    // Results borrow the C++ buffers; use `to_owned` to keep them.
    for result in gj.extract(&[request])? {
        let result = result?;
        for range in result.iter() {
            println!("{} values: {:?}", range.len(), range.values());
        }
    }
    Ok(())
}
```

Extraction from GRIB files on disk (`extract_from_file`, `extract_from_paths`),
axes queries (`axes`) and index scanning (`scan_paths`, `scan_requests`) are
covered by the examples in this crate.

## Cargo build features

- `vendored` (default) - Build GribJump and its dependencies (FDB, metkit,
  eckit, ecCodes) from source.
- `system` - Link against a system-installed GribJump.

Lower-level feature flags live on the
[`gribjump-sys`](https://crates.io/crates/gribjump-sys) crate; see its README
for the full list.

## Running

Binaries and `cargo run` need no `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`
setup on macOS or Linux.

All C++ shared libraries use `@rpath` install names. Binary crates that
depend on `gribjump` should add a one-line `build.rs`:

```rust
fn main() {
    bindman_utils::emit_rpaths();
}
```

This stamps absolute RPATH entries onto the final binary pointing at each
dependency's build output, so the dynamic linker finds them automatically.

### System / FHS-packaged installs (e.g. RPM, deb)

When the target system already provides GribJump and its dependencies,
typically as distro packages installed under `/usr/lib{,64}` with headers
under `/usr/include`, build against the system libraries with:

```bash
cargo build --release --no-default-features --features system
```

The build script calls `find_package(gribjump)` (and the same for fdb5 /
metkit / eckit / eccodes), links the Rust binary against those system
libraries, and stamps absolute RPATH entries pointing at the lib directories
the CMake search resolved. To use a custom prefix, point `CMAKE_PREFIX_PATH`
at your install tree before running cargo (e.g.
`CMAKE_PREFIX_PATH=/opt/ecmwf cargo build --features system`).

## Copyright and License

Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF).

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE) which can also be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
