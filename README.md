# GribJump

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity)

GribJump is a C++ library for extracting subsets of data from GRIB files, particularly data archived in the [FDB](https://github.com/ecmwf/fdb).

<!-- :warning: **This is a work in progress.** Expect bugs, and for interfaces and functionality to change. Consult the developers before using. -->

> \[!IMPORTANT\]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

## Installation

### Dependencies

Currently, GribJump has the following runtime dependencies:

- [eccodes](https://github.com/ecmwf/eccodes)
- [eckit](https://github.com/ecmwf/eckit)
- [metkit](https://github.com/ecmwf/metkit)
- [fdb5](https://github.com/ecmwf/fdb)
- [libaec](https://github.com/MathisRosenhauer/libaec)

as well as [ecbuild](https://github.com/ecmwf/ecbuild) for building.

### C++ Library

Building and installing:

```bash
   # Environment --- Edit as needed
   srcdir=$(pwd)
   builddir=build
   installdir=$HOME/local  
   
   # 1. Create the build directory:
   mkdir $builddir
   cd $builddir

   # 2. Run ecbuild
   ecbuild --prefix=${installdir} -- \
   -DENABLE_MEMFS=ON \
   -DENABLE_ECCODES_THREADS=ON \
   -DENABLE_AEC=ON \
   -DCMAKE_INSTALL_PREFIX=$</path/to/installations> ${srcdir}
   
   # 3. Compile / Install
   make -j10
   make install
```

A clientside build (which cannot extract data but talks to a server which can) is built using the flag `-DENABLE_GRIBJUMP_LOCAL_EXTRACT=OFF`. This requires only eckit and metkit to be installed.

### Python Library

The pybind11-based Python interface, `pygribjump`, requires Python 3.11 or newer
and can be installed from PyPI:

```bash
python3 -m pip install --upgrade pygribjump
```

`pygribjump` uses [findlibs](https://github.com/ecmwf/findlibs) to find an installed `libgribjump` library. You can use `export GRIBJUMP_HOME=/path/to/gribjump/install` to help with finding the library.

For a source build, install pybind11 >=3.0.1 and the Python `build` package, enable
`-DENABLE_PYTHON_GRIBJUMP_INTERFACE=ON` when configuring GribJump, and build the
`pygribjump-wheel` target. Install the resulting `pygribjump-*.whl` from the build
directory with pip. The central bundle release builds this wheel alongside
`gribjumplib` and pins it to the matching native-library release.

The legacy CFFI sources are retained but are no longer published. Their packaging
configuration is archived as `pyproject-cffi.toml` to exclude it from automatic
release discovery. See the [migration guide](docs/pygribjump/migration.rst) for
API differences.

See the `docs/examples/` files for simple use cases.


## Acknowledgements

Past and current funding and support is listed in the [Acknowledgements](./ACKNOWLEDGEMENTS.rst).
