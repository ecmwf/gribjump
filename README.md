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

The python interface, `pygribjump`, can be installed with pip:

```bash
python3 -m pip install --upgrade git+ssh://git@github.com/ecmwf/gribjump.git@master
```

`pygribjump` uses [findlibs](https://github.com/ecmwf/findlibs) to find an installed `libgribjump` library. You can use `export GRIBJUMP_HOME=/path/to/gribjump/install` to help with finding the library.

See the `pygribjump/example*.py` files for simple use cases.


## Acknowledgements

Past and current funding and support is listed in the adjoining [Acknowledgements](./ACKNOWLEDGEMENTS.rst).