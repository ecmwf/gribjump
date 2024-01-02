# GribJump

GribJump is a C++ library for extracting subsets of data from GRIB files archived in the [FDB](https://github.com/ecmwf/fdb).

:warning: **This is a work in progress.** Consult the developers before using.

## Installation

### Dependencies

Currently, GribJump has the following runtime dependencies:

- [eccodes](https://github.com/ecmwf/eccodes)
- [eckit](https://github.com/ecmwf/eckit)
- [metkit](https://github.com/ecmwf/metkit)
- [fdb5](https://github.com/ecmwf/fdb)

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
   ecbuild --prefix=$installdir -- -DCMAKE_INSTALL_PREFIX=</path/to/installations> $srcdir
   
   # 3. Compile / Install
   make -j10
   make install
```

### Python Library

The python interface, `pygribjump`, can be installed with pip:

```bash
python3 -m pip install --upgrade git+ssh://git@github.com/ecmwf/gribjump.git@master
```

`pygribjump` uses [findlibs](https://github.com/ecmwf/findlibs) to find an installed `libgribjump` library. You can use `export GRIBJUMP_HOME=/path/to/gribjump/install` to help with finding the library.

See `pygribjump/test.py` for a simple use case.
