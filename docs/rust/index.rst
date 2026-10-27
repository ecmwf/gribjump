.. _Rust_Introduction:

Rust interface
==============

:Version: |version|

GribJump answers one question: give me these grid points from those fields,
without decoding the fields. The ``gribjump`` crate asks it from Rust. A
program hands over MARS requests with index ranges, and gets back the values
and their validity bits for every matching field, read straight out of the
FDB or out of GRIB files on disk by the same C++ library that ``gribjump-extract``
and :ref:`pygribjump <PyGribJump_Introduction>` use.

Two crates live in ``rust/``: ``gribjump`` is the API applications use, and
``gribjump-sys`` is the `cxx <https://cxx.rs>`__ bridge underneath it, which
also builds or locates the C++ library. Both carry the version of the GribJump
release they wrap, and both pull in the eckit, metkit, ecCodes and FDB crates,
since GribJump is built on all four.

The crate exposes the public surface of ``gribjump::GribJump``: extraction
from MARS requests, from a MARS request that is expanded first, from GRIB
files by path and offset, the ``axes`` query, the two ``scan`` operations that
build the GribJump index, and ``stats``. The ``LogContext`` argument the C++
methods accept is not bridged, nor is the constructor that takes a ``Config``;
a ``GribJump`` is configured through the environment, as described in
:doc:`building`.

Quick start
-----------

.. code-block:: toml

   [dependencies]
   gribjump = "0.13"
   eckit = "2.5"

The default build compiles GribJump and the four libraries under it from
source, so it needs a C++17 compiler, CMake, git and Python 3. The ``eckit``
crate is there for ``eckit::init``.

The FDB to read from is the one the environment describes, through
``FDB5_CONFIG``, ``FDB5_CONFIG_FILE`` or ``FDB_HOME``. The request names one
field, the ranges are grid point indices with an exclusive end, and the hash
is the ``md5GridSection`` key of the field's GRIB message, which GribJump
checks against the field before it extracts:

.. literalinclude:: examples/quickstart.rs
   :language: rust

:doc:`usage` goes through each operation with a program that builds its own
FDB, :doc:`approach` explains what the crate does differently from the C++
API and why, and :doc:`building` covers the build and the run-time
configuration. The per-item reference is rustdoc's:

.. code-block:: bash

   cd rust
   cargo doc -p gribjump --no-deps --open

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   usage
   approach
   building
