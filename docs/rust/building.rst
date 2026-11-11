Building the Rust Crates
========================

.. code-block:: bash

   cargo add gribjump eckit
   cargo build

This builds GribJump, the FDB, metkit, eckit and ecCodes from source as part
of ``cargo build`` (the ``vendored`` feature, on by default). It needs Rust
1.90 or later, a C++17 compiler, CMake and git. The first build
also needs network access, to download ecbuild and the releases matching the
crate versions. When the crate comes from a checkout of the gribjump
repository, as a path or git dependency, the C++ sources of that checkout
are compiled instead.

To link an installed GribJump, 0.11.0 or later, switch the crates to the
``system`` feature and point CMake at the installations with
``GRIBJUMP_DIR``, ``FDB_DIR``, ``METKIT_DIR`` and ``ECKIT_DIR`` or
``CMAKE_PREFIX_PATH``:

.. code-block:: bash

   cargo add gribjump --no-default-features --features system
   cargo add eckit --no-default-features --features system
   GRIBJUMP_DIR=/path/to/gribjump/install cargo build

Use ``system`` when the program must share one installation with other
software, for example on the HPC or in a bundle build and ``vendored`` for a
self-contained build.

The shared libraries of a vendored build are found at run time through
rpath entries on the binary. An application adds them from its build
script:

.. code-block:: toml

   [build-dependencies]
   bindman-utils = "0.2"

.. code-block:: rust

   // build.rs
   fn main() {
       bindman_utils::emit_rpaths();
   }

Running the Examples
--------------------

The programs quoted in :doc:`usage` are examples of the ``gribjump`` crate,
in ``rust/crates/gribjump/examples``. From a checkout of the repository:

.. code-block:: bash

   cd rust
   cargo run -p gribjump --example quickstart
   cargo run -p gribjump --example extract

Configuration
-------------

GribJump's own options come from the file named by ``GRIBJUMP_CONFIG_FILE``,
else ``etc/gribjump/config.yaml`` under ``GRIBJUMP_HOME``. The options and
their environment overrides are listed in
:doc:`../gribjump/list_of_configuration_options`; three matter for a program
using this crate:

``GRIBJUMP_IGNORE_GRID``
   Skip the grid hash check. Read once, when the first ``GribJump`` is
   created.

``GRIBJUMP_THREADS``
   Size of the extraction thread pool, fixed by the first ``GribJump`` of
   the process.

``GRIBJUMP_DEBUG``
   Verbose library logging.

For the index itself, written by the FDB plugin or by the scan tools, see
:doc:`../gribjump/setup_gribjump_on_fdb`; ``scan_paths`` and
``scan_requests`` are the operations of ``gribjump-scan-files`` and
``gribjump-scan``.
