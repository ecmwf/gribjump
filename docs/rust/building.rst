Building and configuring
========================

Cargo drives the whole build; CMake and ecbuild run from the ``gribjump-sys``
build script. It needs Rust 1.90, a C++17 compiler, CMake, git, and Python 3
for the ecCodes build.

Where the libraries come from
-----------------------------

With the default ``vendored`` feature the build compiles GribJump from
source, and with it eckit, ecCodes, metkit and the FDB, into Cargo's output
directory. Inside a checkout of the gribjump repository the sources of that
checkout are built, so C++ and Rust changes are tested together; the build
warns when the checkout's ``VERSION`` differs from the crate version.
Elsewhere the release tag matching the crate version is cloned.

With ``system`` the build links an installed GribJump, 0.11.0 or later, found
through CMake with ``GRIBJUMP_DIR`` or ``CMAKE_PREFIX_PATH``, and likewise
``FDB_DIR``, ``METKIT_DIR``, ``ECCODES_DIR`` and ``ECKIT_DIR`` for the
libraries under it:

.. code-block:: toml

   [dependencies]
   gribjump = { version = "0.13", default-features = false, features = ["system"] }

The two features are mutually exclusive and are forwarded down the whole
chain, so all five libraries come from the same place. ``gribjump-sys`` has
one more, ``local-extract``, on by default, which builds the extraction
engine itself; off, the crate can only talk to a GribJump server.

A vendored build's shared libraries are found at run time through rpaths on
the final binary, which an application's ``build.rs`` adds:

.. code-block:: rust

   fn main() {
       bindman_utils::emit_rpaths();
   }

with ``bindman-utils = "0.2"`` under ``[build-dependencies]``.

Run-time configuration
----------------------

``GribJump::new`` takes no arguments. The FDB it reads comes from the
environment, resolved as the FDB library does it: ``FDB5_CONFIG`` holding the
YAML, else ``FDB5_CONFIG_FILE`` naming a file, else ``FDB_HOME`` with
``etc/fdb/config.yaml`` under it. GribJump's own options come from
``GRIBJUMP_CONFIG_FILE``, or ``GRIBJUMP_HOME`` with ``etc/gribjump/config.yaml``
under it, and individual options have environment overrides, listed in
:doc:`../gribjump/list_of_configuration_options`. Three of them matter for
a program using this crate:

``GRIBJUMP_IGNORE_GRID``
   Skip the grid hash check. Read once, at the first extraction.

``GRIBJUMP_THREADS``
   Size of the extraction thread pool, fixed by the first ``GribJump`` of the
   process.

``GRIBJUMP_DEBUG``
   Verbose library logging, which ``eckit::init`` routes to the ``log`` crate
   under the target ``gribjump``.

For the index itself, written by the FDB plugin or by the scan tools, see
:doc:`../gribjump/setup_gribjump_on_fdb`; ``scan_paths`` and
``scan_requests`` are the same operations as ``gribjump-scan-files`` and
``gribjump-scan``.

Working on the crates
---------------------

From ``rust/``:

.. code-block:: bash

   cargo clippy --all-targets
   cargo fmt
   cargo test -- --test-threads=1
   cargo doc --no-deps --open

The integration tests build an FDB in a temporary directory and set
``FDB5_CONFIG`` for the process, which is why they run single-threaded. The
examples under ``rust/crates/gribjump/examples`` are command-line tools in
the style of ``gribjump-extract``; the ones in ``docs/rust/examples`` are the
programs quoted in these pages, complete with the FDB they create, and build
as examples of a package depending on the ``gribjump``, ``fdb`` and
``eckit`` crates.
