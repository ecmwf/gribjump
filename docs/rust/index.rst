.. _Rust_Introduction:

Rust Interface
==============

:Version: |version|

The Rust interface lives in the ``rust/`` directory of the gribjump
repository and calls the same GribJump library as the ``gribjump-*`` tools
and :ref:`pygribjump <PyGribJump_Introduction>`. This chapter is for
developers writing applications against the crates.

``gribjump`` is the crate applications depend on; it gives the C++ library a
Rust API, with extraction requests as plain structs, results read in place
from the library's memory and a handle that threads share. ``gribjump-sys``
builds or links the C++ library underneath it.

.. note::

   Use ``gribjump`` in your code. ``gribjump-sys`` is the crate that builds
   the C++ library (:doc:`building`); calling it directly means handling
   ``cxx`` pointers and raw exception strings yourself.

Coverage
--------

.. list-table::
   :widths: 32 33 35
   :header-rows: 1

   * - Rust
     - C++
     - Notes
   * - ``gribjump::GribJump``
     - ``gribjump::GribJump``
     - ``extract``, ``extract_mars``, ``extract_from_file``,
       ``extract_from_paths``, ``axes``, ``scan_paths``, ``scan_requests``
       and ``print_stats``
   * - ``gribjump::Range``, ``ExtractionRequest``,
       ``PathExtractionRequest``, ``FileExtraction``
     - The request types of ``gribjump::GribJump``
     - Grid point ranges with an exclusive end, the MARS request or file that
       names the field and its grid hash
   * - ``gribjump::ExtractionIterator``, ``ExtractionResult``,
       ``RangeView``, ``RangeResult``
     - ``gribjump::ExtractionIterator``, ``gribjump::ExtractionResult``
     - One result per field; values and validity bits per range, borrowed
       from the result or copied out
   * - ``gribjump::version``, ``gribjump::git_sha1``
     - ``LibGribJump::version``, ``LibGribJump::gitsha1``
     - Version of the linked library
   * - ``gribjump::Error``
     - The ``eckit::Exception`` classes
     - ``Eckit`` for an exception thrown by the C++ libraries,
       ``InvalidArgument`` and ``IteratorExhausted`` for failures in Rust
       code

Quick Start
-----------

Add the crates:

.. code-block:: bash

   cargo add gribjump eckit

The crate version is the version of the GribJump release it wraps. ``eckit``
provides ``eckit::init``. :doc:`building` lists the requirements and the
alternative of linking installed libraries.

The program extracts two ranges of grid points from the first message of a
GRIB file, the three-field fixture of the crate's tests:

.. literalinclude:: ../../rust/crates/gribjump/examples/quickstart.rs
   :language: rust
   :start-after: // [main]
   :end-before: // [end main]

.. toctree::
   :maxdepth: 2
   :caption: Contents
   :hidden:

   approach
   usage
   building

The per-item API reference is on docs.rs: `gribjump
<https://docs.rs/gribjump>`_ and `gribjump-sys
<https://docs.rs/gribjump-sys>`_.
