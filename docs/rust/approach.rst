Approach
========

This page gives the reasons behind the shape of the interface. How to use it
is on :doc:`usage`. The FDB the library reads is configured as the FDB
documentation describes; ``eckit::init`` and the eckit error classes are
documented in the Rust Interface chapter of the eckit documentation.

Two Crates
----------

``gribjump-sys``
   Unsafe bindings to the C++ library, built on `cxx <https://cxx.rs>`_.

   * :ref:`Builds libgribjump from source or links an installed one <rust/building:Building the Rust Crates>`.
   * Converts eckit exceptions into typed errors.

``gribjump``
   Safe Rust API over ``gribjump-sys``.

   * :ref:`gribjump::Result and a matchable gribjump::Error <rust/usage:Errors>`.
   * :ref:`One handle shared between threads <rust/approach:Handle>`.
   * :ref:`Results read in place <rust/approach:Results>`.
   * :ref:`Requests as plain structs <rust/approach:Requests>`.

Results
-------

A result is read in place: ``range`` and ``iter`` return ``RangeView``,
slices borrowed from the C++ result, so values are copied only when
``to_owned`` or ``to_owned_ranges`` asks for it. The validity words are
built on first access and cached inside the result, which is why
``ExtractionResult`` is ``Send`` but not ``Sync``.

Handle
------

``GribJump`` is one shared handle; ``extract`` returns after every
requested field has been read, so iterating the result does no I/O and
holds no lock. The first ``GribJump`` created in a process fixes the
process-wide settings, thread counts among them, from the environment and
the configuration file.

Calls block the calling thread for the time the C++ library spends on I/O.
In an async program, run them on a blocking thread, for example with
``tokio::task::spawn_blocking``.

Requests
--------

``ExtractionRequest``, ``PathExtractionRequest`` and ``FileExtraction`` are
plain structs, to clone and reuse. ``Range`` is start-inclusive,
end-exclusive, like ``std::ops::Range``, from which it converts.

An FDB or path request carries the grid hash of the field, the
``md5GridSection`` key of its GRIB message. GribJump refuses a field on
another grid: the index ranges would mean other points.

Error Type
----------

``gribjump::Error`` nests ``eckit::Error`` instead of copying its variants,
so an eckit exception thrown inside GribJump is the same value it would be
coming from eckit itself. GribJump's own exception classes, such as
``DataNotFoundException``, have no variant and arrive as
``eckit::Error::Other`` with their message.
