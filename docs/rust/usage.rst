Using the ``gribjump`` crate
============================

The programs on this page are in ``docs/rust/examples``. They start by
creating a local FDB in a temporary directory, archiving a three-field GRIB
file into it and pointing ``FDB5_CONFIG`` at it, which is what
``GribJump::new`` reads to find the FDB:

.. literalinclude:: examples/common/mod.rs
   :language: rust
   :start-after: // [setup]
   :end-before: // [end setup]

A real program skips this and sets the environment as the ``gribjump-*``
tools expect; see :doc:`building`. Call ``eckit::init()`` first either way:
it sets up the eckit runtime and sends the library's log output to the Rust
``log`` crate.

Extracting from an FDB
----------------------

An ``ExtractionRequest`` is a MARS request that selects exactly one field,
the ranges of grid point indices to read from it, and the grid hash. Several
requests go into one ``extract`` call, which returns one result per request
in the same order. A result has one ``RangeView`` per range, with the values
and a validity bit per value; a bit is clear where the field has no data at
that point.

.. literalinclude:: examples/extract.rs
   :language: rust
   :start-after: // [extract]
   :end-before: // [end extract]
   :dedent:

The hash is the ``md5GridSection`` key of the GRIB message, and every field
has to match it: GribJump refuses to return values from a field on a
different grid, since the index ranges would mean different points. Setting
``GRIBJUMP_IGNORE_GRID=1`` in the environment turns the check off for the
whole process.

``extract_mars`` takes one request with several values per key, expands it
and extracts the same ranges from every field it matches:

.. literalinclude:: examples/extract.rs
   :language: rust
   :start-after: // [mars]
   :end-before: // [end mars]
   :dedent:

A ``RangeView`` borrows from its result. To keep values after the result is
gone, or to move them to another thread, copy them out:

.. literalinclude:: examples/extract.rs
   :language: rust
   :start-after: // [owned]
   :end-before: // [end owned]
   :dedent:

Extracting from files
---------------------

GribJump reads GRIB files directly, given the byte offset of each message.
``eckit::MessageReader`` is one way to find the offsets:

.. literalinclude:: examples/file.rs
   :language: rust
   :start-after: // [offsets]
   :end-before: // [end offsets]
   :dedent:

``extract_from_file`` takes one file with a list of offsets and the ranges to
read at each; the grid is not checked. ``extract_from_paths`` takes one
``PathExtractionRequest`` per message, each with its own path, offset and
hash, which is the form an FDB listing produces:

.. literalinclude:: examples/file.rs
   :language: rust
   :start-after: // [file]
   :end-before: // [end paths]
   :dedent:

Building the index
------------------

Extraction from an FDB is fast only where a GribJump index exists next to
the data, either written by the FDB plugin at archive time or built
afterwards by scanning. ``scan_requests`` scans the fields an FDB request
matches and ``scan_paths`` scans GRIB files, writing a ``.gribjump`` file
next to each ``.data`` or GRIB file; both return the number of messages
scanned. ``axes`` lists the values present for each key of the fields a
request matches, from the FDB index:

.. literalinclude:: examples/index.rs
   :language: rust
   :start-after: // [scan]
   :end-before: // [end axes]
   :dedent:

Threads
-------

``GribJump`` is ``Clone``, and a clone is a second reference to the same
handle. Each thread takes one; the calls serialise on the handle's lock for
the time it takes to start an extraction, and the iterators run without it:

.. literalinclude:: examples/threads.rs
   :language: rust
   :start-after: // [threads]
   :end-before: // [end threads]
   :dedent:

A result can move to another thread but not be shared between two, and the
calls block, so in an async program they belong on a blocking thread.

Errors
------

``Range::new`` fails before any C++ runs; everything the library rejects,
a grid hash that does not match among it, comes back as
``Error::Eckit``:

.. literalinclude:: examples/errors.rs
   :language: rust
   :start-after: // [errors]
   :end-before: // [end errors]
   :dedent:
