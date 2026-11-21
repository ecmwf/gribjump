Using the ``gribjump`` Crate
============================

The code on this page is quoted from the examples of the ``gribjump``
crate; :ref:`rust/building:Running the Examples` says how to run them. The
methods of each type are listed on `docs.rs <https://docs.rs/gribjump>`_.

Runtime Initialisation
----------------------

Call ``eckit::init()`` before any other call. Library messages then arrive
through the ``log`` crate under the target ``eckit``; with
``GRIBJUMP_DEBUG=1`` in the environment the debug channel arrives under the
target ``gribjump``.

Opening
-------

The FDB a ``GribJump`` reads comes from the environment, as for the FDB
tools: ``FDB_CONFIG`` holding the YAML, else ``FDB_CONFIG_FILE`` naming a
file, else ``etc/fdb/config.yaml`` under ``FDB_HOME``. The examples on this
page create a local FDB in a temporary directory, archive a three-field GRIB
file into it and set ``FDB_CONFIG`` before opening:

.. literalinclude:: ../../rust/crates/gribjump/examples/common/mod.rs
   :language: rust
   :start-after: // [setup]
   :end-before: // [end setup]

Extracting from an FDB
----------------------

An ``ExtractionRequest`` is a MARS request that selects exactly one field,
the ranges of grid point indices to read from it and the grid hash.
``extract`` returns one result per request, in request order. A result has
one ``RangeView`` per range, with the values and a validity bit per value;
the bit is clear where the field has no data at that point:

.. literalinclude:: ../../rust/crates/gribjump/examples/extract.rs
   :language: rust
   :start-after: // [extract]
   :end-before: // [end extract]
   :dedent:

The hash is the ``md5GridSection`` key of the GRIB message. Setting
``GRIBJUMP_IGNORE_GRID=1`` in the environment turns the check off for the
whole process.

``extract_mars`` takes one request with several values per key, expands it
and extracts the same ranges from every field it matches:

.. literalinclude:: ../../rust/crates/gribjump/examples/extract.rs
   :language: rust
   :start-after: // [mars]
   :end-before: // [end mars]
   :dedent:

To keep values after the result is gone or to move them to another thread,
copy them out:

.. literalinclude:: ../../rust/crates/gribjump/examples/extract.rs
   :language: rust
   :start-after: // [owned]
   :end-before: // [end owned]
   :dedent:

Extracting from Files
---------------------

GribJump reads GRIB files directly, given the byte offset of each message.
``eckit::MessageReader`` finds the offsets:

.. literalinclude:: ../../rust/crates/gribjump/examples/file.rs
   :language: rust
   :start-after: // [offsets]
   :end-before: // [end offsets]
   :dedent:

``extract_from_file`` does not check the grid. ``extract_from_paths`` takes
one ``PathExtractionRequest`` per message, each with its own path, offset
and hash, the form an FDB listing produces:

.. literalinclude:: ../../rust/crates/gribjump/examples/file.rs
   :language: rust
   :start-after: // [file]
   :end-before: // [end paths]
   :dedent:

Scanning
--------

Extraction from an FDB is fast only where a GribJump index exists next to
the data, written by the FDB plugin at archive time or built afterwards by
scanning. ``scan_requests`` scans the fields an FDB request matches, or
with its flag set the whole files holding them; ``scan_paths`` scans GRIB
files. Both write a ``.gribjump`` file next to each ``.data`` or GRIB file
and return the number of messages scanned. ``axes`` lists the values
present for each key of the fields a request matches, down to the index
depth given:

.. literalinclude:: ../../rust/crates/gribjump/examples/index.rs
   :language: rust
   :start-after: // [scan]
   :end-before: // [end axes]
   :dedent:

Threads
-------

``GribJump`` is ``Send`` and ``Sync``; a clone is a second reference to the
same handle:

.. literalinclude:: ../../rust/crates/gribjump/examples/threads.rs
   :language: rust
   :start-after: // [threads]
   :end-before: // [end threads]
   :dedent:

``ExtractionIterator`` and ``ExtractionResult`` are ``Send`` but not
``Sync``. ``RangeView``, ``RangeResult`` and the request structs are both.

Errors
------

An empty range is ``InvalidArgument``. Everything the library rejects comes
back as ``Error::Eckit``; a grid hash that does not match arrives as
``eckit::Error::SeriousBug``:

.. literalinclude:: ../../rust/crates/gribjump/examples/errors.rs
   :language: rust
   :start-after: // [errors]
   :end-before: // [end errors]
   :dedent:
