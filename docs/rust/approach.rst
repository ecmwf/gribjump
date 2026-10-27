Approach
========

The crate follows the pattern of the other ECMWF Rust interfaces, documented
in the eckit repository under ``docs/content/rust``: a ``-sys`` crate that
owns the C++ boundary and a safe crate on top, with errors, logging, requests
and data handles coming from the eckit and metkit crates rather than being
reinvented. What follows is where GribJump departs from that pattern, and why.

Results are views, not copies
-----------------------------

A C++ ``ExtractionResult`` holds a vector of value vectors and a vector of
``std::bitset<64>`` vectors, one pair per range. GribJump exists to move
less data, so the crate does not copy that out. ``ExtractionResult`` keeps
the C++ object, and ``range(i)`` or ``iter()`` hand out a ``RangeView``: two
borrowed slices, ``&[f64]`` for the values and ``&[u64]`` for the mask words,
pointing into the C++ memory. The borrow is tied to the result, so the
compiler stops a view from outliving the data it reads. ``to_owned`` and
``to_owned_ranges`` copy when a program needs the values past the result's
life, for example to send them to another thread.

The mask words are built from the bitsets on first access and cached inside
the C++ object. That cache is why ``ExtractionResult`` is ``Send`` but not
``Sync``: two threads reading one result would race on it.

One handle, cloned
------------------

``GribJump`` holds the C++ object behind an ``Arc<Mutex>``, and ``Clone``
hands out another reference to the same one. Each extraction takes the lock
only to start; the iterator it returns does not hold it, because the C++
``GribJump::extract`` materialises every result before returning. The
iterator keeps a reference to the handle so that the C++ object outlives the
results it produced.

The first ``GribJump`` created in a process fixes the process-wide settings,
thread counts among them, from the environment and the configuration file.
Later handles share them.

Requests cross by value
-----------------------

``ExtractionRequest``, ``PathExtractionRequest`` and ``FileExtraction`` are
plain Rust structs. The bridge copies them into their C++ counterparts on
each call, so a request can be built, cloned and reused freely on the Rust
side. ``Range`` is start-inclusive, end-exclusive, like ``std::ops::Range``,
from which it converts; ``Range::new`` rejects an empty range, which the C++
type would accept and later fail on.

Errors
------

``gribjump-sys`` declares no exception classes of its own. Its handler is
built from the headers that ``eckit-sys``, ``metkit-sys`` and ``fdb-sys``
publish, so an eckit exception thrown inside GribJump keeps its class. The
library's own ``GribJumpException``, ``DataNotFoundException`` and
``JumpInfoExtractionDisabled`` are eckit exceptions without a variant of their
own and arrive as ``eckit::Error::Other`` with their message. The crate adds
``InvalidArgument``, for a range it rejects before any C++ runs, and
``IteratorExhausted``, for ``next_result`` on a finished iterator.

Downstream
----------

``gribjump-sys`` exports ``DEP_GRIBJUMP_SYS_ROOT`` and
``DEP_GRIBJUMP_SYS_INCLUDE`` to the build scripts of crates that depend on it,
and in ``system`` builds the library directories of GribJump and each of its
dependencies, for their rpaths. The bridge is annotated with `bindman
<https://github.com/ecmwf/bindman>`__ against ``gribjump/GribJump.h``, so a
method added to the C++ class is reported when the crate builds.
