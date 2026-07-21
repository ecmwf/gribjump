.. _GribJump_CPP_API:

C++ API
=======

GribJump is primarily a C++ library. This page documents the main public C++
interfaces.

The GribJump class
------------------

:cpp:class:`gribjump::GribJump` is the high-level entry point to the library. It
is the object you construct to scan data, extract subsets of GRIB fields and
query their axes.

.. doxygenclass:: gribjump::GribJump
   :members:
   :undoc-members:

Extraction results
------------------

Extraction calls return a :cpp:class:`gribjump::ExtractionIterator`, a simple
iterator over the :cpp:class:`gribjump::ExtractionResult` objects produced by the
request.

.. doxygenclass:: gribjump::ExtractionIterator
   :members:
   :undoc-members:

.. doxygenclass:: gribjump::IResultSource
   :members:
   :undoc-members:

.. doxygenclass:: gribjump::VectorSource
   :members:
   :undoc-members:
