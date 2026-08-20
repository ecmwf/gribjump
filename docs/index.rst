.. GribJump documentation master file
    :author: ECMWF

GribJump documentation
======================

.. warning::
   These documentation pages are a work in progress.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :hidden:

   gribjump/index
   pygribjump/index
   indices

:ref:`GribJump <GribJump_Introduction>` is part of `ECMWF
<https://www.ecmwf.int>`__'s high-performance data infrastructure. It provides
fast extraction of subsets of GRIB data directly from the `FDB
<https://github.com/ecmwf/fdb>`__ object store, without the need to decode
entire fields. GribJump indexes the compressed GRIB data so that individual
values (or ranges of values) can be read and decoded on demand.

:ref:`pygribjump <PyGribJump_Introduction>` is the Python interface to GribJump,
providing a thin, idiomatic Python layer over the GribJump client library
installed on your system.
