.. _GribJump_Introduction:

GribJump
========

:Version: |version|

GribJump is part of `ECMWF <https://www.ecmwf.int>`__'s high-performance data
infrastructure. It provides fast extraction of subsets of GRIB data directly
from the `FDB <https://github.com/ecmwf/fdb>`__ object store, without the need
to decode entire fields. GribJump indexes the compressed GRIB data so that
individual values (or ranges of values) can be read and decoded on demand.

This section documents the main C++ library: how to set it up, configure it,
and the concepts behind its inner machinery.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   setup_gribjump_on_fdb
   gribjump_index
   list_of_configuration_options
   gribjump_with_remotefdb
   round-robin-scheduling
   details
   api
