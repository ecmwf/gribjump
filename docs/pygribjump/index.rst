.. _PyGribJump_Introduction:

pygribjump
==========

:Version: |version|

.. warning::
   These documentation pages are a work in progress.

``pygribjump`` is the Python interface to :ref:`GribJump <GribJump_Introduction>`.
It provides a thin, idiomatic Python layer over the GribJump client library
installed on your system, so you can drive GribJump extraction operations
directly from Python scripts and notebooks. Extracted values and bitmasks are
returned as ``numpy`` arrays.

.. note::

   As of GribJump 0.14, ``pygribjump`` is built on `pybind11
   <https://pybind11.readthedocs.io>`__. The previous implementation used
   ``cffi`` and is deprecated; it will be removed in a future release. The
   public API is unchanged for typical use — see the
   :ref:`PyGribJump_Migration` for the details.

Installation
------------

``pygribjump`` is published on PyPI:

.. code-block:: bash

   pip install pygribjump

This pulls in the binary dependencies. Point ``pygribjump`` at the GribJump
installation you want to use (and, for extraction from an FDB, at the FDB):

.. code-block:: bash

   export GRIBJUMP_HOME=<path-to-gribjump-home>
   export FDB_HOME=<path-to-fdb-home>

To check which shared libraries were picked up at runtime:

.. code-block:: bash

   python -m pygribjump --print-home-deps

Getting started
---------------

Extract two ranges of values from every field matching a MARS request:

.. code-block:: python

   import pygribjump

   gribjump = pygribjump.GribJump()

   request = {
       "class": "od",
       "expver": "0001",
       "stream": "oper",
       "date": "20230508",
       "time": "1200",
       "levtype": "sfc",
       "param": "151130",
   }

   for result in gribjump.extract_from_ranges([request], [(0, 10), (20, 30)]):
       print(result.values)       # list of numpy arrays, one per range
       print(result.values_flat)  # all ranges as a single flat array
       print(result.masks)        # bitmask of the missing values, as uint64

Each iteration step yields one :py:class:`~pygribjump.ExtractionResult`, i.e.
one field matched by the request. Ranges are half-open intervals ``[lo, hi)``.

The region to extract can also be given as a boolean mask
(``extract_from_mask``) or as a list of point indices
(``extract_from_indices``), and a single request of arbitrary cardinality can be
extracted with ``extract_single``:

.. code-block:: python

   request["step"] = ["0", "1", "2", "3"]

   for result in gribjump.extract_single(request, [(0, 10)]):
       print(result.values_flat)

To find out which data is available, use ``axes``:

.. code-block:: python

   gribjump.axes({"date": "20230508"}, level=3)

Configuration
-------------

Pass a dictionary to configure a client:

.. code-block:: python

   import pygribjump

   checked = pygribjump.GribJump(config={
       "threads": 4,
       "cache": {"directory": "/existing/cache"},
       "ignoreGridHash": False,
   })
   unchecked = pygribjump.GribJump(config={"ignoreGridHash": True})

``checked`` and ``unchecked`` retain independent grid-validation settings and
share the process cache and worker pool. The configuration is copied at
construction; later edits to the dictionary leave existing clients unchanged.
Clients can also be used as context managers:

.. code-block:: python

   with pygribjump.GribJump(config={"ignoreGridHash": True}) as client:
       # Use client here.
       pass

See :doc:`../gribjump/list_of_configuration_options` for the full list, grouped
into **per-object** and **process-wide** options. Python uses the same names,
defaults and environment/resource precedence as C++.

* ``GribJump()`` and ``GribJump(config=None)`` use file defaults.
* ``GribJump(config={})`` uses built-in per-object defaults. Omitted process
  settings retain the established settings, or use file defaults on first use.
* Nested dictionaries represent YAML sections. Dotted keys such as
  ``"cache.size"`` are also accepted. Values can be ``bool``, ``int``, ``float``,
  ``str``, dictionaries, or lists of dictionaries (for example, ``servermap``).
  Dictionary keys must be non-empty strings. Unsupported Python value types
  raise ``TypeError``.

For example, forwarding endpoints can be supplied as:

.. code-block:: python

   client = pygribjump.GribJump(config={
       "forwardExtraction": True,
       "servermap": [
           {"fdb": "store.example:9000", "gribjump": "store.example:9777"},
       ],
   })

Establish process settings at startup, either in the first client constructor
or explicitly with ``configure_process``:

.. code-block:: python

   pygribjump.configure_process({"threads": 4, "cache": {"size": 2048}})
   client = pygribjump.GribJump(config={"allowMissing": True})

The first configuration or process-service use fixes shared settings. Later
matching values are accepted; conflicts raise ``GribJumpException`` identifying
the option. ``configure_process`` applies only process-wide options. Call it
before creating clients, using the FDB plugin, or constructing
``ExtractionRequest`` objects: standalone request construction reads the
process-wide ``requestParsing`` setting and can therefore fix the process
configuration before any client exists.

These configuration APIs are available in the pybind11 implementation.
The deprecated cffi API is unchanged.

Error handling
--------------

Errors reported by the GribJump library are raised as
``pygribjump.GribJumpException``. It derives from :py:exc:`RuntimeError`, so
catching either works:

.. code-block:: python

   try:
       results = list(gribjump.extract_from_ranges([request], [(0, 10)]))
   except pygribjump.GribJumpException as error:
       print(f"extraction failed: {error}")

Threading
---------

The bindings release the GIL for the duration of an extraction, so extractions
issued from several Python threads run concurrently in the GribJump library.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   migration
   /autoapi/pygribjump/index

API Reference
-------------

The :doc:`API reference </autoapi/pygribjump/index>` is generated automatically
from the ``pygribjump`` source.
