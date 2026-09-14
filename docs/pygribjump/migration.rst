.. _PyGribJump_Migration:

Migrating from the cffi interface
=================================

Up to and including GribJump 0.13, ``pygribjump`` was implemented with `cffi
<https://cffi.readthedocs.io>`__ on top of the :doc:`GribJump C API
</gribjump/c_api>`. From GribJump 0.14 onwards it is implemented with `pybind11
<https://pybind11.readthedocs.io>`__ directly on top of the GribJump C++ API.

The legacy cffi interface still ships, but it is deprecated and emits a
``FutureWarning`` on import. It will be removed in a future release. Please
migrate.

.. note::

   The package name, the import name and the class names are unchanged, and for
   typical use no code changes are required at all:

   .. code-block:: python

      import pygribjump

      gribjump = pygribjump.GribJump()
      for result in gribjump.extract([(request, ranges)], ctx={"source": "my-app"}):
          values = result.values

What is unchanged
-----------------

* ``GribJump()`` and its methods ``extract``, ``extract_single``,
  ``extract_from_paths``, ``extract_from_mask``, ``extract_from_indices``,
  ``extract_from_ranges`` and ``axes``.
* The ``ExtractionRequest`` and ``PathExtractionRequest`` types, including the
  ``from_mask`` and ``from_indices`` constructors and the ``shape``, ``ranges``
  and ``indices()`` accessors.
* Iterating the returned ``ExtractionIterator``, and the result accessors
  ``values``, ``masks``, ``values_flat``, ``masks_flat``,
  ``compute_bool_masks()``, ``copy_values()`` and ``copy_masks()``.
* ``dump_values()`` on the iterator, though it now returns the arrays owned by the
  results rather than copies of them (see below).
* The helper functions ``version()``, ``library_version()`` and
  ``dic_to_request()``.
* The log context (``ctx=...``) semantics: user-supplied entries take
  precedence over the defaults filled in by ``pygribjump``.
* Values and bitmasks are still ``numpy`` arrays, and the per-range arrays are
  still views into the flat arrays of the same result.

What changed
------------

Keyword arguments are now snake_case
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Deprecated
     - Use instead
     - Affected callables
   * - ``gridHash``
     - ``grid_hash``
     - ``ExtractionRequest``, ``ExtractionRequest.from_mask``,
       ``ExtractionRequest.from_indices``, ``PathExtractionRequest``,
       ``GribJump.extract_single``, ``GribJump.extract_from_mask``,
       ``GribJump.extract_from_indices``, ``GribJump.extract_from_ranges``
   * - ``req``
     - ``request``
     - ``ExtractionRequest``, ``ExtractionRequest.from_mask``,
       ``ExtractionRequest.from_indices``, ``GribJump.axes``
   * - ``polyrequest``
     - ``requests``
     - ``GribJump.extract``

The old names are still accepted and raise a ``DeprecationWarning``; passing
both spellings raises a ``TypeError``. Positional arguments are unaffected.

.. code-block:: python

   # before
   request = pygribjump.ExtractionRequest(req=my_request, ranges=my_ranges, gridHash=my_hash)

   # now
   request = pygribjump.ExtractionRequest(request=my_request, ranges=my_ranges, grid_hash=my_hash)

Exceptions
^^^^^^^^^^

Errors raised by the library are still ``pygribjump.GribJumpException``. The
exception is now created by the bindings and derives from ``RuntimeError``, so
both of these keep working:

.. code-block:: python

   try:
       ...
   except pygribjump.GribJumpException:
       ...

   except RuntimeError:  # also catches GribJumpException
       ...

``dump_legacy()`` has been removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ExtractionIterator.dump_legacy()`` reproduced the return value of ``extract()``
as it was before GribJump 0.11, when a single request could match several fields
and extraction returned a nested ``result[request][field][range]`` list. Since
extraction yields one result per field, the field dimension of that layout has
been a hardcoded single element ever since, and the shape is only faithful for
``extract()`` (whose requests must have cardinality 1), not for
``extract_single()``. It has therefore not been ported to the pybind11
interface.

Iterate the results, or use ``dump_values()``:

.. code-block:: python

   # before
   legacy = gribjump.extract(polyrequest).dump_legacy()
   values = legacy[i][0][k][0]   # ith field, kth range
   mask = legacy[i][0][k][1]

   # now
   results = list(gribjump.extract(requests))
   values = results[i].values[k]
   mask = results[i].masks[k]

   # or, for the values of every field
   values_per_field = gribjump.extract(requests).dump_values()

If you cannot migrate yet, ``dump_legacy()`` is still available in the legacy
cffi interface for as long as that ships.

The polyrequest tuple syntax is deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``extract()`` still accepts a list of ``(request, ranges)`` or
``(request, ranges, grid_hash)`` tuples, but it now raises a
``DeprecationWarning``. Build ``ExtractionRequest`` objects instead — they carry
the same information explicitly, and are what the other ``extract_*`` methods
build internally:

.. code-block:: python

   # deprecated
   polyrequest = [(request, ranges) for request, ranges in zip(requests, all_ranges)]
   iterator = gribjump.extract(polyrequest)

   # now
   iterator = gribjump.extract(
       [pygribjump.ExtractionRequest(request, ranges)
        for request, ranges in zip(requests, all_ranges)]
   )

   # or, when every request uses the same ranges
   iterator = gribjump.extract_from_ranges(requests, ranges)

``rangestr_to_list()`` and ``list_to_rangestr()`` have been removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These converted between ``[(0, 6), (7, 12)]`` and the string ``"0-6,7-12"``. That
format is not produced or consumed anywhere in GribJump — not by the C++ API, the
C API, the command line tools or the bindings — so the helpers have not been
ported. If you rely on the format, it is a one-liner:

.. code-block:: python

   ranges = [tuple(map(int, r.split("-"))) for r in rangestr.split(",")]
   rangestr = ",".join("-".join(map(str, r)) for r in ranges)

``dump_values()`` no longer copies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ExtractionIterator.dump_values()`` used to copy every array, because the cffi
arrays were views into a buffer owned by the result, which died with it. The
arrays are now owned by Python and stay valid once the iterator is exhausted, so
the copy has been dropped and ``dump_values()`` uses half the memory. Use
``copy_values()`` on an individual result if you need arrays which are
independent of the result's flat buffer.

The cffi internals are gone
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``ctype`` properties of ``GribJump``, ``ExtractionRequest`` and
``PathExtractionRequest``, as well as the module level ``ffi``, ``lib``,
``PatchedLib`` and ``CData`` names, no longer exist, and the ``gribjump_c.h``
header is no longer shipped inside the Python package. ``cffi`` and
``packaging`` are no longer dependencies.

Code that reached into these internals was tied to the C API; use the
:doc:`C API </gribjump/c_api>` directly if you need that level of control.

The helper iterator classes ``ExtractionIteratorFromPath`` and
``ExtractionSingleIterator`` have been folded into ``ExtractionIterator``,
which — like ``ExtractionResult`` — is now only created by the library, not by
user code.

Moved helper functions
^^^^^^^^^^^^^^^^^^^^^^

* ``multivalued_dic_to_request()`` has been removed. ``dic_to_request()`` now
  handles multiple values and numbers as well:

  .. code-block:: python

     pygribjump.dic_to_request({"class": "od", "step": [1, 2, 3]})
     # 'class=od,step=1/2/3'

* ``default_context()`` and ``merge_default_context()`` are now internal
  (``pygribjump._internal.ContextMapper``). Pass ``ctx={...}`` to the extraction
  methods as before; the defaults are merged in for you.

Version reporting
^^^^^^^^^^^^^^^^^

``pygribjump.__version__`` and ``version()`` report the GribJump version the
bindings were compiled against, and ``library_version()`` reports the version of
the shared library actually loaded at runtime. A mismatch between the two now
produces a ``UserWarning`` instead of failing hard.

New in the pybind11 interface
-----------------------------

* Requests may be given as a ``dict`` **or** as a MARS request ``str``.
* ``GribJump`` supports the context manager protocol, and gains ``scan()`` and
  ``scan_request()`` to populate the GribJump index for files or MARS requests.
* ``ExtractionResult.shape`` reports the number of values per range, and the
  request and result types have useful ``__repr__`` implementations.
* ``python -m pygribjump --print-home`` / ``--print-home-deps`` show which
  shared libraries were picked up at runtime.
* The interface is fully type annotated.
* The GIL is released during extraction, so extractions from several Python
  threads run concurrently.

Silencing the legacy warning
----------------------------

While migrating you can silence the ``FutureWarning`` emitted by the legacy
interface, either through the standard :py:mod:`warnings` filters or by setting

.. code-block:: bash

   export PYGRIBJUMP_SUPPRESS_LEGACY_WARNING=1
