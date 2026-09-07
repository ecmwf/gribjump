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
* ``dump_values()`` and ``dump_legacy()`` on the iterator.
* The helper functions ``version()``, ``library_version()``,
  ``dic_to_request()``, ``rangestr_to_list()`` and ``list_to_rangestr()``.
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
