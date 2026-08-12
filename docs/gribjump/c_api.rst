.. _GribJump_C_API:

C API
=====

GribJump also exposes a plain C interface, declared in ``gribjump_c.h``. It is a
thin wrapper over the C++ :cpp:class:`gribjump::GribJump` class and is the
foundation the Python bindings (:ref:`pygribjump <PyGribJump_Introduction>`)
build upon.

All functions return a :c:enum:`gribjump_error_t` (with iterator stepping
reported via :c:enum:`gribjump_iterator_status_t`); on failure, a human-readable
message can be retrieved with :c:func:`gribjump_error_string`. Opaque handle,
request, result and iterator types are created and released through their
matching ``_new_``/``_delete_`` functions.

.. doxygenfile:: gribjump_c.h
   :project: GribJump
