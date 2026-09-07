try:
    from . import pygribjump_bindings as pygribjump_bindings
except ImportError as exc:
    raise ImportError(
        "pygribjump's compiled pybind11 extension (pygribjump_bindings) failed to import. "
        "This usually means it wasn't built, or was built for a "
        "different Python version/platform."
    ) from exc
