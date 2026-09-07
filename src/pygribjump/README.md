[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity)

> \[!IMPORTANT\]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).

# PyGribJump

`PyGribJump` is the Python interface to [GribJump](https://github.com/ecmwf/gribjump),
a library for extracting subsets of data from GRIB files, in particular from data
archived in the [FDB](https://github.com/ecmwf/fdb). Instead of decoding whole GRIB
messages, `GribJump` decodes only the ranges of values that were asked for, which makes
point and region extraction from large archives fast.

`PyGribJump` provides a thin, idiomatic Python layer over the `GribJump` client library
installed on your system, so you can drive extraction directly from Python scripts and
notebooks. Values and bitmasks are handed back as `numpy` arrays.

## Installation via PyPI

Install the package from PyPI in your `venv`:

```
uv venv
source .venv/bin/activate
uv pip install pygribjump
```

This will bring in some necessary binary dependencies for you.
Set the `GRIBJUMP_HOME` and `FDB_HOME` environment variables accordingly:

```
export GRIBJUMP_HOME=<path_to_gribjump_home>
export FDB_HOME=<path_to_fdb_home>
```

## Usage

```python
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
    print(result.values)
```

To inspect which libraries have been picked up at runtime:

```
python -m pygribjump --print-home-deps
```

## Migrating from the cffi based pygribjump (<= 0.13)

`PyGribJump` used to be implemented with `cffi`; it is now built on `pybind11`. The
public API is unchanged for typical use: `GribJump()`, `extract`, `extract_single`,
`extract_from_paths`, `extract_from_mask`, `extract_from_indices`,
`extract_from_ranges`, `axes`, the `ExtractionRequest`/`PathExtractionRequest` types
and the `values`/`masks`/`values_flat`/`masks_flat`/`compute_bool_masks` accessors of
the results (including `dump_values` and `dump_legacy`) all keep working as before.

**The full migration guide is published at
<https://sites.ecmwf.int/docs/gribjump/pygribjump/migration.html>** (source:
`docs/pygribjump/migration.rst`). In short:

- **Keyword arguments were renamed to snake_case**: `gridHash` -> `grid_hash`,
  `req` -> `request`, `polyrequest` -> `requests`. The old names are still accepted
  and raise a `DeprecationWarning`; they will be removed in a future release.
- **Errors** raised by the library are `pygribjump.GribJumpException`, as before. The
  exception is now created by the bindings and derives from `RuntimeError`, so both
  `except GribJumpException` and `except RuntimeError` work.
- **cffi internals are gone**: the `ctype` properties, `ffi`, `lib`, `PatchedLib` and
  the bundled `gribjump_c.h` no longer exist, and `cffi` is no longer a dependency.
  The internal iterator classes `ExtractionIteratorFromPath` and
  `ExtractionSingleIterator` have been folded into `ExtractionIterator`.
- **Helper functions moved**: `multivalued_dic_to_request` is superseded by
  `dic_to_request`, which now also handles lists and numbers; `default_context` and
  `merge_default_context` are now internal (`pygribjump._internal.ContextMapper`).
- **`__version__` and `version()`** report the version of the gribjump library the
  bindings were compiled against; `library_version()` reports the version of the
  library actually loaded at runtime. A mismatch produces a `UserWarning` instead of
  the previous hard failure.
- **Requests accept a `str`** as well as a `dict`, and extraction releases the GIL, so
  extractions from several threads now run concurrently.

## Documentation

For implementation details and tooling, see the [GribJump project pages](https://github.com/ecmwf/gribjump).

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ecmwf/gribjump/blob/develop/LICENSE)
