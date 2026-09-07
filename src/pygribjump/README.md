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

## Documentation

For implementation details and tooling, see the [GribJump project pages](https://github.com/ecmwf/gribjump).

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ecmwf/gribjump/blob/develop/LICENSE)
