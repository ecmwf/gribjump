# GribJump Examples

This directory contains small examples demonstrating how to use GribJump from Python and C++.

## Prerequisites

- `pygribjump` is installed in your Python environment.
- The GribJump shared library is discoverable at runtime (for example via `GRIBJUMP_HOME`).
- FDB is configured and accessible for the requests used by the examples.

## Required environment variables

Set the following variables before running the Python examples:

- `GRIBJUMP_HOME`: Path to a GribJump build/install prefix containing the library.
- `GRIBJUMP_CONFIG_FILE`: Path to the GribJump YAML configuration file.
- `GRIBJUMP_IGNORE_GRID`: Set to `1` when running examples that should skip grid-hash checks.

Some examples may also require FDB-specific environment variables (for example `FDB_HOME` and `FDB_ENABLE_GRIBJUMP`) depending on your local setup.

## Run the examples

From this directory:

```bash
python3 example_extract.py
python3 example_axes.py
python3 walkthrough.py
```

## What each example demonstrates

- `example_extract.py`: Builds multiple MARS requests and range lists, then extracts value subsets for each request.
- `example_axes.py`: Queries available axes (dimension keys and values) for a partial MARS request.
- `walkthrough.py`: End-to-end local walkthrough that prepares synthetic data in a temporary FDB setup and performs extraction.

## Placeholder paths

The scripts use placeholder paths such as `/path/to/...`. Replace these with real paths in your environment before running the examples.
