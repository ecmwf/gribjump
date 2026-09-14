# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
API tests extracting from an FDB.

These require pyfdb and a writable FDB. Under ctest they run automatically if
pyfdb is built alongside (see tests/pygribjump/CMakeLists.txt); when running
pytest by hand, set PYGRIBJUMP_TEST_ENABLE_FDB=1 to enable them.
"""

import os
import pathlib
import shutil

import numpy as np
import pytest
from helpers import (
    BASE_REQUEST,
    CONTEXT,
    compare_synthetic_data,
    expected_values,
    validate_masks,
)

from pygribjump import ExtractionRequest, GribJump

# Skip by default (sorry...)
SKIP_FDB = os.getenv("PYGRIBJUMP_TEST_ENABLE_FDB") != "1"

pytestmark = pytest.mark.skipif(SKIP_FDB, reason="FDB tests are skipped")


@pytest.fixture(scope="function")
def read_only_fdb_setup(data_path: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Creates an FDB in this test's temp directory, holding 5 steps of synthetic data.
    """
    import pyfdb
    import yaml

    db_store_path = tmp_path / "db_store"
    db_store_path.mkdir(exist_ok=True)
    schema_path = tmp_path / "schema"
    config = dict(
        type="local",
        engine="toc",
        schema=str(schema_path),
        spaces=[
            dict(
                handler="Default",
                roots=[
                    {"path": str(db_store_path)},
                ],
            )
        ],
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))
    shutil.copy(data_path / "schema", schema_path)
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)

    grib_file = data_path / "synth11.grib"

    request = dict(BASE_REQUEST)

    fdb = pyfdb.FDB()
    for step in range(5):
        request["step"] = str(step)
        fdb.archive(grib_file.read_bytes(), request)
    fdb.flush()

    return tmp_path


def test_extract(read_only_fdb_setup) -> None:
    gribjump = GribJump()

    ranges = [
        [(0, 49), (49, 50), (50, 100)],
        [(0, 100)],
        [(0, 1), (1, 2), (92, 93)],
        [(0, 1)],
    ]

    requests = []
    for step in range(len(ranges)):
        request = dict(BASE_REQUEST)
        request["step"] = str(step)
        requests.append(request)

    # list of tuples api: deprecated (see test_cffi_compat.py), still supported for now
    polyrequest = list(zip(requests, ranges))
    results = list(gribjump.extract(polyrequest, ctx=CONTEXT))
    assert len(results) == len(ranges)
    for result, request_ranges in zip(results, ranges):
        compare_synthetic_data(result.values, expected_values(request_ranges))
        validate_masks(result)

    # list of ExtractionRequest api
    polyrequest = [ExtractionRequest(r, rng) for r, rng in zip(requests, ranges)]
    results = list(gribjump.extract(polyrequest, ctx=CONTEXT))
    assert len(results) == len(ranges)
    for result, request_ranges in zip(results, ranges):
        compare_synthetic_data(result.values, expected_values(request_ranges))


def test_extract_single(read_only_fdb_setup) -> None:
    gribjump = GribJump()

    request = dict(BASE_REQUEST)
    request["step"] = ["0", "1", "2", "3"]

    ranges = [(0, 49), (49, 50), (50, 100)]

    results = list(gribjump.extract_single(request, ranges, ctx=CONTEXT))

    assert len(results) == 4
    for result in results:
        compare_synthetic_data(result.values, expected_values(ranges))
        validate_masks(result)


def test_extract_from_mask(read_only_fdb_setup) -> None:
    from helpers import SYNTHETIC_DATA

    gribjump = GribJump()

    mask = np.where(np.isnan(SYNTHETIC_DATA), 0, 1)
    expected_flat = np.array(SYNTHETIC_DATA)[mask == 1]

    results = list(gribjump.extract_from_mask([dict(BASE_REQUEST)], mask, ctx=CONTEXT))

    assert len(results) == 1
    np.testing.assert_array_equal(results[0].values_flat, expected_flat)


def test_extract_from_indices_matches_ranges(read_only_fdb_setup) -> None:
    from helpers import SYNTHETIC_DATA

    gribjump = GribJump()

    points = [10, 20, 30, 40, 50]
    ranges = [(p, p + 1) for p in points]

    from_indices = next(iter(gribjump.extract_from_indices([dict(BASE_REQUEST)], points, ctx=CONTEXT)))
    from_ranges = next(iter(gribjump.extract_from_ranges([dict(BASE_REQUEST)], ranges, ctx=CONTEXT)))

    expected = [SYNTHETIC_DATA[p] for p in points]
    np.testing.assert_array_equal(from_indices.values_flat, expected)
    np.testing.assert_array_equal(from_ranges.values_flat, expected)


def test_axes(read_only_fdb_setup) -> None:
    gribjump = GribJump()

    request = {"date": BASE_REQUEST["date"]}

    assert len(gribjump.axes(request, level=1, ctx=CONTEXT)) == 6
    assert len(gribjump.axes(request, level=2, ctx=CONTEXT)) == 8

    axes = gribjump.axes(request, level=3, ctx=CONTEXT)
    assert len(axes) == 11
    assert sorted(axes["step"]) == ["0", "1", "2", "3", "4"]
