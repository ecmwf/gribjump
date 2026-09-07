# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
API tests extracting from GRIB files at a known location.

These do not require an FDB: the fields are addressed by (path, offset) directly.
"""

import pathlib

import numpy as np
import pytest
from helpers import (
    CONTEXT,
    SYNTHETIC_DATA,
    compare_synthetic_data,
    expected_values,
    validate_masks,
)

from pygribjump import (
    ExtractionIterator,
    ExtractionResult,
    GribJump,
    GribJumpException,
    PathExtractionRequest,
)


def path_request(grib_file: pathlib.Path, ranges) -> PathExtractionRequest:
    return PathExtractionRequest(str(grib_file), "file", 0, "", 0, ranges)


def test_extract_from_paths(grib_file: pathlib.Path) -> None:
    ranges = [(0, 49), (49, 50), (50, 100)]

    gribjump = GribJump()
    iterator = gribjump.extract_from_paths([path_request(grib_file, ranges)], ctx=CONTEXT)

    assert isinstance(iterator, ExtractionIterator)

    results = list(iterator)
    assert len(results) == 1

    result = results[0]
    assert isinstance(result, ExtractionResult)
    assert result.shape == [49, 1, 50]

    compare_synthetic_data(result.values, expected_values(ranges))
    compare_synthetic_data([result.values_flat], [SYNTHETIC_DATA])
    validate_masks(result)


def test_extract_from_paths_multiple_requests(grib_file: pathlib.Path, tmp_path: pathlib.Path) -> None:
    ranges = [
        [(0, 10)],
        [(10, 20), (90, 100)],
        [(0, 1), (1, 2), (92, 93)],
    ]

    message = grib_file.read_bytes()
    multi_field_file = tmp_path / "multi.grib"
    multi_field_file.write_bytes(message * len(ranges))
    offsets = [i * len(message) for i in range(len(ranges))]

    gribjump = GribJump()
    requests = [
        PathExtractionRequest(str(multi_field_file), "file", offset, "", 0, r)
        for offset, r in zip(offsets, ranges)
    ]

    results = list(gribjump.extract_from_paths(requests, ctx=CONTEXT))
    assert len(results) == len(ranges)

    for result, request_ranges in zip(results, ranges):
        compare_synthetic_data(result.values, expected_values(request_ranges))
        validate_masks(result)


def test_extract_from_paths_values_are_numpy_arrays(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()

    result = next(iter(gribjump.extract_from_paths([path_request(grib_file, [(0, 10)])], ctx=CONTEXT)))

    assert isinstance(result.values_flat, np.ndarray)
    assert result.values_flat.dtype == np.float64
    assert isinstance(result.masks_flat, np.ndarray)
    assert result.masks_flat.dtype == np.uint64

    for values in result.values:
        assert isinstance(values, np.ndarray)

    copied = result.copy_values()
    del result
    compare_synthetic_data(copied, expected_values([(0, 10)]))


def test_extract_from_paths_dump_values(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()

    dumped = gribjump.extract_from_paths([path_request(grib_file, [(0, 6)])], ctx=CONTEXT).dump_values()

    assert len(dumped) == 1
    compare_synthetic_data(dumped[0], expected_values([(0, 6)]))


def test_extract_from_paths_dump_legacy(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()

    dumped = gribjump.extract_from_paths([path_request(grib_file, [(0, 6)])], ctx=CONTEXT).dump_legacy()

    assert np.array_equal(dumped[0][0][0][0], SYNTHETIC_DATA[0:6], equal_nan=True)


def test_extract_from_paths_without_context(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()

    result = next(iter(gribjump.extract_from_paths([path_request(grib_file, [(3, 5)])])))

    compare_synthetic_data(result.values, expected_values([(3, 5)]))


def test_extract_from_paths_missing_file_raises(tmp_path: pathlib.Path) -> None:
    gribjump = GribJump()

    with pytest.raises(GribJumpException):
        list(gribjump.extract_from_paths([path_request(tmp_path / "no_such_file.grib", [(0, 1)])]))
