# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Tests for the request types of the pygribjump API. These do not require any data.
"""

import numpy as np
import pytest
from helpers import BASE_REQUEST

from pygribjump import ExtractionRequest, PathExtractionRequest


@pytest.mark.parametrize(
    "ranges,indices",
    [
        ([(1, 2)], [1]),
        # Overlapping ranges should fail during retrieval, kept for documentation
        ([(1, 2), (1, 3)], [1, 1, 2]),
        ([(0, 5), (11, 12), (12, 13)], [0, 1, 2, 3, 4, 11, 12]),
    ],
)
def test_extraction_request(ranges, indices) -> None:
    request = ExtractionRequest({"step": "0"}, ranges)

    assert request.ranges == ranges
    assert request.shape == [hi - lo for lo, hi in ranges]
    np.testing.assert_array_equal(request.indices(), np.array(indices, dtype=int))


def test_extraction_request_errors_for_invalid_ranges() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ExtractionRequest({"step": "0"}, [])

    with pytest.raises(ValueError, match="invalid range"):
        ExtractionRequest({"step": "0"}, [(2, 2)])

    with pytest.raises(ValueError, match="invalid range"):
        ExtractionRequest({"step": "0"}, [(1, 2), (4, 2)])


def test_extraction_request_from_mask() -> None:
    mask = np.array([1, 1, 0, 0, 1, 0, 1, 1, 1, 0], dtype=bool)

    request = ExtractionRequest(BASE_REQUEST, [(0, 2), (4, 5), (6, 9)])
    from_mask = ExtractionRequest.from_mask(BASE_REQUEST, mask)

    assert from_mask.ranges == request.ranges
    np.testing.assert_array_equal(from_mask.indices(), np.flatnonzero(mask))


def test_extraction_request_from_mask_errors_for_empty_mask() -> None:
    with pytest.raises(ValueError, match="at least one True"):
        ExtractionRequest.from_mask(BASE_REQUEST, np.zeros(10, dtype=bool))


def test_extraction_request_from_indices() -> None:
    points = [10, 20, 30, 40, 50]

    request = ExtractionRequest.from_indices(BASE_REQUEST, points)

    assert request.ranges == [(p, p + 1) for p in points]
    assert request.shape == [1] * len(points)
    np.testing.assert_array_equal(request.indices(), np.array(points, dtype=int))


def test_extraction_request_from_indices_rejects_non_integer_points() -> None:
    with pytest.raises(TypeError, match="integer bounds"):
        ExtractionRequest.from_indices(BASE_REQUEST, [2.7, 5.2])

    with pytest.raises(ValueError, match="non-negative"):
        ExtractionRequest.from_indices(BASE_REQUEST, [-5, -3])


def test_extraction_request_from_indices_accepts_numpy_integers() -> None:
    points = np.array([10, 20, 30], dtype=np.int64)

    request = ExtractionRequest.from_indices(BASE_REQUEST, points)

    assert request.ranges == [(10, 11), (20, 21), (30, 31)]
    assert request.indices().dtype == np.int64


def test_extraction_request_repr_holds_the_request() -> None:
    request = ExtractionRequest({"class": "od", "step": [0, 1]}, [(0, 10)])

    assert "class=od" in repr(request)
    assert "step=0/1" in repr(request)


def test_path_extraction_request() -> None:
    ranges = [(0, 10), (20, 30)]

    request = PathExtractionRequest("/some/path.grib", "file", 1234, "", 0, ranges)

    assert request.ranges == ranges
    assert request.shape == [10, 10]
    assert "/some/path.grib" in repr(request)


def test_path_extraction_request_errors_for_invalid_ranges() -> None:
    with pytest.raises(ValueError, match="at least one"):
        PathExtractionRequest("/some/path.grib", "file", 0, "", 0, [])
