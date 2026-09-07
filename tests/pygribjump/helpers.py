# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Data and helpers shared by the pygribjump (pybind11) API tests.
"""

import numpy as np

# The synthetic data encoded in `data/synth11.grib`: the values 0..99 with an
# increasing number of missing values interleaved.
SYNTHETIC_DATA = [
    0.0, np.nan, np.nan, 3.0, 4.0, np.nan, np.nan, np.nan, 8.0, 9.0,
    10.0, np.nan, np.nan, np.nan, np.nan, 15.0, 16.0, 17.0, 18.0, np.nan,
    np.nan, np.nan, np.nan, np.nan, 24.0, 25.0, 26.0, 27.0, 28.0, np.nan,
    np.nan, np.nan, np.nan, np.nan, np.nan, 35.0, 36.0, 37.0, 38.0, 39.0,
    40.0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 48.0, 49.0,
    50.0, 51.0, 52.0, 53.0, 54.0, np.nan, np.nan, np.nan, np.nan, np.nan,
    np.nan, np.nan, np.nan, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0,
    70.0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
    80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, np.nan,
    np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 99.0,
]

# For completeness: the log context handed over to the library.
CONTEXT = {
    "source": "pytest",
}

# A MARS selection matching the single field stored in `data/synth11.grib`.
BASE_REQUEST = {
    "domain": "g",
    "levtype": "sfc",
    "date": "20230508",
    "time": "1200",
    "step": "0",
    "param": "151130",
    "class": "od",
    "type": "fc",
    "stream": "oper",
    "expver": "0001",
}


def expected_values(ranges) -> list:
    """
    The values of the synthetic data which correspond to the given ranges.
    """
    return [SYNTHETIC_DATA[lo:hi] for lo, hi in ranges]


def compare_synthetic_data(values, expected) -> None:
    """
    Compare the extracted values with the expected values, treating NaN as equal.
    """
    assert len(values) == len(expected)
    for actual, wanted in zip(values, expected):
        assert np.array_equal(actual, wanted, equal_nan=True)


def validate_masks(result) -> None:
    """
    For a given bitmask, check that the masked-out values are NaN, and vice versa.
    """
    values = result.values
    boolmask = result.compute_bool_masks()

    assert len(values) == len(boolmask)

    for vals, mask in zip(values, boolmask):
        assert len(vals) == len(mask)
        for val, is_set in zip(vals, mask):
            if is_set:
                assert not np.isnan(val)
            else:
                assert np.isnan(val)

    # Check that the flattened mask matches the concatenated per-range masks
    assert np.array_equal(np.concatenate(result.masks), result.masks_flat)
