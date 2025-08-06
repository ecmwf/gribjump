"""
NOTE:

To be able to run the tests pyfdb and pygribjump need to find the native libraries.
If not installed into a default location export the following environent
variables in the shell running pytest:
    GRIBJUMP_HOME = <cmake-build-root-for-gribjump> or <custom-install-location>
    FDB5_DIR = <cmake-build-root-for-fdb> or <custom-install-location>
"""

import os
import pathlib
import shutil

import numpy as np
import pytest
import yaml
import re
import subprocess

# Do not reorder the imports otherwise this will trigger an assertion in eckits
# library registration handling.
from pygribjump import GribJump, ExtractionRequest, ExtractionResult, PathExtractionRequest
import pyfdb

synthetic_data = [
    0., np.nan, np.nan, 3., 4., np.nan, np.nan, np.nan, 8., 9.,
    10., np.nan, np.nan, np.nan, np.nan, 15., 16., 17., 18., np.nan,
    np.nan, np.nan, np.nan, np.nan, 24., 25., 26., 27., 28., np.nan,
    np.nan, np.nan, np.nan, np.nan, np.nan, 35., 36., 37., 38., 39.,
    40., np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 48., 49.,
    50., 51., 52., 53., 54., np.nan, np.nan, np.nan, np.nan, np.nan,
    np.nan, np.nan, np.nan, 63., 64., 65., 66., 67., 68., 69.,
    70., np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
    80., 81., 82., 83., 84., 85., 86., 87., 88., np.nan,
    np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 99.
]

# for completeness
context = {
    "source": "pytest",
}


def compare_synthetic_data(values, expected):
    """
    Compare the synthetic data with the expected data.
    """
    assert len(values) == len(expected)
    for i in range(len(values)):
        assert np.array_equal(values[i], expected[i], equal_nan=True)


def validate_masks(result: ExtractionResult):
    """
    For a given bitmask, check if the values are nan
    """
    values = result.values
    boolmask = result.compute_bool_masks()

    assert len(values) == len(boolmask)

    for i, vals in enumerate(values):
        assert len(vals) == len(boolmask[i])

        for j, val in enumerate(vals):
            if boolmask[i][j] == False:
                assert np.isnan(val)
            else:
                assert not np.isnan(val)

    # Check that the flattened mask's values are as expected
    assert np.array_equal(np.concatenate(result.masks),
                          result.masks_flat, equal_nan=True)


@pytest.fixture(scope="function")
def read_only_fdb_setup(data_path: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    """
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

    request = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
    }

    fdb = pyfdb.FDB()
    for i in range(5):
        request["step"] = str(i)
        fdb.archive(grib_file.read_bytes(), key=request)
    fdb.flush()

    return tmp_path


def test_extract_dump_legacy(read_only_fdb_setup) -> None:
    gribjump = GribJump()
    reqstrs = [
        {
            "domain": "g",
            "levtype": "sfc",
            "date": "20230508",
            "time": "1200",
            "step": "1",
            "param": "151130",
            "class": "od",
            "type": "fc",
            "stream": "oper",
            "expver": "0001",
        },
    ]
    ranges = [
        [(0, 6)],
    ]

    polyrequest = [(a, b) for a, b in zip(reqstrs, ranges)]

    expected = synthetic_data[0:6]
    actual = gribjump.extract(
        polyrequest, ctx=context).dump_legacy()  # old api output
    assert np.array_equal(expected, actual[0][0][0][0], equal_nan=True)


def test_extract_iter(read_only_fdb_setup) -> None:
    gribjump = GribJump()

    basereq = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
    }

    # We will do 4 requests at once
    ranges = [
        [(0, 49), (49, 50), (50, 100)],
        [(0, 100)],
        [(0, 1), (1, 2), (92, 93)],
        [(0, 1)],
    ]

    reqstrs = []
    for step in [0, 1, 2, 3]:
        req = basereq.copy()
        req["step"] = str(step)
        reqstrs.append(req)

    # list of tuples api:
    polyrequest = [(a, b) for a, b in zip(reqstrs, ranges)]

    for i, result in enumerate(gribjump.extract(polyrequest, ctx=context)):
        compare_synthetic_data(
            result.values, [synthetic_data[r[0]:r[1]] for r in (ranges[i])])
        validate_masks(result)
    assert i == len(ranges) - 1

    # list of ExtractionRequest api:
    polyrequest = [ExtractionRequest(reqstrs[i], ranges[i])
                   for i in range(len(ranges))]

    for i, result in enumerate(gribjump.extract(polyrequest, ctx=context)):
        compare_synthetic_data(
            result.values, [synthetic_data[r[0]:r[1]] for r in (ranges[i])])
        validate_masks(result)
    assert i == len(ranges) - 1

    # Single request api:
    request = basereq.copy()
    request["step"] = ['0', '1', '2', '3']

    expected = [synthetic_data[r[0]:r[1]] for r in (ranges[0])]
    for i, result in enumerate(gribjump.extract_single(request, ranges[0], ctx=context)):
        compare_synthetic_data(result.values, expected)
        validate_masks(result)

    assert i == 3


def test_extract_from_paths(read_only_fdb_setup) -> None:
    gribjump = GribJump()

    basereq = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
    }

    # We will do 4 requests at once
    ranges = [
        [(0, 49), (49, 50), (50, 100)],
    ]

    reqstrs = []
    req = basereq.copy()
    req["step"] = str(0)
    reqstrs.append(req)

    host = ""
    port = 0

    fdb = pyfdb.FDB()
    result = fdb.list({
        "class": "od", "type": "fc", "stream": "oper",
        "param": "151130", "date": "20230508", "time": "1200", "step": "1"
    })

    filenames = []
    offsets = []
    for r in result:
        filenames.append(r["path"])
        offsets.append(r["offset"])

    scheme = "file"

    polyrequest = [PathExtractionRequest(filenames[i], scheme, offsets[i], host, port, ranges[i])
                   for i in range(len(offsets))]

    for i, result in enumerate(gribjump.extract_from_paths(polyrequest, ctx=context)):
        compare_synthetic_data(
            result.values, [synthetic_data[r[0]:r[1]] for r in (ranges[i])])
        validate_masks(result)
    assert i == len(ranges) - 1


def test_extract_from_mask(read_only_fdb_setup) -> None:

    gribjump = GribJump()

    req = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
        "step": "0"
    }

    mask = np.where(np.isnan(synthetic_data), 0, 1)
    expected_values_flat = np.array(synthetic_data)[mask == 1]  # flat values
    expected_values = np.split(expected_values_flat, [
                               1, 3, 6, 10, 15, 21, 28, 36, 45])  # reshaped

    it = gribjump.extract_from_mask([req], mask, ctx=context)

    i = -1
    for i, result in enumerate(it):
        compare_synthetic_data(result.values, expected_values)
        compare_synthetic_data(result.values_flat, expected_values_flat)
        validate_masks(result)

        for vals in result.values:
            for val in vals:
                assert not np.isnan(val)
    assert i == 0


def test_extract_from_mask_edge_cases(read_only_fdb_setup) -> None:

    gribjump = GribJump()

    req = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
        "step": "0"
    }
    masks = [
        # All 0 is considered an error inside the library
        np.zeros((len(synthetic_data),), dtype=np.int8),
        np.ones((len(synthetic_data),), dtype=np.int8),  # All 1
        np.zeros((len(synthetic_data),), dtype=np.int8),  # 0000...01
        np.ones((len(synthetic_data),), dtype=np.int8),  # 1111...10
    ]
    masks[-2][-1] = 1
    masks[-1][-1] = 0

    for i, mask in enumerate(masks):
        if i == 0:
            with pytest.raises(ValueError):
                gribjump.extract_from_mask([req], mask, ctx=context)
            continue

        expected_values_flat = np.array(synthetic_data)[
            mask == 1]  # flat values
        it = gribjump.extract_from_mask([req], mask, ctx=context)
        j = -1
        for j, result in enumerate(it):
            compare_synthetic_data(result.values_flat, expected_values_flat)
        assert j == 0


def test_extract_from_indices() -> None:

    points = [10, 20, 30, 40, 50]
    ranges = [(10, 11), (20, 21), (30, 31), (40, 41), (50, 51)]

    r = {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": "1",
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "0001",
        "step": "0"
    }

    # Extract with ranges and points, check if the values are the same

    gribjump = GribJump()
    result1 = next(
        iter(gribjump.extract_from_indices([r], points, ctx=context)))
    result2 = next(
        iter(gribjump.extract_from_ranges([r], ranges, ctx=context)))

    compare_synthetic_data(result1.values_flat, [
                           synthetic_data[i] for i in points])
    compare_synthetic_data(result2.values_flat, [
                           synthetic_data[i] for i in points])


def test_axes(read_only_fdb_setup) -> None:
    gribjump = GribJump()
    req = {
        "date": "20230508",
    }
    # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'stream': ['oper'], 'time': ['1200']}
    ax1 = gribjump.axes(req, level=1, ctx=context)
    # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'levtype': ['sfc'], 'stream': ['oper'], 'time': ['1200'], 'type': ['fc']}
    ax2 = gribjump.axes(req, level=2, ctx=context)
    # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'levelist': [''], 'levtype': ['sfc'], 'param': ['151130'], 'step': ['1'], 'stream': ['oper'], 'time': ['1200'], 'type': ['fc']}
    ax3 = gribjump.axes(req, level=3, ctx=context)

    assert len(ax1.keys()) == 6
    assert len(ax2.keys()) == 8
    assert len(ax3.keys()) == 11


@pytest.mark.parametrize("ranges,indices", [
    ([(1, 2)], [1]),
    # Should fail during retrieval, kept for documentation
    ([(1, 2), (1, 3)], [1, 1, 2]),
    ([(0, 5), (11, 12), (12, 13)], [0, 1, 2, 3, 4, 11, 12])
])
def test_extraction_request(ranges, indices):
    request = ExtractionRequest({"a": "b"}, ranges)

    arr_expected_indices = np.array(indices, dtype=int)
    arr_observed_indices = request.indices()

    assert request.ranges == ranges
    np.testing.assert_array_equal(arr_observed_indices, arr_expected_indices)


def test_extraction_request_errors_for_invalid_ranges():
    with pytest.raises(ValueError, match='at least one'):
        ExtractionRequest({"a": "b"}, [])

    with pytest.raises(ValueError, match='invalid range'):
        ExtractionRequest({"a": "b"}, [(2, 2)])

    with pytest.raises(ValueError, match='invalid range'):
        ExtractionRequest({"a": "b"}, [(1, 2), (4, 2)])
