# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Tests for the compatibility with the cffi based pygribjump (<= 0.13).
"""

import os
import pathlib
import warnings

import numpy as np
import pytest
from helpers import BASE_REQUEST, CONTEXT, SYNTHETIC_DATA

import pygribjump
from pygribjump import ExtractionRequest, GribJump, GribJumpException, PathExtractionRequest


def test_gribjump_exception_derives_from_runtime_error() -> None:
    assert issubclass(GribJumpException, RuntimeError)
    assert GribJumpException.__module__ == "pygribjump_bindings.pygribjump_bindings"


def test_library_errors_raise_gribjump_exception(tmp_path: pathlib.Path) -> None:
    gribjump = GribJump()
    request = PathExtractionRequest(str(tmp_path / "no_such_file.grib"), "file", 0, "", 0, [(0, 1)])

    with pytest.raises(GribJumpException, match="no_such_file.grib"):
        list(gribjump.extract_from_paths([request]))

    # ... and are still caught by code written against the cffi interface
    with pytest.raises(RuntimeError):
        list(gribjump.extract_from_paths([request]))


def test_deprecated_grid_hash_alias() -> None:
    with pytest.deprecated_call(match="gridHash"):
        request = ExtractionRequest(BASE_REQUEST, [(0, 1)], gridHash="somehash")

    assert request.ranges == [(0, 1)]

    with pytest.deprecated_call(match="gridHash"):
        PathExtractionRequest("/some/path.grib", "file", 0, "", 0, [(0, 1)], gridHash="somehash")


def test_deprecated_req_alias() -> None:
    with pytest.deprecated_call(match="req"):
        request = ExtractionRequest(req=BASE_REQUEST, ranges=[(0, 1)])

    assert "param=151130" in repr(request)

    with pytest.deprecated_call(match="req"):
        ExtractionRequest.from_indices(req=BASE_REQUEST, points=[1, 2])

    with pytest.deprecated_call(match="req"):
        ExtractionRequest.from_mask(req=BASE_REQUEST, mask=np.array([True, False]))


def test_deprecated_polyrequest_alias() -> None:
    gribjump = GribJump()

    # The deprecation is reported before the (empty) request is rejected
    with pytest.deprecated_call(match="polyrequest"), pytest.raises(ValueError):
        gribjump.extract(polyrequest=[], ctx=CONTEXT)


def test_deprecated_alias_conflicts_with_current_name() -> None:
    with pytest.raises(TypeError, match="deprecated alias"):
        ExtractionRequest(request=BASE_REQUEST, req=BASE_REQUEST, ranges=[(0, 1)])


def test_results_share_memory_between_accessors(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()
    request = PathExtractionRequest(str(grib_file), "file", 0, "", 0, [(0, 4), (10, 12)])

    result = next(iter(gribjump.extract_from_paths([request], ctx=CONTEXT)))

    # Repeated access returns the same arrays, as it did with the cffi views
    assert result.values[0] is result.values[0]
    assert result.masks[0] is result.masks[0]

    # ... and the per-range arrays are views into the flat arrays
    result.values[0][0] = 999.0
    assert result.values_flat[0] == 999.0
    assert result.values[0][0] == 999.0

    # ... while the copies are independent
    copied = result.copy_values()
    copied[0][0] = -1.0
    assert result.values[0][0] == 999.0


def test_cffi_internals_are_gone() -> None:
    for name in ["ffi", "lib", "PatchedLib", "CData", "multivalued_dic_to_request"]:
        assert not hasattr(pygribjump, name)

    assert not hasattr(GribJump, "ctype")
    assert not hasattr(ExtractionRequest, "ctype")


def test_range_string_helpers_are_not_ported() -> None:
    """
    The "0-6,7-12" range string format is not used anywhere in gribjump.
    """
    for name in ["rangestr_to_list", "list_to_rangestr"]:
        assert not hasattr(pygribjump, name)
        assert name not in pygribjump.__all__


def test_polyrequest_tuples_are_deprecated() -> None:
    """
    Passing (request, ranges) tuples to extract() still works, but warns.

    The extraction itself needs an FDB, so only the call is exercised here.
    """
    gribjump = GribJump()

    def extract(requests) -> list[warnings.WarningMessage]:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                list(gribjump.extract(requests, ctx=CONTEXT))
            except Exception:  # no FDB in this test setup
                pass
        return [w for w in caught if issubclass(w.category, DeprecationWarning)]

    deprecations = extract([(BASE_REQUEST, [(0, 4)])])
    assert len(deprecations) == 1
    assert "tuples to extract() is deprecated" in str(deprecations[0].message)
    # The warning must point at the caller, not at pygribjump's own frames
    assert os.path.realpath(deprecations[0].filename) == os.path.realpath(__file__), (
        f"warning was attributed to {deprecations[0].filename}, not to the caller"
    )

    # Building the requests explicitly does not warn
    assert extract([ExtractionRequest(BASE_REQUEST, [(0, 4)])]) == []


def test_dump_legacy_is_not_ported(grib_file: pathlib.Path) -> None:
    """
    The pre-0.11 nested list layout is deliberately not part of the new interface.

    Iterate the results, or use `dump_values()`, instead.
    """
    gribjump = GribJump()
    request = PathExtractionRequest(str(grib_file), "file", 0, "", 0, [(0, 6)])

    iterator = gribjump.extract_from_paths([request], ctx=CONTEXT)

    assert not hasattr(iterator, "dump_legacy")

    dumped = iterator.dump_values()
    assert len(dumped) == 1
    assert np.array_equal(dumped[0][0], SYNTHETIC_DATA[0:6], equal_nan=True)


def test_dic_to_request_supersedes_multivalued_dic_to_request() -> None:
    assert pygribjump.dic_to_request({"class": "od", "step": [1, 2, 3]}) == "class=od,step=1/2/3"
