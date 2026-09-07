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

import pathlib

import numpy as np
import pytest
from helpers import BASE_REQUEST, CONTEXT

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

    with pytest.deprecated_call(match="polyrequest"), pytest.raises(ValueError):
        gribjump.extract(polyrequest=[], ctx=CONTEXT)


def test_deprecated_alias_conflicts_with_current_name() -> None:
    with pytest.raises(TypeError, match="deprecated alias"):
        ExtractionRequest(request=BASE_REQUEST, req=BASE_REQUEST, ranges=[(0, 1)])


def test_results_share_memory_between_accessors(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()
    request = PathExtractionRequest(str(grib_file), "file", 0, "", 0, [(0, 4), (10, 12)])

    result = next(iter(gribjump.extract_from_paths([request], ctx=CONTEXT)))

    assert result.values[0] is result.values[0]
    assert result.masks[0] is result.masks[0]

    result.values[0][0] = 999.0
    assert result.values_flat[0] == 999.0
    assert result.values[0][0] == 999.0

    copied = result.copy_values()
    copied[0][0] = -1.0
    assert result.values[0][0] == 999.0


def test_cffi_internals_are_gone() -> None:
    for name in ["ffi", "lib", "PatchedLib", "CData", "multivalued_dic_to_request"]:
        assert not hasattr(pygribjump, name)

    assert not hasattr(GribJump, "ctype")
    assert not hasattr(ExtractionRequest, "ctype")


def test_dic_to_request_supersedes_multivalued_dic_to_request() -> None:
    assert pygribjump.dic_to_request({"class": "od", "step": [1, 2, 3]}) == "class=od,step=1/2/3"
