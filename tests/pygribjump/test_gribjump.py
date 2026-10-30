# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Tests for the GribJump object itself.
"""

import pathlib
import shutil

import pytest
from helpers import BASE_REQUEST, CONTEXT

from pygribjump import ExtractionRequest, GribJump


def test_gribjump_can_be_constructed() -> None:
    gribjump = GribJump()

    assert repr(gribjump) == "GribJump()"


def test_gribjump_is_a_context_manager() -> None:
    with GribJump() as gribjump:
        assert isinstance(gribjump, GribJump)


def test_context_manager_does_not_swallow_exceptions() -> None:
    with pytest.raises(ValueError, match="propagated"):
        with GribJump():
            raise ValueError("propagated")


def test_extract_rejects_bad_input() -> None:
    gribjump = GribJump()

    with pytest.raises(ValueError, match="should be a list"):
        gribjump.extract(ExtractionRequest(BASE_REQUEST, [(0, 1)]), ctx=CONTEXT)

    with pytest.raises(ValueError, match="should not be empty"):
        gribjump.extract([], ctx=CONTEXT)

    with pytest.raises(ValueError, match="should be a list of tuples"):
        gribjump.extract(["not-a-request"], ctx=CONTEXT)


def test_extract_rejects_mixed_request_types() -> None:
    gribjump = GribJump()

    request = ExtractionRequest(BASE_REQUEST, [(0, 1)])
    tuple_request = (BASE_REQUEST, [(0, 1)])

    with pytest.raises(ValueError, match="not a mixture of types"):
        gribjump.extract([request, tuple_request], ctx=CONTEXT)

    with pytest.raises(ValueError, match="not a mixture of types"):
        gribjump.extract([tuple_request, request], ctx=CONTEXT)


def test_extract_from_ranges_rejects_bad_input() -> None:
    gribjump = GribJump()

    with pytest.raises(ValueError, match="list of dictionaries"):
        gribjump.extract_from_ranges(BASE_REQUEST, [(0, 1)], ctx=CONTEXT)


def test_extract_from_paths_rejects_bad_input() -> None:
    gribjump = GribJump()

    with pytest.raises(ValueError, match="list of PathExtractionRequest"):
        gribjump.extract_from_paths("not-a-list", ctx=CONTEXT)

    with pytest.raises(ValueError, match="not be empty"):
        gribjump.extract_from_paths([], ctx=CONTEXT)


def test_polyrequest_tuples_are_unpacked() -> None:
    gribjump = GribJump()

    # The tuple syntax is deprecated (see test_cffi_compat.py) but still supported,
    # so this emits a DeprecationWarning before rejecting the malformed tuple.
    with pytest.raises(ValueError, match="length 2 or 3"):
        gribjump.extract([(BASE_REQUEST, [(0, 1)], "hash", "too-much")], ctx=CONTEXT)


def test_scan_of_a_grib_file(grib_file: pathlib.Path) -> None:
    gribjump = GribJump()

    # A single field is stored in the test file
    assert gribjump.scan([str(grib_file)], ctx=CONTEXT) == 1


def test_multiple_instances_remain_usable(grib_file: pathlib.Path) -> None:
    second_file = grib_file.with_name("second.grib")
    shutil.copy(grib_file, second_file)
    first = GribJump()
    second = GribJump()

    assert first.scan([str(grib_file)], ctx=CONTEXT) == 1
    assert second.scan([str(second_file)], ctx=CONTEXT) == 1
