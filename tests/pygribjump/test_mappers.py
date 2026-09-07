# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Tests for the mapping of user input onto the internal representation.
"""

import json

import pytest

import pygribjump
from pygribjump._internal.pygribjump_internal import ContextMapper, RequestMapper


def test_request_mapper_single_values() -> None:
    request = {"class": "od", "expver": "0001", "levtype": "pl"}

    assert RequestMapper.to_request_string(request) == "class=od,expver=0001,levtype=pl"
    assert RequestMapper.to_retrieve_string(request) == "retrieve,class=od,expver=0001,levtype=pl"


def test_request_mapper_multiple_values() -> None:
    request = {"class": "od", "expver": "0001", "step": [1, 2, 3]}

    assert RequestMapper.to_request_string(request) == "class=od,expver=0001,step=1/2/3"


def test_request_mapper_passes_strings_through() -> None:
    assert RequestMapper.to_request_string("class=od,step=1") == "class=od,step=1"


def test_request_mapper_rejects_unknown_types() -> None:
    with pytest.raises(ValueError, match="must be str or dict"):
        RequestMapper.to_request_string(42)

    with pytest.raises(ValueError, match="Unknown type for key"):
        RequestMapper.to_request_string({"step": None})


def test_request_mapper_validates_ranges() -> None:
    assert RequestMapper.to_ranges([(0, 6), (7, 12)]) == [(0, 6), (7, 12)]

    with pytest.raises(ValueError, match="at least one"):
        RequestMapper.to_ranges([])

    with pytest.raises(ValueError, match="invalid range"):
        RequestMapper.to_ranges([(6, 0)])


def test_context_mapper_provides_defaults() -> None:
    ctx = json.loads(ContextMapper.to_json(None, "pytest_action", "1.2.3", "1.2.3"))

    assert ctx["source"] == "pygribjump"
    assert ctx["action"] == "pytest_action"
    assert ctx["pygribjump_version"] == "1.2.3"
    assert ctx["gribjump_version"] == "1.2.3"
    assert "user" in ctx
    assert "hostname" in ctx


def test_context_mapper_user_context_takes_precedence() -> None:
    ctx = json.loads(ContextMapper.to_json({"source": "pytest", "extra": "1"}, "act", "1.2.3", "1.2.3"))

    assert ctx["source"] == "pytest"
    assert ctx["extra"] == "1"
    assert ctx["action"] == "act"


def test_context_mapper_rejects_unknown_types() -> None:
    with pytest.raises(ValueError, match="must be dict"):
        ContextMapper.to_json("not-a-dict", "act", "1.2.3", "1.2.3")


def test_dic_to_request() -> None:
    assert pygribjump.dic_to_request({"class": "od", "levtype": "pl"}) == "class=od,levtype=pl"
    assert pygribjump.dic_to_request({"class": "od", "step": [1, 2, 3]}) == "class=od,step=1/2/3"
