# (C) Copyright 2026- ECMWF.
# This software is licensed under the terms of the Apache Licence Version 2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import gc
import os
from types import MappingProxyType

import pytest
from helpers import BASE_REQUEST, CONTEXT, compare_synthetic_data, expected_values
from test_extract_fdb import read_only_fdb_setup  # noqa: F401 -- shared fixture

from pygribjump import GribJump, GribJumpException, ListIterator, ListResult, PathExtractionRequest

requires_fdb = pytest.mark.skipif(
    os.getenv("PYGRIBJUMP_TEST_ENABLE_FDB") != "1", reason="requires a test FDB"
)


@requires_fdb
@pytest.mark.parametrize("form", ["mapping", "selection", "list", "retrieve"])
def test_list_locations_and_extract(read_only_fdb_setup, form):
    request = {"date": BASE_REQUEST["date"], "step": [1, 3]}
    if form != "mapping":
        request = f"date={BASE_REQUEST['date']}, step=1/3"
        if form != "selection":
            request = form + ", " + request
    client = GribJump()
    iterator = client.list(request, ctx=CONTEXT)
    assert isinstance(iterator, ListIterator)
    assert iter(iterator) is iterator
    fields = list(iterator)
    assert len(fields) == 2
    with pytest.raises(StopIteration):
        next(iterator)
    assert list(iterator) == []
    assert {field.metadata["step"] for field in fields} == {"1", "3"}
    for field in fields:
        assert isinstance(field, ListResult)
        assert field.scheme == "file"
        assert field.path
        assert field.offset >= 0
        assert field.length > 0
        assert f"#{field.offset}" in field.uri
        assert "param=" in field.mars_request
        metadata = field.metadata
        metadata["param"] = "changed"
        assert field.metadata["param"] != "changed"

    requests = [field.to_extraction_request([(0, 10)]) for field in fields]
    assert all(isinstance(request, PathExtractionRequest) for request in requests)
    for result in client.extract_from_paths(requests):
        compare_synthetic_data(result.values, expected_values([(0, 10)]))


@requires_fdb
def test_list_independent_of_extraction_and_client_lifetime(read_only_fdb_setup):
    config = {"type": "remote", "uri": "localhost:1", "lister": {"type": "fdb"}}
    client = GribJump(config)
    config["lister"]["type"] = "remote"
    iterator = client.list(MappingProxyType({"step": [0, 1, 2, 3, 4]}))
    first = next(iterator)
    del client
    gc.collect()
    fields = list(iterator)
    assert len(fields) == 4
    del iterator
    gc.collect()
    assert first.uri
    assert first.to_extraction_request([(0, 1)]).ranges == [(0, 1)]


@requires_fdb
def test_list_empty_and_unconstrained_selection(read_only_fdb_setup):
    client = GribJump()
    assert list(client.list({"step": 999})) == []
    assert len(list(client.list({}))) == 5
    assert len(list(client.list(""))) == 5
    assert len(list(client.list({"step": "0/1"}))) == 2


def test_remote_listing_reserved_but_path_extraction_still_works(grib_file):
    client = GribJump({"lister": {"type": "remote", "uri": "localhost:1"}})
    with pytest.raises(GribJumpException, match="remote GribJump server is not implemented"):
        client.list({"date": "20230508"})
    request = PathExtractionRequest(str(grib_file), "file", 0, "", 0, [(0, 5)])
    result = next(iter(client.extract_from_paths([request])))
    compare_synthetic_data(result.values, expected_values([(0, 5)]))


@pytest.mark.parametrize("selection", [[], [{"date": "20230508"}], None, 42, {1: "x"}, {"step": object()}])
def test_list_rejects_non_request_objects(selection):
    with pytest.raises(TypeError):
        GribJump().list(selection)


def test_list_rejects_multiple_requests():
    client = GribJump({"lister": {"type": "remote"}})
    with pytest.raises(GribJumpException, match="single MARS request"):
        client.list("list,param=130\nlist,param=131")


def test_mapping_conversion_does_not_roundtrip_through_request_text():
    class Backend:
        def list(self, request, ctx):
            self.request = request
            return iter(())

    client = GribJump()
    backend = Backend()
    client.gribjump = backend
    assert list(client.list({"custom": "a,b=c", "step": [1, 2], "date": "20230508"})) == []
    assert backend.request == {"custom": ["a,b=c"], "step": ["1", "2"], "date": ["20230508"]}
