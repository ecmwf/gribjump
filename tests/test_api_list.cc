/*
 * (C) Copyright 2026- ECMWF.
 * This software is licensed under the terms of the Apache Licence Version 2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <set>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "gribjump/GribJump.h"
#include "gribjump/MarsListerClient.h"
#include "gribjump/gribjump_config.h"

using namespace eckit::testing;
using namespace gribjump;

CASE("List results own locations and metadata and convert to path requests") {
    eckit::URI uri("fdb", eckit::PathName("/data/fields.grib"));
    uri.host("store");
    uri.port(9000);
    uri.query("length", "123");
    uri.fragment("4294967296");
    ListIterator iterator({ListResult(uri, {{"param", "130"}, {"step", "6"}}, eckit::Length(123))});
    EXPECT(iterator.hasNext());
    auto result = iterator.next();
    EXPECT(!iterator.hasNext());
    EXPECT(!iterator.next());
    EXPECT(!iterator.next());
    EXPECT_EQUAL(result->uri().asString(), uri.asString());
    EXPECT_EQUAL(static_cast<long long>(result->offset()), 4294967296LL);
    EXPECT_EQUAL(result->metadata().at("step"), "6");
    EXPECT_EQUAL(result->marsRequest().values("param")[0], "130");
    auto request = result->extractionRequest({{0, 10}}, "grid");
    EXPECT_EQUAL(request.path(), "/data/fields.grib");
    EXPECT_EQUAL(request.host(), "store");
    EXPECT_EQUAL(request.port(), 9000);
    EXPECT_EQUAL(request.scheme(), "fdb");
    EXPECT_EQUAL(request.offset(), 4294967296ULL);
    EXPECT_EQUAL(request.gridHash(), "grid");
}

CASE("FDB listing returns full locations and can feed path extraction") {
    const std::string cwd = eckit::LocalPathName::cwd();
    eckit::TmpDir root(cwd.c_str());
    root.mkdir();
    const std::string config =
        "type: local\nengine: toc\nschema: schema\nspaces:\n"
        "- roots:\n  - path: " +
        root.asString() + "\n";
    SetEnv env("FDB5_CONFIG", config.c_str());
    fdb5::FDB fdb;
    const eckit::PathName source("extract_ranges.grib");
    auto handle = std::unique_ptr<eckit::DataHandle>(source.fileHandle());
    fdb.archive(*handle);
    fdb.flush();

    // A sparse request: gribjump imposes no minimum set of keys.
    metkit::mars::MarsRequest selection("list");
    selection.setValue("date", "20230508");
    selection.values("step", {"1", "2", "3"});

    std::set<std::string> expected;
    auto fdbIterator = fdb.list(fdb5::FDBToolRequest(selection), true);
    fdb5::ListElement element;
    while (fdbIterator.next(element)) {
        expected.insert(element.location().fullUri().asRawString());
    }
    EXPECT_EQUAL(expected.size(), 3);

    // The iterator survives destruction of the client. Listing is independent
    // of the remote extraction endpoint (nothing is listening on this port).
    auto iterator = [&] {
        Config cfg;
        cfg.set("type", "remote");
        cfg.set("uri", "localhost:1");
        cfg.set("lister.type", "fdb");
        GribJump client(cfg);
        cfg.set("lister.type", "remote");  // the client owns a snapshot
        return client.list(selection);
    }();
    std::vector<PathExtractionRequest> requests;
    std::set<std::string> actual;
    while (auto field = iterator.next()) {
        actual.insert(field->uri().asRawString());
        EXPECT(!field->uri().fragment().empty());
        EXPECT(static_cast<long long>(field->length()) > 0);
        EXPECT_EQUAL(field->metadata().at("date"), "20230508");
        requests.push_back(field->extractionRequest({{0, 5}}, "33c7d6025995e1b4913811e77d38ec50"));
    }
    EXPECT_EQUAL(actual, expected);

    GribJump local;
    auto extracted = local.extract(requests).dumpVector();
    EXPECT_EQUAL(extracted.size(), 3);
    for (const auto& field : extracted) {
        EXPECT_EQUAL(field->total_values(), 5);
    }
    selection.setValue("step", "999");
    EXPECT(!local.list(selection).next());
}

CASE("Remote listing is configured independently and fails explicitly when used") {
    Config cfg;
    cfg.set("lister.type", "remote");
    cfg.set("lister.uri", "localhost:1");
    GribJump client(cfg);
    EXPECT_THROWS_AS(client.list(metkit::mars::MarsRequest("list")), eckit::NotImplemented);
    // It remains possible to use the extraction backend without a catalogue.
    EXPECT_NO_THROW(client.scan(std::vector<eckit::PathName>{"extract_ranges.grib"}));
}

#ifndef GRIBJUMP_HAVE_DHSKIT
CASE("MARS without dhskit fails explicitly instead of trying the network") {
    Config cfg;
    cfg.set("lister.type", "mars");
    cfg.set("lister.uri", "localhost:1");
    GribJump client(cfg);
    EXPECT_THROWS_AS(client.list(metkit::mars::MarsRequest("list")), eckit::UserError);
}
#endif

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
