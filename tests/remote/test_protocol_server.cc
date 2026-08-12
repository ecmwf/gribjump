/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Server-side protocol tests: drive the real Request classes and the server
/// dispatch (dispatchRequest) against an in-memory stream, using a MockEngine
/// so no FDB (or socket) is required. These tests pin the server's parse ->
/// execute -> reply behaviour and the request-type dispatch / version
/// negotiation.

#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/GribJumpUser.h"

#include "protocol_test_helpers.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE("Server EXTRACT: parse, execute (mock), reply") {
    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};
    size_t n                                = requests.size();

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::EXTRACT);
        s << n;
        for (auto& r : requests) {
            s << r;
        }
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastExtractRequests, 2);

    // Decode the reply as the client would: error block then per-request results
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 0);

    for (size_t i = 0; i < n; i++) {
        size_t nfields;
        reply >> nfields;
        EXPECT_EQUAL(nfields, 1);
        ExtractionResult res(reply);
        EXPECT_EQUAL(res.nrange(), 2);
        EXPECT_EQUAL(res.nvalues(0), 2);
        EXPECT_EQUAL(res.nvalues(1), 1);
        EXPECT_EQUAL(res.values()[0][1], 20.0);
    }
}

CASE("Server AXES: parse, execute (mock), reply") {
    std::string request = "class=rd,expver=xxxx";
    int level           = 3;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::AXES);
        s << request;
        s << level;
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastAxesRequest, request);
    EXPECT_EQUAL(engine.lastAxesLevel, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 0);

    // Decode axes reply order-insensitively
    std::map<std::string, std::unordered_set<std::string>> axes;
    size_t nAxes;
    reply >> nAxes;
    for (size_t i = 0; i < nAxes; i++) {
        std::string name;
        reply >> name;
        size_t nvals;
        reply >> nvals;
        std::unordered_set<std::string> vals;
        for (size_t j = 0; j < nvals; j++) {
            std::string v;
            reply >> v;
            vals.insert(v);
        }
        axes[name] = vals;
    }
    EXPECT_EQUAL(axes.size(), 2);
    EXPECT_EQUAL(axes["step"].size(), 3);
    EXPECT(axes["step"].count("2") == 1);
    EXPECT_EQUAL(axes["levtype"].size(), 1);
}

CASE("Server SCAN: parse, execute (mock), reply") {
    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");
    std::vector<metkit::mars::MarsRequest> requests = {mr};
    bool byfiles                                    = true;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::SCAN);
        s << byfiles;
        size_t n = requests.size();
        s << n;
        for (auto& r : requests) {
            s << r;
        }
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 7;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 0);
    size_t nFields;
    reply >> nFields;
    EXPECT_EQUAL(nFields, 7);
}

CASE("Server dispatch rejects protocol version mismatch") {
    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        uint16_t badVersion = remoteProtocolVersion + 1;
        s << badVersion;
        s << LogContext("{}");
        s << static_cast<uint16_t>(RequestType::SCAN);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    EXPECT_THROWS_AS(dispatchRequest(stream, &engine), eckit::SeriousBug);
}

CASE("Server dispatch rejects unknown request type") {
    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        s << remoteProtocolVersion;
        s << LogContext("{}");
        s << static_cast<uint16_t>(9999);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    EXPECT_THROWS_AS(dispatchRequest(stream, &engine), eckit::SeriousBug);
}

CASE("Server FORWARD_SCAN: parse, execute (mock), reply") {
    eckit::OffsetList offsets = {eckit::Offset(0), eckit::Offset(1024), eckit::Offset(2048)};

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::FORWARD_SCAN);
        size_t nFiles = 1;
        s << nFiles;
        std::string fname = "/data/file.grib";
        s << fname;
        s << offsets;
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 5;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastScanmapFiles, 1);
    EXPECT_EQUAL(engine.lastScanmapOffsets, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 0);
    size_t nfields;
    reply >> nfields;
    EXPECT_EQUAL(nfields, 5);
}

CASE("Server FORWARD_EXTRACT: parse, execute (mock), reply") {
    ExtractionRequest req = fixtureRequest(1);
    eckit::URI uri("file", eckit::PathName("/data/file.grib"));
    std::string fname = "/data/file.grib";

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::FORWARD_EXTRACT);
        size_t nFiles = 1;
        s << nFiles;
        s << fname;
        size_t nItems = 1;
        s << nItems;
        s << req;
        s << uri;
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastFilemapFiles, 1);
    EXPECT_EQUAL(engine.lastFilemapItems, 1);

    // Reply: error block, then per-file [fname, nItems, results...]
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 0);
    std::string replyFname;
    reply >> replyFname;
    EXPECT_EQUAL(replyFname, fname);
    size_t nItems;
    reply >> nItems;
    EXPECT_EQUAL(nItems, 1);
    ExtractionResult res(reply);
    EXPECT_EQUAL(res.nrange(), 2);
    EXPECT_EQUAL(res.values()[0][0], 10.0);
}

CASE("Server reports engine errors in the error block") {
    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::SCAN);
        bool byfiles = false;
        s << byfiles;
        size_t n = 1;
        s << n;
        s << mr;
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.errors = {"boom: something failed", "and another"};
    dispatchRequest(stream, &engine);

    // Client-side error decoding (mirrors ProtocolCodec::decodeErrors)
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    size_t nErrors;
    reply >> nErrors;
    EXPECT_EQUAL(nErrors, 2);
    std::vector<std::string> got;
    for (size_t i = 0; i < nErrors; i++) {
        std::string e;
        reply >> e;
        got.push_back(e);
    }
    EXPECT_EQUAL(got[0], std::string("boom: something failed"));
    EXPECT_EQUAL(got[1], std::string("and another"));
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
