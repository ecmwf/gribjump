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
/// so no FDB (or socket) is required. Requests are encoded and replies decoded
/// through the real remote Protocol (the same code the real client runs),
/// so a change to a production encoder/decoder is reflected here automatically.

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
        Protocol::encodeExtractRequest(s, requests);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastExtractRequests, 2);

    // Decode the reply as the client would: error block then per-request results
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    auto results = Protocol::decodeExtractReply(reply, n);
    EXPECT_EQUAL(results.size(), n);
    for (auto& res : results) {
        EXPECT_EQUAL(res->nrange(), 2);
        EXPECT_EQUAL(res->nvalues(0), 2);
        EXPECT_EQUAL(res->nvalues(1), 1);
        EXPECT_EQUAL(res->values()[0][1], 20.0);
    }
}

CASE("Server AXES: parse, execute (mock), reply") {
    std::string request = "class=rd,expver=xxxx";
    int level           = 3;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::AXES);
        Protocol::encodeAxesRequest(s, request, level);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastAxesRequest, request);
    EXPECT_EQUAL(engine.lastAxesLevel, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    auto axes = Protocol::decodeAxesReply(reply);
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
        Protocol::encodeScanRequest(s, requests, byfiles);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 7;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    EXPECT_EQUAL(Protocol::decodeScanReply(reply), 7ul);
}

CASE("Server dispatch rejects unsupported protocol version") {
    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        uint16_t badVersion = 9999;  // outside supportedProtocolVersions {3, 4}
        s << badVersion;
        s << LogContext("{}");
        s << static_cast<uint16_t>(RequestType::SCAN);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    EXPECT_THROWS_AS(dispatchRequest(stream, &engine), eckit::SeriousBug);
}

CASE("Server dispatch accepts every supported protocol version") {
    for (uint16_t version : supportedProtocolVersions) {
        auto reqBytes = encodeRequest([&](eckit::Stream& s) {
            s << version;
            s << LogContext("{}");
            s << static_cast<uint16_t>(RequestType::SCAN);
            Protocol::encodeScanRequest(s, {}, false);
        });

        DuplexTestStream stream(reqBytes);
        MockEngine engine;
        EXPECT_NO_THROW(dispatchRequest(stream, &engine));
    }
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
    scanmap_t scanmap;
    scanmap[eckit::PathName("/data/file.grib")] = offsets;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::FORWARD_SCAN);
        Protocol::encodeForwardScanRequest(s, scanmap);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 5;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastScanmapFiles, 1);
    EXPECT_EQUAL(engine.lastScanmapOffsets, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    EXPECT_EQUAL(Protocol::decodeScanReply(reply), 5ul);
}

CASE("Server FORWARD_EXTRACT: parse, execute (mock), reply") {
    // Build a one-item filemap and drive both the request encode and the reply
    // decode through the production codec (as the real client does).
    auto item = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(fixtureRequest(1)));
    item->URI(eckit::URI("file", eckit::PathName("/data/file.grib")));

    filemap_t filemap;
    filemap["/data/file.grib"] = {item.get()};

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::FORWARD_EXTRACT);
        Protocol::encodeForwardExtractRequest(s, filemap);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastFilemapFiles, 1);
    EXPECT_EQUAL(engine.lastFilemapItems, 1);

    // Client-side decode: error block, then results filled into the filemap.
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    Protocol::decodeForwardExtractReply(reply, filemap);

    auto res = item->result();
    EXPECT(res != nullptr);
    EXPECT_EQUAL(res->nrange(), 2);
    EXPECT_EQUAL(res->values()[0][0], 10.0);
}

CASE("Server reports engine errors in the error block") {
    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeader(s, RequestType::SCAN);
        Protocol::encodeScanRequest(s, {mr}, false);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.errors = {"boom: something failed", "and another"};
    dispatchRequest(stream, &engine);

    // Manual decode here on purpose: Protocol::decodeErrors does not surface the individual messages (it throws/logs),
    // so to assert the exact error strings and their order we read the block directly. The block's *encoding* is pinned
    // by the codec golden test.
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

//-----------------------------------------------------------------------------
// v4 streaming reply path: the same server serves v3 and v4.

CASE("Server EXTRACT v4: streams results, reassembled by index") {
    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2), fixtureRequest(3)};
    size_t n                                = requests.size();

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeaderVersion(s, streamingProtocolVersion, RequestType::EXTRACT);
        Protocol::encodeExtractRequest(s, requests);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    EXPECT_EQUAL(engine.lastExtractRequests, n);

    // Decode as a v4 client would: RESULTS chunks (any order) then END + footer.
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    auto results = Protocol::decodeExtractReplyStreaming(reply, n);
    EXPECT_EQUAL(results.size(), n);
    for (auto& res : results) {
        EXPECT(res != nullptr);  // every index was filled despite reverse-order chunks
        EXPECT_EQUAL(res->nrange(), 2);
        EXPECT_EQUAL(res->nvalues(0), 2);
        EXPECT_EQUAL(res->nvalues(1), 1);
        EXPECT_EQUAL(res->values()[0][1], 20.0);
    }
}

CASE("Server EXTRACT v4: engine errors surface in the END-chunk footer") {
    std::vector<ExtractionRequest> requests = {fixtureRequest(1)};
    size_t n                                = requests.size();

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeaderVersion(s, streamingProtocolVersion, RequestType::EXTRACT);
        Protocol::encodeExtractRequest(s, requests);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.errors = {"boom: streaming failed"};
    dispatchRequest(stream, &engine);

    // No leading error block in v4: the reply starts with chunks and the footer
    // (raise=true) throws the collected errors.
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT_THROWS_AS(Protocol::decodeExtractReplyStreaming(reply, n), eckit::RemoteException);
}

CASE("Server EXTRACT v3 client still gets the buffered reply") {
    // A v3-pinned client against the v4-capable server: leading error block +
    // buffered per-request results.
    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};
    size_t n                                = requests.size();

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        writeHeaderVersion(s, remoteProtocolVersion, RequestType::EXTRACT);
        Protocol::encodeExtractRequest(s, requests);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!Protocol::decodeErrors(reply));
    auto results = Protocol::decodeExtractReply(reply, n);
    EXPECT_EQUAL(results.size(), n);
    for (auto& res : results) {
        EXPECT_EQUAL(res->nrange(), 2);
    }
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
