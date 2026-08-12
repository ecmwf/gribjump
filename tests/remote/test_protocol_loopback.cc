/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Full client<->server loopback tests. These drive the *real* ProtocolCodec
/// encode path (as the client uses) into the *real* server parse + reply
/// (dispatchRequest) back-to-back over an in-memory stream, with a mock engine,
/// then decode the reply with the *same* ProtocolCodec. Because client and
/// server share one codec, this catches any disagreement about the wire format
/// -- and proves the codec round-trips against the live server path.
/// No FDB, no sockets.

#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/ProtocolCodec.h"

#include "protocol_test_helpers.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE("Loopback: EXTRACT round-trips through codec and server") {
    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};

    // Client-side encode: header + payload, exactly as RemoteGribJump sends.
    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        ProtocolCodec::writeRequestHeader(s, RequestType::EXTRACT, LogContext("{}"));
        ProtocolCodec::encodeExtractRequest(s, requests);
    });

    // Server parses + executes (mock) + replies.
    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastExtractRequests, 2);

    // Client-side decode of the reply.
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!ProtocolCodec::decodeErrors(reply));
    auto results = ProtocolCodec::decodeExtractReply(reply, requests.size());

    EXPECT_EQUAL(results.size(), 2);
    for (auto& r : results) {
        EXPECT_EQUAL(r->nrange(), 2);
        EXPECT_EQUAL(r->nvalues(0), 2);
        EXPECT_EQUAL(r->nvalues(1), 1);
        EXPECT_EQUAL(r->values()[0][0], 10.0);
        EXPECT_EQUAL(r->values()[0][1], 20.0);
        EXPECT_EQUAL(r->values()[1][0], 30.0);
    }
}

CASE("Loopback: SCAN round-trips through codec and server") {
    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");
    mr.setValue("expver", "xxxx");
    std::vector<metkit::mars::MarsRequest> requests = {mr};
    bool byfiles                                    = true;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        ProtocolCodec::writeRequestHeader(s, RequestType::SCAN, LogContext("{}"));
        ProtocolCodec::encodeScanRequest(s, requests, byfiles);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 11;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!ProtocolCodec::decodeErrors(reply));
    EXPECT_EQUAL(ProtocolCodec::decodeScanReply(reply), 11);
}

CASE("Loopback: AXES round-trips through codec and server") {
    std::string request = "class=rd,expver=xxxx";
    int level           = 3;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        ProtocolCodec::writeRequestHeader(s, RequestType::AXES, LogContext("{}"));
        ProtocolCodec::encodeAxesRequest(s, request, level);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastAxesRequest, request);
    EXPECT_EQUAL(engine.lastAxesLevel, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    EXPECT(!ProtocolCodec::decodeErrors(reply));
    auto axes = ProtocolCodec::decodeAxesReply(reply);

    EXPECT_EQUAL(axes.size(), 2);
    EXPECT(axes.find("step") != axes.end());
    EXPECT_EQUAL(axes["step"].size(), 3);
    EXPECT(axes["step"].count("2") == 1);
    EXPECT_EQUAL(axes["levtype"].size(), 1);
    EXPECT(axes["levtype"].count("sfc") == 1);
}

CASE("Loopback: server errors propagate to the client as an exception") {
    std::vector<metkit::mars::MarsRequest> requests = {metkit::mars::MarsRequest("retrieve")};

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        ProtocolCodec::writeRequestHeader(s, RequestType::SCAN, LogContext("{}"));
        ProtocolCodec::encodeScanRequest(s, requests, false);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.errors = {"deliberate failure"};
    dispatchRequest(stream, &engine);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    // decodeErrors raises when the server reported errors, exactly as the real
    // client would.
    EXPECT_THROWS_AS(ProtocolCodec::decodeErrors(reply), eckit::RemoteException);
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
