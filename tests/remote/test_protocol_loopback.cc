/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Full client<->server loopback tests. These wire the *real* client
/// serialisation (RemoteGribJump's codec helpers) to the *real* server parse +
/// reply (dispatchRequest) back-to-back over an in-memory stream, with a mock
/// engine. This catches any disagreement between the client and server about
/// the wire format -- the class of bug most likely to slip past two
/// independently maintained codecs. No FDB, no sockets.

#include "eckit/net/Endpoint.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/GribJumpUser.h"

#include "protocol_test_helpers.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// Grants the loopback tests access to RemoteGribJump's transport-independent
// codec helpers (declared as a friend of RemoteGribJump), so we can drive the
// real client encode/decode without opening a TCP connection.

class RemoteProtocolTestAccess {
public:

    explicit RemoteProtocolTestAccess(RemoteGribJump& client) : c_(client) {}

    void sendHeader(eckit::Stream& s, RequestType type) { c_.sendHeader(s, type); }

    void encodeExtractRequest(eckit::Stream& s, std::vector<ExtractionRequest>& r) { c_.encodeExtractRequest(s, r); }
    std::vector<std::unique_ptr<ExtractionResult>> decodeExtractReply(eckit::Stream& s, size_t n) {
        return c_.decodeExtractReply(s, n);
    }

    void encodeScanRequest(eckit::Stream& s, const std::vector<metkit::mars::MarsRequest>& r, bool byfiles) {
        c_.encodeScanRequest(s, r, byfiles);
    }
    size_t decodeScanReply(eckit::Stream& s) { return c_.decodeScanReply(s); }

    void encodeAxesRequest(eckit::Stream& s, const std::string& r, int level) { c_.encodeAxesRequest(s, r, level); }
    std::map<std::string, std::unordered_set<std::string>> decodeAxesReply(eckit::Stream& s) {
        return c_.decodeAxesReply(s);
    }

    bool receiveErrors(eckit::Stream& s, bool raise = true) { return c_.receiveErrors(s, raise); }

private:

    RemoteGribJump& c_;
};

//-----------------------------------------------------------------------------
// The endpoint is never actually contacted; only the codec helpers are used.

CASE("Loopback: EXTRACT round-trips through real client and server") {
    RemoteGribJump client(eckit::net::Endpoint("localhost", 1));
    RemoteProtocolTestAccess access(client);

    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};

    // Client encodes the request frame (header + payload) exactly as it would
    // over TCP.
    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        access.sendHeader(s, RequestType::EXTRACT);
        access.encodeExtractRequest(s, requests);
    });

    // Server parses + executes (mock) + replies.
    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastExtractRequests, 2);

    // Client decodes the reply.
    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    bool error = access.receiveErrors(reply);
    EXPECT(!error);
    auto results = access.decodeExtractReply(reply, requests.size());

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

CASE("Loopback: SCAN round-trips through real client and server") {
    RemoteGribJump client(eckit::net::Endpoint("localhost", 1));
    RemoteProtocolTestAccess access(client);

    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");
    mr.setValue("expver", "xxxx");
    std::vector<metkit::mars::MarsRequest> requests = {mr};
    bool byfiles                                    = true;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        access.sendHeader(s, RequestType::SCAN);
        access.encodeScanRequest(s, requests, byfiles);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.scanNFields = 11;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    bool error = access.receiveErrors(reply);
    EXPECT(!error);
    size_t nFields = access.decodeScanReply(reply);
    EXPECT_EQUAL(nFields, 11);
}

CASE("Loopback: AXES round-trips through real client and server") {
    RemoteGribJump client(eckit::net::Endpoint("localhost", 1));
    RemoteProtocolTestAccess access(client);

    std::string request = "class=rd,expver=xxxx";
    int level           = 3;

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        access.sendHeader(s, RequestType::AXES);
        access.encodeAxesRequest(s, request, level);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    dispatchRequest(stream, &engine);
    EXPECT_EQUAL(engine.lastAxesRequest, request);
    EXPECT_EQUAL(engine.lastAxesLevel, 3);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    bool error = access.receiveErrors(reply);
    EXPECT(!error);
    auto axes = access.decodeAxesReply(reply);

    EXPECT_EQUAL(axes.size(), 2);
    EXPECT(axes.find("step") != axes.end());
    EXPECT_EQUAL(axes["step"].size(), 3);
    EXPECT(axes["step"].count("2") == 1);
    EXPECT_EQUAL(axes["levtype"].size(), 1);
    EXPECT(axes["levtype"].count("sfc") == 1);
}

CASE("Loopback: server errors propagate to the client as an exception") {
    RemoteGribJump client(eckit::net::Endpoint("localhost", 1));
    RemoteProtocolTestAccess access(client);

    std::vector<metkit::mars::MarsRequest> requests = {metkit::mars::MarsRequest("retrieve")};

    auto reqBytes = encodeRequest([&](eckit::Stream& s) {
        access.sendHeader(s, RequestType::SCAN);
        access.encodeScanRequest(s, requests, false);
    });

    DuplexTestStream stream(reqBytes);
    MockEngine engine;
    engine.errors = {"deliberate failure"};
    dispatchRequest(stream, &engine);

    eckit::MemoryStream reply(stream.written().data(), stream.written().size());
    // receiveErrors raises when the server reported errors, exactly as the real
    // client would.
    EXPECT_THROWS_AS(access.receiveErrors(reply), eckit::RemoteException);
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
