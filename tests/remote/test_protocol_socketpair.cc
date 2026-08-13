/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Socketpair smoke test. Unlike the loopback test -- which drives the codecs
/// back-to-back over an in-memory buffer -- this test pushes the real bytes
/// through a genuine kernel socket (a connected AF_UNIX socketpair). The server
/// half runs dispatchRequest() on its own thread against a mock engine, exactly
/// as the production NetUser would, while the client half writes the request
/// and reads the reply over eckit's InstantTCPStream.

#include <sys/socket.h>
#include <thread>

#include "eckit/net/TCPSocket.h"
#include "eckit/net/TCPStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/Protocol.h"

#include "protocol_test_helpers.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// Wraps an already-connected file descriptor (e.g. from socketpair) in an
// eckit::net::TCPSocket so it can be driven by InstantTCPStream. The base
// class's socket_ member is protected, so a thin subclass is the simplest way
// to adopt a raw fd.

class FdSocket : public eckit::net::TCPSocket {
public:

    explicit FdSocket(int fd) { socket_ = fd; }
};

//-----------------------------------------------------------------------------

CASE("Socketpair: EXTRACT round-trips over a kernel socket") {
    int fds[2];
    EXPECT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    MockEngine engine;

    // Server thread: read + dispatch a single request, reply, then exit.
    std::thread server([&]() {
        FdSocket sock(fds[0]);
        eckit::net::InstantTCPStream s(sock);
        dispatchRequest(s, &engine);
        sock.close();
    });

    // Client: encode header + request and read back the reply.
    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};

    {
        FdSocket sock(fds[1]);
        eckit::net::InstantTCPStream s(sock);

        Protocol::writeRequestHeader(s, RequestType::EXTRACT, LogContext("{}"));
        Protocol::encodeExtractRequest(s, requests);

        EXPECT(!Protocol::decodeErrors(s));
        auto results = Protocol::decodeExtractReply(s, requests.size());

        EXPECT_EQUAL(results.size(), 2);
        for (auto& r : results) {
            EXPECT_EQUAL(r->nrange(), 2);
            EXPECT_EQUAL(r->nvalues(0), 2);
            EXPECT_EQUAL(r->nvalues(1), 1);
            EXPECT_EQUAL(r->values()[0][0], 10.0);
            EXPECT_EQUAL(r->values()[1][0], 30.0);
        }

        sock.close();
    }

    server.join();
    EXPECT_EQUAL(engine.lastExtractRequests, 2);
}

CASE("Socketpair: SCAN round-trips over a kernel socket") {
    int fds[2];
    EXPECT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    MockEngine engine;
    engine.scanNFields = 7;

    std::thread server([&]() {
        FdSocket sock(fds[0]);
        eckit::net::InstantTCPStream s(sock);
        dispatchRequest(s, &engine);
        sock.close();
    });

    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");
    mr.setValue("expver", "xxxx");
    std::vector<metkit::mars::MarsRequest> requests = {mr};

    {
        FdSocket sock(fds[1]);
        eckit::net::InstantTCPStream s(sock);

        Protocol::writeRequestHeader(s, RequestType::SCAN, LogContext("{}"));
        Protocol::encodeScanRequest(s, requests, true);

        EXPECT(!Protocol::decodeErrors(s));
        EXPECT_EQUAL(Protocol::decodeScanReply(s), 7);

        sock.close();
    }

    server.join();
    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
