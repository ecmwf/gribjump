/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Client tests. Unlike the loopback test -- which drives the Protocol codecs
/// directly -- these exercise the real RemoteGribJump client: its version
/// negotiation and its reply-decode branch (v4 streaming vs v3 buffered). The
/// client's transport is injectable, so we hand it a socketpair-backed transport
/// whose peer runs the real server dispatch against a mock engine. A single
/// remote.extract(...) call drives header write -> server parse+execute -> reply
/// -> client decode over a genuine kernel socket, with no live TCP server.

#include <sys/socket.h>
#include <thread>

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPStream.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/ClientTransport.h"
#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/Protocol.h"
#include "gribjump/remote/RemoteGribJump.h"

#include "protocol_test_helpers.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// A ClientConnection over one half of a socketpair fd: owns the socket and the
// stream wrapping it (declaration order matters -- the stream references the
// socket).

class FdConnection : public ClientConnection {
public:

    explicit FdConnection(int fd) : sock_(fd), stream_(sock_) {}

    ~FdConnection() override { sock_.close(); }

    eckit::Stream& stream() override { return stream_; }

private:

    FdSocket sock_;
    eckit::net::InstantTCPStream stream_;
};

// Injectable transport whose peer runs the real server dispatch against a mock
// engine on its own thread. connect() is called once per exchange
// (RemoteGribJump opens one connection per request); the server thread is
// joined on teardown.

class SocketpairTransport : public ClientTransport {
public:

    explicit SocketpairTransport(MockEngine& engine) : engine_(engine) {}

    ~SocketpairTransport() override {
        if (server_.joinable()) {
            server_.join();
        }
    }

    std::unique_ptr<ClientConnection> connect() override {
        int fds[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            throw eckit::FailedSystemCall("socketpair");
        }

        server_ = std::thread([serverFd = fds[0], &engine = engine_]() {
            FdSocket sock(serverFd);
            try {
                eckit::net::InstantTCPStream s(sock);
                dispatchRequest(s, &engine);
            }
            catch (...) {
                // The client may stop reading and close on error; a real server
                // tolerates the resulting write failure rather than crashing.
            }
            sock.close();
        });

        return std::make_unique<FdConnection>(fds[1]);
    }

private:

    MockEngine& engine_;
    std::thread server_;
};

//-----------------------------------------------------------------------------

CASE("Client: v4 client streams and reassembles results end-to-end") {
    MockEngine engine;
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), streamingProtocolVersion);

    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2), fixtureRequest(3)};
    auto results                            = remote.extract(requests);

    EXPECT_EQUAL(engine.lastExtractRequests, 3);
    EXPECT_EQUAL(results.size(), 3);
    for (auto& r : results) {
        EXPECT(r != nullptr);  // every index filled despite the mock's reverse-order chunks
        EXPECT_EQUAL(r->nrange(), 2);
        EXPECT_EQUAL(r->nvalues(0), 2);
        EXPECT_EQUAL(r->nvalues(1), 1);
        EXPECT_EQUAL(r->values()[0][0], 10.0);
        EXPECT_EQUAL(r->values()[1][0], 30.0);
    }
}

CASE("Client: v3-pinned client gets the buffered reply end-to-end") {
    MockEngine engine;
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), remoteProtocolVersion);

    std::vector<ExtractionRequest> requests = {fixtureRequest(1), fixtureRequest(2)};
    auto results                            = remote.extract(requests);

    EXPECT_EQUAL(engine.lastExtractRequests, 2);
    EXPECT_EQUAL(results.size(), 2);
    for (auto& r : results) {
        EXPECT(r != nullptr);
        EXPECT_EQUAL(r->nrange(), 2);
        EXPECT_EQUAL(r->values()[0][0], 10.0);
        EXPECT_EQUAL(r->values()[1][0], 30.0);
    }
}

CASE("Client: scan() round-trips end-to-end (non-EXTRACT verb over a real socket)") {
    MockEngine engine;
    engine.scanNFields = 7;
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), streamingProtocolVersion);

    metkit::mars::MarsRequest mr("retrieve");
    mr.setValue("class", "rd");
    mr.setValue("expver", "xxxx");
    std::vector<metkit::mars::MarsRequest> requests = {mr};

    size_t nFields = remote.scan(requests, true);

    EXPECT_EQUAL(nFields, 7);
    EXPECT_EQUAL(engine.lastScanRequests, 1);
    EXPECT_EQUAL(engine.lastByfiles, true);
}

CASE("Client: v4 client with no requests yields an empty result") {
    MockEngine engine;
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), streamingProtocolVersion);

    std::vector<ExtractionRequest> requests;  // empty
    auto results = remote.extract(requests);

    EXPECT_EQUAL(engine.lastExtractRequests, 0);
    EXPECT_EQUAL(results.size(), 0);
}

CASE("Client: v4 server errors surface as an exception") {
    MockEngine engine;
    engine.errors = {"deliberate streaming failure"};
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), streamingProtocolVersion);

    std::vector<ExtractionRequest> requests = {fixtureRequest(1)};
    EXPECT_THROWS_AS(remote.extract(requests), eckit::RemoteException);
}

CASE("Client: v3-pinned server errors surface as an exception") {
    MockEngine engine;
    engine.errors = {"deliberate buffered failure"};
    RemoteGribJump remote(std::make_unique<SocketpairTransport>(engine), remoteProtocolVersion);

    std::vector<ExtractionRequest> requests = {fixtureRequest(1)};
    EXPECT_THROWS_AS(remote.extract(requests), eckit::RemoteException);
}

CASE("Client: an unsupported advertised version is rejected") {
    MockEngine engine;
    const uint16_t bogusVersion = 99;
    EXPECT_THROWS_AS(RemoteGribJump(std::make_unique<SocketpairTransport>(engine), bogusVersion), eckit::UserError);
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
