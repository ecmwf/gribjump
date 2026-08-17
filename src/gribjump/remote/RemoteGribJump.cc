/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"

#include "gribjump/GribJumpFactory.h"
#include "gribjump/LogRouter.h"
#include "gribjump/remote/Protocol.h"
#include "gribjump/remote/RemoteGribJump.h"

namespace gribjump {

namespace {

ProtocolVersion validatedProtocolVersion(uint16_t version) {
    if (!isSupportedProtocolVersion(version)) {
        throw eckit::UserError("Unsupported client protocol version: " + std::to_string(version), Here());
    }
    return ProtocolVersion{version};
}

ProtocolVersion configuredClientVersion() {
    return validatedProtocolVersion(static_cast<uint16_t>(ConfigOptions::instance().clientProtocolVersion()));
}

// A single TCP request/reply exchange. Owns the socket (TCPClient) and the
// stream wrapping it; declaration order matters -- InstantTCPStream holds a
// reference to the socket, so client_ must outlive (be declared before)
// stream_.
class TcpConnection : public ClientConnection {
public:

    TcpConnection(const std::string& host, int port) : stream_(client_.connect(host, port)) {}

    eckit::Stream& stream() override { return stream_; }

private:

    eckit::net::TCPClient client_;
    eckit::net::InstantTCPStream stream_;
};

// Opens a fresh TCP connection per request, exactly as the client did inline
// before the transport seam was introduced.
class TcpTransport : public ClientTransport {
public:

    TcpTransport(std::string host, int port) : host_(std::move(host)), port_(port) {}

    std::unique_ptr<ClientConnection> connect() override { return std::make_unique<TcpConnection>(host_, port_); }

private:

    std::string host_;
    int port_;
};

}  // namespace

RemoteGribJump::RemoteGribJump() : protocolVersion_(configuredClientVersion()) {
    std::string uri = ConfigOptions::instance().remoteURI();

    if (uri.empty())
        throw eckit::UserError("RemoteGribJump requires uri to be set in config (format host:port)", Here());

    eckit::net::Endpoint endpoint(uri);
    host_      = endpoint.host();
    port_      = endpoint.port();
    transport_ = std::make_unique<TcpTransport>(host_, port_);
}

RemoteGribJump::RemoteGribJump(eckit::net::Endpoint endpoint) :
    host_(endpoint.host()),
    port_(endpoint.port()),
    transport_(std::make_unique<TcpTransport>(endpoint.host(), endpoint.port())),
    protocolVersion_(configuredClientVersion()) {}

RemoteGribJump::RemoteGribJump(std::unique_ptr<ClientTransport> transport, uint16_t protocolVersion) :
    host_(""),
    port_(0),
    transport_(std::move(transport)),
    protocolVersion_(validatedProtocolVersion(protocolVersion)) {}

RemoteGribJump::~RemoteGribJump() {}

void RemoteGribJump::sendHeader(eckit::Stream& stream, RequestType type) {
    Protocol::writeRequestHeader(stream, type, ContextManager::instance().context(), protocolVersion_.value);
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()", LogRouter::instance().get("timer"));

    // connect to server
    auto connection       = transport_->connect();
    eckit::Stream& stream = connection->stream();
    timer.report("Connection established");

    sendHeader(stream, RequestType::SCAN);
    Protocol::encodeScanRequest(stream, requests, byfiles);

    std::stringstream ss;
    ss << "Sent " << requests.size() << " requests";
    timer.report(ss.str());

    // receive responses

    Protocol::decodeErrors(stream);

    size_t nFields = Protocol::decodeScanReply(stream);

    timer.report("Scans complete");
    return nFields;
}

// Forward scan request to another server
size_t RemoteGribJump::forwardScan(const std::map<eckit::PathName, eckit::OffsetList>& map) {
    ///@todo we could probably do the connection logic in the ctor
    eckit::Timer timer("RemoteGribJump::scan()", LogRouter::instance().get("timer"));
    auto connection       = transport_->connect();
    eckit::Stream& stream = connection->stream();
    timer.report("Connection established");

    sendHeader(stream, RequestType::FORWARD_SCAN);

    Protocol::encodeForwardScanRequest(stream, map);

    Protocol::decodeErrors(stream);

    size_t nfields = Protocol::decodeScanReply(stream);

    eckit::Log::info() << "Scanned " << nfields << " field(s) on endpoint " << host_ << ":" << port_ << std::endl;

    timer.report("Scans complete");
    return nfields;
}

std::vector<std::unique_ptr<ExtractionResult>> RemoteGribJump::extract(std::vector<ExtractionRequest>& requests) {
    eckit::Timer timer("RemoteGribJump::extract()", LogRouter::instance().get("timer"));
    std::vector<std::unique_ptr<ExtractionResult>> result;

    // connect to server
    auto connection       = transport_->connect();
    eckit::Stream& stream = connection->stream();
    timer.report("Connection established");

    sendHeader(stream, RequestType::EXTRACT);

    size_t nRequests = requests.size();
    Protocol::encodeExtractRequest(stream, requests);

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive response

    if (protocolVersion_.streaming()) {
        // v4: results arrive as out-of-order RESULTS chunks terminated by END,
        // followed by the error trailer (which raises on server-side errors).
        result = Protocol::decodeExtractReplyStreaming(stream, nRequests);
    }
    else {
        // v3: leading error block, then the buffered in-order reply.
        Protocol::decodeErrors(stream);
        result = Protocol::decodeExtractReply(stream, nRequests);
    }
    timer.report("All data recieved");
    return result;
}

std::vector<std::unique_ptr<ExtractionResult>> RemoteGribJump::extract(std::vector<PathExtractionRequest>& requests) {
    NOTIMP;
}

// Forward extraction request to another server
void RemoteGribJump::forwardExtract(filemap_t& filemap) {

    eckit::Timer timer("RemoteGribJump::forwardExtract()", LogRouter::instance().get("timer"));

    ///@todo we could probably do the connection logic in the ctor
    auto connection       = transport_->connect();
    eckit::Stream& stream = connection->stream();
    timer.report("Connection established");

    sendHeader(stream, RequestType::FORWARD_EXTRACT);

    Protocol::encodeForwardExtractRequest(stream, filemap);

    timer.report("Request sent");
    Protocol::decodeErrors(stream);

    // receive results
    Protocol::decodeForwardExtractReply(stream, filemap);

    timer.report("Results received");

    return;
}

std::map<std::string, std::unordered_set<std::string>> RemoteGribJump::axes(const std::string& request, int level) {
    eckit::Timer timer("RemoteGribJump::axes()", LogRouter::instance().get("timer"));
    std::map<std::string, std::unordered_set<std::string>> result;

    // connect to server
    auto connection       = transport_->connect();
    eckit::Stream& stream = connection->stream();
    timer.report("Connection established");

    sendHeader(stream, RequestType::AXES);
    Protocol::encodeAxesRequest(stream, request, level);
    timer.report("Request sent");

    // receive response

    Protocol::decodeErrors(stream);

    result = Protocol::decodeAxesReply(stream);
    timer.report("Axes received");

    return result;
}

static GribJumpBuilder<RemoteGribJump> builder("remote");

}  // namespace gribjump
