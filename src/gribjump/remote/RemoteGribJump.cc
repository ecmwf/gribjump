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
uint16_t configuredClientVersion() {
    auto version = static_cast<uint16_t>(ConfigOptions::instance().clientProtocolVersion());
    if (!isSupportedProtocolVersion(version)) {
        throw eckit::UserError("Unsupported clientProtocolVersion: " + std::to_string(version), Here());
    }
    return version;
}
}  // namespace

RemoteGribJump::RemoteGribJump() : protocolVersion_(configuredClientVersion()) {
    std::string uri = ConfigOptions::instance().remoteURI();

    if (uri.empty())
        throw eckit::UserError("RemoteGribJump requires uri to be set in config (format host:port)", Here());

    eckit::net::Endpoint endpoint(uri);
    host_ = endpoint.host();
    port_ = endpoint.port();
}

RemoteGribJump::RemoteGribJump(eckit::net::Endpoint endpoint) :
    host_(endpoint.host()), port_(endpoint.port()), protocolVersion_(configuredClientVersion()) {}

RemoteGribJump::~RemoteGribJump() {}

void RemoteGribJump::sendHeader(eckit::Stream& stream, RequestType type) {
    Protocol::writeRequestHeader(stream, type, ContextManager::instance().context(), protocolVersion_);
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()", LogRouter::instance().get("timer"));

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
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
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
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
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    sendHeader(stream, RequestType::EXTRACT);

    size_t nRequests = requests.size();
    Protocol::encodeExtractRequest(stream, requests);

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive response

    if (protocolVersion_ >= streamingProtocolVersion) {
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
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
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
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
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
