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
#include "gribjump/remote/ProtocolCodec.h"
#include "gribjump/remote/RemoteGribJump.h"

namespace gribjump {

RemoteGribJump::RemoteGribJump() {
    std::string uri = ConfigOptions::instance().remoteURI();

    if (uri.empty())
        throw eckit::UserError("RemoteGribJump requires uri to be set in config (format host:port)", Here());

    eckit::net::Endpoint endpoint(uri);
    host_ = endpoint.host();
    port_ = endpoint.port();
}

RemoteGribJump::RemoteGribJump(eckit::net::Endpoint endpoint) : host_(endpoint.host()), port_(endpoint.port()) {}

RemoteGribJump::~RemoteGribJump() {}

void RemoteGribJump::sendHeader(eckit::Stream& stream, RequestType type) {
    ProtocolCodec::writeRequestHeader(stream, type, ContextManager::instance().context());
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()", LogRouter::instance().get("timer"));

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    sendHeader(stream, RequestType::SCAN);
    ProtocolCodec::encodeScanRequest(stream, requests, byfiles);

    std::stringstream ss;
    ss << "Sent " << requests.size() << " requests";
    timer.report(ss.str());

    // receive responses

    ProtocolCodec::decodeErrors(stream);

    size_t nFields = ProtocolCodec::decodeScanReply(stream);

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

    ProtocolCodec::encodeForwardScanRequest(stream, map);

    ProtocolCodec::decodeErrors(stream);

    size_t nfields = ProtocolCodec::decodeScanReply(stream);

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
    ProtocolCodec::encodeExtractRequest(stream, requests);

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive response

    ProtocolCodec::decodeErrors(stream);

    result = ProtocolCodec::decodeExtractReply(stream, nRequests);
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

    ProtocolCodec::encodeForwardExtractRequest(stream, filemap);

    timer.report("Request sent");
    ProtocolCodec::decodeErrors(stream);

    // receive results
    ProtocolCodec::decodeForwardExtractReply(stream, filemap);

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
    ProtocolCodec::encodeAxesRequest(stream, request, level);
    timer.report("Request sent");

    // receive response

    ProtocolCodec::decodeErrors(stream);

    result = ProtocolCodec::decodeAxesReply(stream);
    timer.report("Axes received");

    return result;
}

static GribJumpBuilder<RemoteGribJump> builder("remote");

}  // namespace gribjump
