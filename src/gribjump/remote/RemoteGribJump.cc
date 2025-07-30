/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
#include <algorithm>

#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Timer.h"

#include "gribjump/GribJumpFactory.h"
#include "gribjump/LogRouter.h"
#include "gribjump/remote/RemoteGribJump.h"

namespace gribjump {

RemoteGribJump::RemoteGribJump(const Config& config) : GribJumpBase(config) {
    std::string uri = config.getString("uri", "");

    if (uri.empty())
        throw eckit::UserError("RemoteGribJump requires uri to be set in config (format host:port)", Here());

    eckit::net::Endpoint endpoint(uri);
    host_ = endpoint.host();
    port_ = endpoint.port();
}

RemoteGribJump::RemoteGribJump(eckit::net::Endpoint endpoint) : host_(endpoint.host()), port_(endpoint.port()) {}

RemoteGribJump::~RemoteGribJump() {}

void RemoteGribJump::sendHeader(eckit::net::InstantTCPStream& stream, RequestType type) {
    stream << remoteProtocolVersion;
    stream << ContextManager::instance().context();
    stream << static_cast<uint16_t>(type);
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()", LogRouter::instance().get("timer"));

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    sendHeader(stream, RequestType::SCAN);
    stream << byfiles;

    size_t nRequests = requests.size();
    stream << nRequests;
    for (auto& req : requests) {
        stream << req;
    }

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive responses

    bool error = receiveErrors(stream);

    size_t nFields;
    stream >> nFields;

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

    size_t nFiles = map.size();
    stream << nFiles;

    for (auto& [fname, offsets] : map) {
        stream << fname;
        stream << offsets;
    }

    bool error = receiveErrors(stream);

    size_t nfields = 0;
    stream >> nfields;

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
    stream << nRequests;
    for (auto& req : requests) {
        stream << req;
    }

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive response

    bool error = receiveErrors(stream);

    for (size_t i = 0; i < nRequests; i++) {
        std::vector<std::unique_ptr<ExtractionResult>> response;
        size_t nfields;
        stream >> nfields;
        ASSERT(nfields == 1);  // temporary. Note will have to update remote protocol if we wish to not send this. Not
                               // really a problem though.
        result.push_back(std::make_unique<ExtractionResult>(stream));
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

    size_t nFiles = filemap.size();
    stream << nFiles;

    for (auto& [fname, extractionItems] : filemap) {
        // we will send (and receive) the extraction items in order of offset
        std::sort(extractionItems.begin(), extractionItems.end(),
                  [](const ExtractionItem* a, const ExtractionItem* b) { return a->offset() < b->offset(); });

        stream << fname;
        size_t nItems = extractionItems.size();
        stream << nItems;
        for (auto& item : extractionItems) {
            // We have URI, no need to send a request string
            ExtractionRequest req("", item->intervals(), item->gridHash());
            stream << req;
            stream << item->URI();
        }
    }

    timer.report("Request sent");
    bool error = receiveErrors(stream);

    // receive results
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        stream >> fname;
        size_t nItems;
        stream >> nItems;
        ASSERT(nItems == filemap[fname].size());
        for (size_t j = 0; j < nItems; j++) {
            filemap[fname][j]->result(std::make_unique<ExtractionResult>(stream));
        }
    }

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
    stream << request;
    stream << level;
    timer.report("Request sent");

    // receive response

    bool error = receiveErrors(stream);

    size_t nAxes;
    stream >> nAxes;
    for (size_t i = 0; i < nAxes; i++) {
        std::string axisName;
        stream >> axisName;
        size_t nVals;
        stream >> nVals;
        std::unordered_set<std::string> vals;
        for (size_t j = 0; j < nVals; j++) {
            std::string val;
            stream >> val;
            vals.insert(val);
        }
        result[axisName] = vals;
    }
    timer.report("Axes received");

    return result;
}

bool RemoteGribJump::receiveErrors(eckit::Stream& stream, bool raise) {
    size_t nErrors;
    stream >> nErrors;
    if (nErrors == 0) {
        return false;
    }

    std::stringstream ss;
    ss << "RemoteGribJump received server-side " << eckit::Plural(nErrors, "error") << std::endl;
    for (size_t i = 0; i < nErrors; i++) {
        std::string error;
        stream >> error;
        ss << error << std::endl;
    }
    if (raise) {
        throw eckit::RemoteException(ss.str(), Here());
    }
    else {
        eckit::Log::error() << ss.str() << std::endl;
    }
    return true;
}

static GribJumpBuilder<RemoteGribJump> builder("remote");

}  // namespace gribjump
