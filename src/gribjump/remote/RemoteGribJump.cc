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
#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"

#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/GribJumpFactory.h"

namespace gribjump {

RemoteGribJump::RemoteGribJump(const Config& config): GribJumpBase(config){
    std::string uri = config.getString("uri", "");
    
    if (uri.empty())
        throw eckit::UserError("RemoteGribJump requires uri to be set in config (format host:port)", Here());

    eckit::net::Endpoint endpoint(uri);
    host_ = endpoint.host();
    port_ = endpoint.port();
}

RemoteGribJump::RemoteGribJump(eckit::net::Endpoint endpoint): host_(endpoint.host()), port_(endpoint.port()) {}

RemoteGribJump::~RemoteGribJump() {}

size_t RemoteGribJump::scan(const eckit::PathName& path) {
    NOTIMP;
}

void RemoteGribJump::sendHeader(eckit::net::InstantTCPStream& stream, RequestType type) {
    stream << remoteProtocolVersion;
    stream << ctx_;
    stream << static_cast<uint16_t>(type);
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()");

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

    size_t count = 0;
    for (size_t i = 0; i < nRequests; i++) {
        size_t nfiles;
        stream >> nfiles;
        eckit::Log::info() << "Scanned " << eckit::Plural(nfiles, "file") << std::endl;
        count += nfiles;
    }

    timer.report("Scans complete");
    return count;
}

std::vector<std::vector<ExtractionResult*>> RemoteGribJump::extract(std::vector<ExtractionRequest> requests, LogContext ctx) {
    eckit::Timer timer("RemoteGribJump::extract()");
    std::vector<std::vector<ExtractionResult*>> result;

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
        std::vector<ExtractionResult*> response;
        size_t nfields;
        stream >> nfields;
        for (size_t i = 0; i < nfields; i++) {
            response.push_back(new ExtractionResult(stream));
        }
        result.push_back(response);
    }
    timer.report("All data recieved");
    return result;
}

std::vector<std::unique_ptr<ExtractionItem>> RemoteGribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) {
    NOTIMP;
}

void RemoteGribJump::extract(filemap_t& filemap){
    eckit::Timer timer("RemoteGribJump::extract()");

    ///@todo we could probably do the connection logic in the ctor
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    sendHeader(stream, RequestType::FORWARD_EXTRACT);

    size_t nFiles = filemap.size();
    stream << nFiles;

    for (auto& [fname, extractionItems] : filemap) {
        // we will send (and receive) the extraction items in order of offset
        std::sort(extractionItems.begin(), extractionItems.end(), [](const ExtractionItem* a, const ExtractionItem* b) {
            return a->offset() < b->offset();
        });

        stream << fname;
        size_t nItems = extractionItems.size();
        stream << nItems;
        for (auto& item : extractionItems) {
            // ExtractionRequest req(item->request(), item->intervals());
            metkit::mars::MarsRequest r(""); // no need to send mars request when we have uri
            ExtractionRequest req(r, item->intervals());
            stream << req;
            stream << item->URI();
        }
    }

    timer.report("Request sent");
    bool error = receiveErrors(stream);

    // receive results
    for (size_t i=0; i<nFiles; i++) {
        std::string fname;
        stream >> fname;
        size_t nItems;
        stream >> nItems;
        ASSERT(nItems == filemap[fname].size());
        for (size_t j=0; j<nItems; j++) {
            ExtractionResult res(stream);

            filemap[fname][j]->values(res.values());
            filemap[fname][j]->mask(res.mask());
        }
    }

    timer.report("Results received");

    return;
}

std::map<std::string, std::unordered_set<std::string>> RemoteGribJump::axes(const std::string& request) {
    eckit::Timer timer("RemoteGribJump::axes()");
    std::map<std::string, std::unordered_set<std::string>> result;   

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    sendHeader(stream, RequestType::AXES);
    stream << request;  
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
    } else {
        eckit::Log::error() << ss.str() << std::endl;
    }
    return true;
}

static GribJumpBuilder<RemoteGribJump> builder("remote");
    
} // namespace gribjump
