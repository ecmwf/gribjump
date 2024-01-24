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

#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"

#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/GribJumpFactory.h"

namespace gribjump {

RemoteGribJump::RemoteGribJump(const Config& config): GribJumpBase(config){
    if (!config.get("host", host_))
        throw eckit::UserError("RemoteGribJump requires host to be set in config", Here());
    
    if (!config.get("port", port_))
        throw eckit::UserError("RemoteGribJump requires port to be set in config", Here());
}

RemoteGribJump::~RemoteGribJump() {}

size_t RemoteGribJump::scan(const eckit::PathName& path) {
    NOTIMP;
}

size_t RemoteGribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) {
    eckit::Timer timer("RemoteGribJump::scan()");

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    stream << "SCAN";
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

std::vector<std::vector<ExtractionResult>> RemoteGribJump::extract(std::vector<ExtractionRequest> polyRequest) {
    eckit::Timer timer("RemoteGribJump::extract()");
    std::vector<std::vector<ExtractionResult>> result;

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    stream << "EXTRACT";

    size_t nRequests = polyRequest.size();
    stream << nRequests;
    for (auto& req : polyRequest) {
        stream << req;
    }

    std::stringstream ss;
    ss << "Sent " << nRequests << " requests";
    timer.report(ss.str());

    // receive response

    bool error = receiveErrors(stream);

    for (size_t i = 0; i < nRequests; i++) {
        std::vector<ExtractionResult> response;
        size_t nfields;
        stream >> nfields;
        for (size_t i = 0; i < nfields; i++) {
            ExtractionResult output(stream);
            response.push_back(output);
        }
        result.push_back(response);
    }
    timer.report("All data recieved");
    return result;
}

std::vector<ExtractionResult> RemoteGribJump::extract(const std::vector<eckit::URI> uris, const std::vector<Range> ranges) {
    NOTIMP;
}

std::vector<ExtractionResult> RemoteGribJump::extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) {
    NOTIMP;
}

std::map<std::string, std::unordered_set<std::string>> RemoteGribJump::axes(const std::string& request) {
    eckit::Timer timer("RemoteGribJump::axes()");
    std::map<std::string, std::unordered_set<std::string>> result;   

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    timer.report("Connection established");

    stream << "AXES";
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

bool RemoteGribJump::receiveErrors(eckit::Stream& stream) {
    size_t nErrors;
    stream >> nErrors;
    for (size_t i = 0; i < nErrors; i++) {
        std::string error;
        stream >> error;
        eckit::Log::error() << error << std::endl;
    }
    return nErrors > 0;
}

static GribJumpBuilder<RemoteGribJump> builder("remote");
    
} // namespace gribjump