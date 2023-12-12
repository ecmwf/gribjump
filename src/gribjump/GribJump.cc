/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "eckit/log/Log.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"


namespace gribjump {

GribJump::GribJump() {}

GribJump::~GribJump() {}


std::vector<std::vector<ExtractionResult>> GribJump::extractRemote(std::vector<ExtractionRequest> polyRequest){
    // XXX
    // Placeholder function for remote forwarding of requests.
    // We probably want to use a factory to create a RemoteGribJump, and override the extract() function.
    // TODO, after config is implemented.
    // For now, rely on env vars.
    std::vector<std::vector<ExtractionResult>> result;

    eckit::Log::info() << "GribJump::extractRemote() called" << std::endl;
    std::string host = getenv("GRIBJUMP_REMOTE_HOST");
    int port = atoi(getenv("GRIBJUMP_REMOTE_PORT"));

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host, port));
    eckit::Log::info() << "connected" << std::endl;

    stream << "EXTRACT";
    eckit::Log::info() << "sent: EXTRACT" << std::endl;

    stream << polyRequest.size();
    for (auto& req : polyRequest) {
        stream << req;
        
        eckit::Log::info() << "sent request" << std::endl;

        // receive response
        std::vector<ExtractionResult> response;
        size_t nfields;
        stream >> nfields;
        eckit::Log::info() << "expecting " << nfields << " fields" << std::endl;
        for (size_t i = 0; i < nfields; i++) {
            ExtractionResult output(stream);
            response.push_back(output);
        }
        result.push_back(response);
    }

    return result;
}

std::vector<std::vector<ExtractionResult>> GribJump::extract(std::vector<ExtractionRequest> polyRequest){
    // XXX TEMP: check for GRIBJUMP_REMOTE, and if so call extractRemote()
    if (getenv("GRIBJUMP_REMOTE") != NULL) {
        return extractRemote(polyRequest);
    }
    
    std::vector<std::vector<ExtractionResult>> result;
    for (auto& exrequest : polyRequest) {
        result.push_back(extract(exrequest.getRequest(), exrequest.getRanges()));
    }
    return result;
}

std::vector<ExtractionResult> GribJump::extract(const metkit::mars::MarsRequest request, const std::vector<std::tuple<size_t, size_t>> ranges){

    const GribJump gj;
    std::vector<ExtractionResult>  result;
    fdb5::FDB fdb;
    fdb5::ListIterator it = fdb.inspect(request); 
    fdb5::ListElement el;
    while (it.next(el)) {

        fdb5::Key key = el.combinedKey();
        const fdb5::FieldLocation& loc = el.location(); // Use the location or uri to check if cached.
        if(gj.isCached(key)){
            // todo ...
        }

        JumpInfo info = gj.extractInfo(loc.dataHandle());

        eckit::Log::debug<LibGribJump>() << "GribJump::extract() key: " << key 
            << ", location: " << loc << ", info: " << info << std::endl;

        ExtractionResult v = gj.directJump(loc.dataHandle(), ranges, info);

        for (auto& val : v.values()) {
            eckit::Log::debug<LibGribJump>() << "GribJump::extract() value: " << val << std::endl;
        }

        result.push_back(v);
    }

    return result;
}


// TODO : We can probably group requests by file, based on fdb.inspect fieldlocations
// 

ExtractionResult GribJump::directJump(eckit::DataHandle* handle,
    std::vector<std::tuple<size_t, size_t>> ranges,
    JumpInfo info) const {
    JumpHandle dataSource(handle);
    info.setStartOffset(0); // Message starts at the beginning of the handle
    ASSERT(info.ready());
    return info.extractRanges(dataSource, ranges);
}

JumpInfo GribJump::extractInfo(eckit::DataHandle* handle) const {
    JumpHandle dataSource(handle);
    return dataSource.extractInfo();
}

std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const std::string& request) {
// std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const fdb5::FDBToolRequest& request) {
    // bare bones implementation: jut a wrapper around list.
    // TODO: implement a proper axes function inside FDB.

    // Note: This is likely to be removed from GribJump, and moved to FDB.
    // Here for now to support polytope.

    using namespace fdb5;

    FDB fdb;
    std::vector<FDBToolRequest> requests = FDBToolRequest::requestsFromString(request);
    ASSERT(requests.size() == 1);
    auto listIter = fdb.list(requests.front(), false);

    std::map<std::string, std::unordered_set<std::string>> values;

    ListElement elem;
    while (listIter.next(elem)) {
        for (const auto& key: elem.key()) {
            for (const auto& param : key) {
                values[param.first].insert(param.second);
            }
        }
    }
    return values;
}

} // namespace GribJump