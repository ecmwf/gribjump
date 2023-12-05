/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"

#include "fdb5/api/FDB.h"



namespace gribjump {

GribJump::GribJump() {}

GribJump::~GribJump() {}


PolyOutput polyOutputFromStream(eckit::Stream& s){
    std::vector<std::vector<double>> values;
    s >> values;
    std::vector<std::vector<std::string>> bitsetStrings;
    s >> bitsetStrings;
    std::vector<std::vector<std::bitset<64>>> bitsets;
    for (auto& v : bitsetStrings) {
        std::vector<std::bitset<64>> bitset;
        for (auto& b : v) {
            bitset.push_back(std::bitset<64>(b));
        }
        bitsets.push_back(bitset);
    }
    return std::make_tuple(values, bitsets);
}

std::vector<std::vector<PolyOutput>> GribJump::extractRemote(PolyRequest polyRequest){
    // XXX
    // Placeholder function for remote forwarding of requests.
    // We probably want to use a factory to create a RemoteGribJump, and override the extract() function.
    // TODO, after config is implemented.
    // For now, rely on env vars.
    std::vector<std::vector<PolyOutput>> result;

    eckit::Log::info() << "GribJump::extractRemote() called" << std::endl;
    std::string host = getenv("GRIBJUMP_REMOTE_HOST");
    int port = atoi(getenv("GRIBJUMP_REMOTE_PORT"));

    // connect to server
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host, port));
    eckit::Log::info() << "connected" << std::endl;

    stream << "EXTRACT";
    eckit::Log::info() << "sent: EXTRACT" << std::endl;

    // stream << polyRequest; // <-- Would be nice to just do this.
    stream << polyRequest.size();
    for (auto& [request, ranges] : polyRequest) {
        stream << request;
        stream << ranges.size();
        for (auto& [start, end] : ranges) {
            stream << start << end;
        }
        
        eckit::Log::info() << "sent request" << std::endl;

        // receive response
        std::vector<PolyOutput> response;
        size_t nfields;
        stream >> nfields;
        eckit::Log::info() << "expecting " << nfields << " fields" << std::endl;
        for (size_t i = 0; i < nfields; i++) {
            PolyOutput output = polyOutputFromStream(stream);
            response.push_back(output);
        }
        result.push_back(response);
    }

    return result;
}

std::vector<std::vector<PolyOutput>> GribJump::extract(PolyRequest polyRequest){
    // XXX TEMP: check for GRIBJUMP_REMOTE, and if so call extractRemote()
    if (getenv("GRIBJUMP_REMOTE") != NULL) {
        return extractRemote(polyRequest);
    }
    
    std::vector<std::vector<PolyOutput>> result;

    for (auto& [request, ranges] : polyRequest) {
        result.push_back(extract(request, ranges));
    }
    return result;
}

std::vector<PolyOutput> GribJump::extract(const metkit::mars::MarsRequest request, const std::vector<std::tuple<size_t, size_t>> ranges){

    const GribJump gj;
    std::vector<PolyOutput>  result;
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

        PolyOutput v = gj.directJump(loc.dataHandle(), ranges, info);
        result.push_back(v);
    }

    return result;
}


// TODO : We can probably group requests by file, based on fdb.inspect fieldlocations
// 

PolyOutput GribJump::directJump(eckit::DataHandle* handle,
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


// std::vector<std::vector<metkit::gribjump::PolyOutput>> FDB::extract(const PolyRequest& polyRequest) {
//     using namespace metkit::gribjump;
    
//     std::vector<std::vector<PolyOutput>> result;

//     for (auto& [request, ranges] : polyRequest) {
//         result.push_back(extract(request, ranges));
//     }
//     return result;
// }

// std::vector<metkit::gribjump::PolyOutput> FDB::extract(const metkit::mars::MarsRequest request, const std::vector<std::tuple<size_t, size_t>> ranges){
//     using namespace metkit::gribjump;

//     const GribJump gj;
//     std::vector<PolyOutput>  result;

//     ListIterator it = inspect(request);
//     ListElement el;
//     while (it.next(el)) {

//         Key key = el.combinedKey();
//         if(gj.isCached(key)){
//             // todo ...
//         }

//         const FieldLocation& loc = el.location();
//         JumpInfo info = gj.extractInfo(loc.dataHandle());

//         eckit::Log::debug<LibFdb5>() << "FDB::extract() key: " << key 
//             << ", location: " << loc << ", info: " << info << std::endl;

//         PolyOutput v = gj.directJump(loc.dataHandle(), ranges, info);
//         result.push_back(v);
//     }

//     return result;
// }


} // namespace GribJump