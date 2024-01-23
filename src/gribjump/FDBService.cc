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
/// @author Tiago Quintino

#include "gribjump/FDBService.h"

#include "eckit/log/Log.h"

#include "gribjump/GribJump.h"

namespace gribjump {

FDBService& FDBService::instance() {
    static FDBService instance_;
    return instance_;
}

FDBService::FDBService() {
}

FDBService::~FDBService() {
}

std::vector<eckit::URI> FDBService::fieldLocations(const metkit::mars::MarsRequest& request) {
    eckit::AutoLock<FDBService> lock(this);

    std::vector<eckit::URI> locations;

    fdb5::FDBToolRequest fdbreq(request);
    auto listIter = fdb_.list(fdbreq, false);
    fdb5::ListElement elem;
    while (listIter.next(elem)) {
            const fdb5::FieldLocation& loc = elem.location();
            locations.push_back(loc.fullUri());
            LOG_DEBUG_LIB(LibGribJump) << loc << std::endl;
        }
    return locations;
}

std::vector<eckit::PathName> FDBService::listFilesInRequest(std::vector<metkit::mars::MarsRequest> requests) {
        
    eckit::AutoLock<FDBService> lock(this);

    std::set< eckit::PathName > files;

    for (auto& request : requests) {

        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb_.list(fdbreq, false);

        fdb5::ListElement elem;
        eckit::PathName last;
        while (listIter.next(elem)) {
            const fdb5::FieldLocation& loc = elem.location();
            LOG_DEBUG_LIB(LibGribJump) << loc << std::endl;
            eckit::PathName path = loc.uri().path();
            if(path != last) { 
                files.insert(path); // minor optimisation
            }
            last = path;
        }
    }

    std::vector<eckit::PathName> output;
    std::copy(files.begin(), files.end(), std::back_inserter(output));

    return output;
}

std::map< eckit::PathName, eckit::OffsetList > FDBService::filesOffsets(std::vector<metkit::mars::MarsRequest> requests) {

    eckit::AutoLock<FDBService> lock(this);

    std::map< eckit::PathName, eckit::OffsetList > files; 

    for (auto& request : requests) {

        size_t count = request.count(); // worse case scenario all fields in one file

        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb_.list(fdbreq, false);

        fdb5::ListElement elem;
        while (listIter.next(elem)) {
                        
            const fdb5::FieldLocation& loc = elem.location();

            eckit::PathName path = loc.uri().path();

            auto it = files.find(path);
            if(it == files.end()) {
                eckit::OffsetList offsets;
                offsets.reserve(count);
                offsets.push_back(loc.offset());
                files.emplace(path, offsets);
            }
            else {
                it->second.push_back(loc.offset());
            }
        }
    }

    return files;
}

std::map<std::string, std::unordered_set<std::string> > FDBService::axes(const fdb5::FDBToolRequest& request) {

    eckit::AutoLock<FDBService> lock(this);

    auto listIter = fdb_.list(request, false);

    std::map<std::string, std::unordered_set<std::string>> values;

    fdb5::ListElement elem;
    while (listIter.next(elem)) {
        for (const auto& key : elem.key()) {
            for (const auto& param : key) {
                values[param.first].insert(param.second);
            }
        }
    }
    return values;

}


} // namespace gribjump
