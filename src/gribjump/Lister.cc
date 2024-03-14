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

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"

#include "gribjump/GribJump.h"
#include "gribjump/Lister.h"

namespace gribjump {
    
//  ------------------------------------------------------------------

Lister::Lister() {
}

Lister::~Lister() {
}

//  ------------------------------------------------------------------

FDBLister& FDBLister::instance() {
    static FDBLister instance;
    return instance;
}

FDBLister::FDBLister() {
}

FDBLister::~FDBLister() {
}

std::vector<eckit::URI> FDBLister::list(const std::vector<metkit::mars::MarsRequest> requests) {

    eckit::AutoLock<FDBLister> lock(this);

    std::vector<eckit::URI> uris;
    for (auto& request : requests) {

        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb_.list(fdbreq, true);

        fdb5::ListElement elem;
        while (listIter.next(elem)) {
            uris.push_back(elem.location().uri());
        }
    }

    return uris;
}


std::string fdbkeyToStr(const fdb5::Key& key) {
    std::stringstream ss;
    std::string separator = "";
    std::set<std::string> keys = key.keys();
    for(const auto& k : keys) {
        ss << separator << k << "=" << key.get(k);
        separator = ",";
    }
    return ss.str();
}

// i.e. do all of the listing work I want...
filemap_t FDBLister::fileMap(const metkit::mars::MarsRequest& unionRequest, const reqToXRR_t& reqToXRR) {

    eckit::AutoLock<FDBLister> lock(this);
    filemap_t filemap;

    fdb5::FDBToolRequest fdbreq(unionRequest);
    auto listIter = fdb_.list(fdbreq, true);

    size_t count = 0;
    
    fdb5::ListElement elem;
    while (listIter.next(elem)) {

        std::string key = fdbkeyToStr(elem.combinedKey());

        // If key not in map, not related to the request
        if (reqToXRR.find(key) == reqToXRR.end()) continue;

        // Set the URI in the ExtractionItem
        eckit::URI uri = elem.location().uri();
        ExtractionItem* xrr = reqToXRR.at(key);
        xrr->URI(new eckit::URI(uri));

        // Add to filemap
        eckit::PathName fname = uri.path();
        auto it = filemap.find(fname);
        if(it == filemap.end()) {
            std::vector<ExtractionItem*> xrrs;
            xrrs.push_back(xrr);
            filemap.emplace(fname, xrrs);
        }
        else {
            it->second.push_back(xrr);
        }

        count++;
    }

    LOG_DEBUG_LIB(LibGribJump) << "Found " << count << " fields in " << filemap.size() << " files" << std::endl;

    if (count != reqToXRR.size()) {
        eckit::Log::warning() << "Number of fields found (" << count << ") does not match Number of keys in reqToXRR (" << reqToXRR.size() << ")" << std::endl;
    }
    
    return filemap;
}

std::map<std::string, std::unordered_set<std::string> > FDBLister::axes(const fdb5::FDBToolRequest& request) {
    
    eckit::AutoLock<FDBLister> lock(this);

    bool DEBUG_USE_FDBAXES = eckit::Resource<bool>("$GRIBJUMP_USE_FDBAXES", true);

    std::map<std::string, std::unordered_set<std::string>> values;

    if (DEBUG_USE_FDBAXES) {
        
        LOG_DEBUG_LIB(LibGribJump) << "Using FDB's (new) axes impl" << std::endl;
        
        fdb5::IndexAxis ax = fdb_.axes(request);
        values = ax.copyAxesMap();
    }
    else {

        LOG_DEBUG_LIB(LibGribJump) << "Using GribJump's (old) axes impl" << std::endl;

        auto listIter = fdb_.list(request, true);
        fdb5::ListElement elem;
        while (listIter.next(elem)) {
            for (const auto& key : elem.key()) {
                for (const auto& param : key) {
                    values[param.first].insert(param.second);
                }
            }
        }
    }

    return values;
}

//  ------------------------------------------------------------------

#if 0
// map (file : offsets)
std::map< eckit::PathName, eckit::OffsetList > FDBLister::list(const std::vector<metkit::mars::MarsRequest> requests) {

    eckit::AutoLock<FDBLister> lock(this);

    std::map< eckit::PathName, eckit::OffsetList > files; 

    for (auto& request : requests) {

        size_t count = request.count(); // worse case scenario all fields in one file

        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb_.list(fdbreq, true);

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
}
#endif

} // namespace gribjump
