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
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/container/Queue.h"
#include "eckit/log/Timer.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribJump.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LocalGribJump.h"
namespace gribjump {

LocalGribJump::LocalGribJump(const Config& config): GribJumpBase(config) {

    std::string cacheDir;
    if (!config.get("cache", cacheDir)) {
        LOG_DEBUG_LIB(LibGribJump) << "GribJump not using cache" << std::endl;
        return;
    }

    LOG_DEBUG_LIB(LibGribJump) << "GribJump is using cache" << std::endl;
    cache_ = GribInfoCache(cacheDir);
    cacheEnabled_ = true;

}

LocalGribJump::~LocalGribJump() {}

std::vector<std::vector<ExtractionResult>> LocalGribJump::extract(std::vector<ExtractionRequest> polyRequest){
    eckit::Log::info() << "GribJump::extract() [batch] called" << std::endl;
    eckit::Timer timer;
    timer.stop();

    fdb5::FDB fdb;
    std::vector<std::vector<JumpInfo>> infos;
    std::vector<std::vector<eckit::DataHandle*>> handles;

    for (auto& req : polyRequest){

        timer.start();
        fdb5::ListIterator it = fdb.inspect(req.getRequest());
        timer.stop();
        stats_.addInspect(timer);

        fdb5::ListElement el;
        infos.push_back(std::vector<JumpInfo>());
        handles.push_back(std::vector<eckit::DataHandle*>());
        while (it.next(el)) {
            const fdb5::FieldLocation& loc = el.location();
            timer.start();
            JumpInfo info = extractInfo(loc);
            timer.stop();
            stats_.addInfo(timer);
            infos.back().push_back(info);
            handles.back().push_back(loc.dataHandle());
        }
    }

    // Extract data from each handle
    std::vector<std::vector<ExtractionResult>> result;
    for (size_t i = 0; i < polyRequest.size(); i++) {
        result.push_back(std::vector<ExtractionResult>());
        for (size_t j = 0; j < infos[i].size(); j++) {
            timer.start();
            result.back().push_back(directJump(handles[i][j], polyRequest[i].getRanges(), infos[i][j]));
            timer.stop();
            stats_.addExtract(timer);
        }
    }

    stats_.report(eckit::Log::debug<LibGribJump>(), "Extraction stats: ");
    return result;
}

std::vector<ExtractionResult> LocalGribJump::extract(const metkit::mars::MarsRequest request, const std::vector<std::pair<size_t, size_t>> ranges){
    std::vector<ExtractionResult>  result;
    fdb5::FDB fdb;
    eckit::Timer timer;
    fdb5::ListIterator it = fdb.inspect(request);
    timer.stop();
    stats_.addInspect(timer);
    fdb5::ListElement el;
    while (it.next(el)) {

        const fdb5::FieldLocation& loc = el.location(); // Use the location or uri to check if cached.
        
        timer.start();
        JumpInfo info = extractInfo(loc);
        timer.stop();
        stats_.addInfo(timer);

        timer.start();
        ExtractionResult v = directJump(loc.dataHandle(), ranges, info);
        timer.stop();
        stats_.addExtract(timer);

        result.push_back(v);
    }

    return result;
}

// TODO(Chris) : We can probably group requests by file, based on fdb.inspect fieldlocations

ExtractionResult LocalGribJump::directJump(eckit::DataHandle* handle,
    std::vector<std::pair<size_t, size_t>> ranges,
    JumpInfo info) const {
    JumpHandle dataSource(handle);
    info.setStartOffset(0); // Message starts at the beginning of the handle
    ASSERT(info.ready());
    return info.extractRanges(dataSource, ranges);
}

JumpInfo LocalGribJump::extractInfo(const fdb5::FieldLocation& loc) {
    if (cacheEnabled_) {
        if(cache_.contains(loc)) return cache_.get(loc);
        eckit::Log::debug<LibGribJump>() << "GribJump::extractInfo() cache miss for file " << loc.uri().path().baseName() << std::endl;
    }

    eckit::DataHandle* handle = loc.dataHandle();
    JumpHandle dataSource(handle);
    return dataSource.extractInfo();
}

std::map<std::string, std::unordered_set<std::string>> LocalGribJump::axes(const std::string& request) {
    // bare bones implementation: jut a wrapper around list.
    // TODO(Chris): implement a proper axes function inside FDB.

    // Note: This is likely to be removed from GribJump, and moved to FDB.
    // Here for now to support polytope.

    using namespace fdb5;

    FDB fdb;
    std::vector<FDBToolRequest> requests = FDBToolRequest::requestsFromString(request, std::vector<std::string>(), true);
    ASSERT(requests.size() == 1);
    auto listIter = fdb.list(requests.front(), false);

    std::map<std::string, std::unordered_set<std::string>> values;

    ListElement elem;
    while (listIter.next(elem)) {
        for (const auto& key : elem.key()) {
            for (const auto& param : key) {
                values[param.first].insert(param.second);
            }
        }
    }
    return values;
}

static GribJumpBuilder<LocalGribJump> builder("localgribjump");
} // namespace gribjump
