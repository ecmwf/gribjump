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

#include <set>
#include <chrono>

#include "eckit/log/Log.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/container/Queue.h"
#include "eckit/log/Timer.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "gribjump/JumpHandle.h"
#include "gribjump/GribJump.h"
#include "gribjump/FDBService.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LocalGribJump.h"
#include "gribjump/GribInfoCache.h"

#include "gribjump/Engine.h"
#include "gribjump/info/InfoExtractor.h"
#include "gribjump/jumper/JumperFactory.h"

namespace gribjump {

using namespace metkit::mars;

typedef std::chrono::high_resolution_clock Clock;

LocalGribJump::LocalGribJump(const Config& config): GribJumpBase(config) {
}

LocalGribJump::~LocalGribJump() {}

size_t LocalGribJump::scan(const eckit::PathName& path) {
    NOTIMP;
    // JumpHandle dataSource(path);
    // std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
    // GribInfoCache::instance().insert(path, infos);
    // return infos.size();
}

size_t LocalGribJump::scan(const std::vector<MarsRequest> requests, bool byfiles) {
    Engine engine;
    return engine.scan(requests, byfiles);
}

std::vector<ExtractionResult*> LocalGribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges){

    // Directly from file, no cache, no queue, no threads

    InfoExtractor extractor;
    std::vector<NewJumpInfo*> infos = extractor.extract(path, offsets);

    eckit::FileHandle fh(path);
    fh.openForRead();

    std::vector<ExtractionResult*> results;

    for (size_t i = 0; i < offsets.size(); i++) {
        NewJumpInfo& info = *infos[i];
        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info));
        ExtractionResult* res = jumper->extract(fh, info, ranges[i]);
        results.push_back(res);
    }

    fh.close();

    return results;
}

/// @todo, change API, remove extraction request
std::vector<std::vector<ExtractionResult*>> LocalGribJump::extract(std::vector<ExtractionRequest> polyRequest){

    std::vector<MarsRequest> requests;
    std::vector<std::vector<Range>> ranges;
    
    bool flatten = true;

    for (auto& req : polyRequest) {
        requests.push_back(req.getRequest());
        ranges.push_back(req.getRanges());
    }

    std::map<MarsRequest, std::vector<ExtractionItem*>> results = extract(requests, ranges, flatten);

    std::vector<std::vector<ExtractionResult*>> extractionResults;
    for (auto& req : polyRequest) {
        auto it = results.find(req.getRequest());
        ASSERT(it != results.end());
        std::vector<ExtractionResult*> res;
        for (auto& item : it->second) {
            ExtractionResult* r = new ExtractionResult(item->values(), item->mask());
            res.push_back(r);
        }

        extractionResults.push_back(res);
    }

    return extractionResults;
}

std::map<MarsRequest, std::vector<ExtractionItem*>> LocalGribJump::extract(const std::vector<MarsRequest>& requests, const std::vector<std::vector<Range>>& ranges, bool flatten) {
    Engine engine;
    std::map<MarsRequest, std::vector<ExtractionItem*>> results = engine.extract(requests, ranges, flatten);
    return results;
}

JumpInfoHandle LocalGribJump::extractInfo(const fdb5::FieldLocation& loc) {
    return extractInfo(loc.uri().path(), loc.offset());
}

JumpInfoHandle LocalGribJump::extractInfo(const eckit::PathName& path, const eckit::Offset& offset) {
    NOTIMP;
    // GribInfoCache& cache = GribInfoCache::instance();

    // JumpInfo* pinfo = cache.get(path, offset);
    // if (pinfo) return JumpInfoHandle(pinfo);

    // std::string f = path.baseName();
    // eckit::Log::info() << "GribJump cache miss file=" << f << ",offset=" << offset << std::endl;

    // eckit::DataHandle* handle = path.fileHandle();
    // JumpHandle dataSource(handle);
    // dataSource.seek(offset); // !!!
    // JumpInfo* info = dataSource.extractInfo();
    // cache.insert(path, offset, info); // takes ownership of info
    // return JumpInfoHandle(info);
}

std::map<std::string, std::unordered_set<std::string>> LocalGribJump::axes(const std::string& request) {
    // bare bones implementation: jut a wrapper around list.
    // TODO(Chris): implement a proper axes function inside FDB.

    // Note: This is likely to be removed from GribJump, and moved to FDB.
    // Here for now to support polytope.

    std::vector<fdb5::FDBToolRequest> requests = fdb5::FDBToolRequest::requestsFromString(request, std::vector<std::string>(), true);
    ASSERT(requests.size() == 1);

    return FDBService::instance().axes(requests.front());
}

static GribJumpBuilder<LocalGribJump> builder("local");

} // namespace gribjump
