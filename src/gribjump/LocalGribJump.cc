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

#include "gribjump/GribHandleData.h"
#include "gribjump/GribJump.h"
#include "gribjump/FDBService.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LocalGribJump.h"

namespace gribjump {



typedef std::chrono::high_resolution_clock Clock;

static std::string thread_id_str() {
    auto id = std::this_thread::get_id();
    std::stringstream ss;
    ss << id;
    return ss.str();
}

LocalGribJump::LocalGribJump(const Config& config): GribJumpBase(config) {
}

LocalGribJump::~LocalGribJump() {}

size_t LocalGribJump::scan(const eckit::PathName& path) {
    JumpHandle dataSource(path);
    std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
    GribInfoCache::instance().insert(path, infos);
    return infos.size();
}

size_t LocalGribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) {

    // note that the order in the map is not guaranteed to be the same as the order of the requests
    std::map< eckit::PathName, eckit::OffsetList > files = FDBService::instance().filesOffsets(requests);

    for (const auto& file : files) {
        if(byfiles) {
            scan(file.first);
        }
        else {
            GribInfoCache::instance().scan(file.first, file.second);
        }
    }

    size_t numFiles = files.size();
    LOG_DEBUG_LIB(LibGribJump) << "Found " << numFiles << " files" << std::endl;

    return numFiles;
}

std::vector<ExtractionResult*> LocalGribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges){

    bool useSharedHandle = eckit::Resource<bool>("$GRIBJUMP_SHARED_HANDLE", true);

    if (useSharedHandle){
        LOG_DEBUG_LIB(LibGribJump) << "Using shared handle" << std::endl;
        std::vector<JumpInfoHandle> infos;
        for (size_t i = 0; i < offsets.size(); i++) {
            infos.push_back(extractInfo(path, offsets[i]));
        }

        std::vector<ExtractionResult*> results = directJumpSharedHandle(path, offsets, ranges, infos);
        return results;
    }

    LOG_DEBUG_LIB(LibGribJump) << "NOT using shared handle" << std::endl;

    eckit::Timer t, t2;

    std::vector<ExtractionResult*> results;

    for (size_t i = 0; i < offsets.size(); i++) {
        JumpInfoHandle info = extractInfo(path, offsets[i]);
        
        t.reset("extractInfo");
        
        // TODO: directjump that takes multiple offsets which uses one handle and seeks accordingly.
        results.emplace_back(directJump(path, offsets[i], ranges[i], info));

        t.reset("directJump");
        t2.reset("total");
    }

    return results;
}

std::vector<std::vector<ExtractionResult>> LocalGribJump::extract(std::vector<ExtractionRequest> polyRequest){
    eckit::Log::info() << "GribJump::extract() [batch] called" << std::endl;
    eckit::Timer timer;
    timer.stop();

    eckit::AutoLock<FDBService> lock(FDBService::instance());
    fdb5::FDB& fdb = FDBService::instance().fdb();

    std::vector<std::vector<JumpInfoHandle>> infos;
    std::vector<std::vector<eckit::DataHandle*>> handles;

    for (auto& req : polyRequest){

        timer.start();
        fdb5::ListIterator it = fdb.inspect(req.getRequest());
        timer.stop();
        stats_.addInspect(timer);

        fdb5::ListElement el;
        infos.push_back(std::vector<JumpInfoHandle>());
        handles.push_back(std::vector<eckit::DataHandle*>());
        while (it.next(el)) {
            const fdb5::FieldLocation& loc = el.location();
            timer.start();
            JumpInfoHandle info = extractInfo(loc);
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

ExtractionResult LocalGribJump::directJump(eckit::DataHandle* handle, const std::vector<Range> ranges, JumpInfoHandle info) const {
    JumpHandle dataSource(handle);
    // XXX: We shouldn't allow modification of jumpinfo.
    info->setStartOffset(0); // Message starts at the beginning of the handle.
    ASSERT(info->ready());
    return info->extractRanges(dataSource, ranges);
}

ExtractionResult* LocalGribJump::directJump(eckit::PathName path, const eckit::Offset offset, const std::vector<Range> ranges, JumpInfoHandle info) const {
    eckit::Length length = info->length();

    eckit::DataHandle* handle = path.partHandle(offset, length); // because we currently require always being at start of message...

    JumpHandle dataSource(handle);

    auto t0 = Clock::now();

    info->setStartOffset(0); // Message starts at the beginning of the handle.
    ASSERT(info->ready());

    ExtractionResult* ret = info->newExtractRanges(dataSource, ranges);

    auto t1 = Clock::now();

    std::chrono::duration<double> delta3 = t1 - t0;

    std::cout << "Thread[" << thread_id_str() << "] offset " << offset << " extract time " << delta3.count() << std::endl;

    return ret;
}

std::vector<ExtractionResult*> LocalGribJump::directJumpSharedHandle(const eckit::PathName path, const std::vector<eckit::Offset> offsets, const std::vector<std::vector<Range>> ranges, std::vector<JumpInfoHandle> infos) const {
    std::vector<eckit::Length> lengths;
    for (auto& info : infos) {
        lengths.push_back(info->length());
    }
    eckit::DataHandle* handle = path.partHandle(offsets, lengths);

    // Set start offset to end of previous message.
    eckit::Offset prevEnd = 0;
    for (size_t i = 0; i < infos.size(); i++) {
        infos[i]->setStartOffset(prevEnd);
        prevEnd += infos[i]->length();
    }

    JumpHandle dataSource(handle);

    std::vector<ExtractionResult*> results;
    for (size_t i = 0; i < infos.size(); i++) {
        ASSERT(infos[i]->ready());
        results.push_back(infos[i]->newExtractRanges(dataSource, ranges[i]));
    }
    return results;
}

JumpInfoHandle LocalGribJump::extractInfo(const fdb5::FieldLocation& loc) {
    // TODO: Assuming it works correctly, can call extractInfo(loc.uri(), loc.offset()) instead.
    
    GribInfoCache& cache = GribInfoCache::instance();

    JumpInfo* pinfo = cache.get(loc);
    if (pinfo) return JumpInfoHandle(pinfo);

    std::string f = loc.uri().path().baseName();
    eckit::Log::info() << "GribJump::extractInfo() cache miss for file " << f << std::endl;

    eckit::DataHandle* handle = loc.dataHandle();
    JumpHandle dataSource(handle);
    JumpInfo* info = dataSource.extractInfo();
    cache.insert(loc, info); // takes ownership of info
    return JumpInfoHandle(info);
}

JumpInfoHandle LocalGribJump::extractInfo(const eckit::PathName& path, const eckit::Offset& offset) {
    
    GribInfoCache& cache = GribInfoCache::instance();

    JumpInfo* pinfo = cache.get(path, offset);
    if (pinfo) return JumpInfoHandle(pinfo);

    std::string f = path.baseName();
    eckit::Log::info() << "GribJump cache miss file=" << f << ",offset=" << offset << std::endl;

    eckit::DataHandle* handle = path.fileHandle();
    JumpHandle dataSource(handle);
    dataSource.seek(offset); // !!!
    JumpInfo* info = dataSource.extractInfo();
    cache.insert(path, offset, info); // takes ownership of info
    return JumpInfoHandle(info);
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
