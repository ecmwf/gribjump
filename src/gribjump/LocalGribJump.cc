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

namespace gribjump {

typedef std::chrono::high_resolution_clock Clock;

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
    const std::map< eckit::PathName, eckit::OffsetList > files = FDBService::instance().filesOffsets(requests);

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

    std::vector<JumpInfoHandle> infos;
    for (size_t i = 0; i < offsets.size(); i++) {
        infos.push_back(extractInfo(path, offsets[i]));
        ASSERT(offsets[i] == infos[i]->offset()); 
    }

    eckit::DataHandle* handle = path.fileHandle();
    
    JumpHandle dataSource(handle);

    std::vector<ExtractionResult*> results;
    for (size_t i = 0; i < infos.size(); i++) {
        ASSERT(infos[i]->ready());
        results.push_back(infos[i]->newExtractRanges(dataSource, ranges[i]));
    }

    return results;
}

std::vector<std::vector<ExtractionResult>> LocalGribJump::extract(std::vector<ExtractionRequest> polyRequest){
    // Old API
    // TODO: use pointers to ExtractionResult.

    // TODO: Remove this function, or significantly change it, such that the server and local implementations call the same functions.

    std::vector<std::vector<JumpInfoHandle>> infos;
    std::vector<std::vector<eckit::DataHandle*>> handles;
    
    { // Get handles
    
        // eckit::AutoLock<FDBService> lock(FDBService::instance()); 
        // fdb5::FDB& fdb = FDBService::instance().fdb(); // NB: A static FDB cannot be destroyed correctly if inspect is ever called...

        // This function is for single-threaded use only.
        fdb5::FDB fdb;
        

        for (auto& req : polyRequest){

            fdb5::ListIterator it = fdb.inspect(req.getRequest());

            fdb5::ListElement el;
            infos.push_back(std::vector<JumpInfoHandle>());
            handles.push_back(std::vector<eckit::DataHandle*>());
            while (it.next(el)) {
                const fdb5::FieldLocation& loc = el.location();
                const JumpInfoHandle info = extractInfo(loc);
                infos.back().push_back(info);
                
                handles.back().push_back(loc.uri().path().fileHandle());
            }
        }
    
    }

    // Extract values
    std::vector<std::vector<ExtractionResult>> result;
    
    for (size_t i = 0; i < polyRequest.size(); i++) {
        result.push_back(std::vector<ExtractionResult>());
        for (size_t j = 0; j < infos[i].size(); j++) {
            JumpHandle dataSource(handles[i][j]);
            result.back().push_back(infos[i][j]->extractRanges(dataSource, polyRequest[i].getRanges()));
        }
    }

    return result;
}

JumpInfoHandle LocalGribJump::extractInfo(const fdb5::FieldLocation& loc) {
    return extractInfo(loc.uri().path(), loc.offset());
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
