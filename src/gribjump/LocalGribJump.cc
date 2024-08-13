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

#include "gribjump/GribJump.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LocalGribJump.h"
#include "gribjump/info/InfoCache.h"

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
    // InfoCache::instance().insert(path, infos);
    // return infos.size();
}

size_t LocalGribJump::scan(const std::vector<MarsRequest> requests, bool byfiles) {
    Engine engine;
    return engine.scan(requests, byfiles);
}


std::vector<std::unique_ptr<ExtractionItem>> LocalGribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) {
    // Directly from file, no cache, no queue, no threads

    InfoExtractor extractor;
    std::vector<std::unique_ptr<JumpInfo>> infos = extractor.extract(path, offsets);

    eckit::FileHandle fh(path);
    fh.openForRead();

    std::vector<std::unique_ptr<ExtractionItem>> results;

    for (size_t i = 0; i < offsets.size(); i++) {
        JumpInfo& info = *infos[i];
        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info));
        std::unique_ptr<ExtractionItem> item(new ExtractionItem(ranges[i]));
        jumper->extract(fh, offsets[i], info, *item);
        results.push_back(std::move(item));
    }

    fh.close();

    return results;
}

/// @todo, change API, remove extraction request
std::vector<std::vector<ExtractionResult*>> LocalGribJump::extract(std::vector<ExtractionRequest> polyRequest) {

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

std::map<std::string, std::unordered_set<std::string>> LocalGribJump::axes(const std::string& request) {

    // Note: This is likely to be removed from GribJump, and moved to FDB.
    // Here for now to support polytope.

    Engine engine;
    return engine.axes(request);
}

// TODO: remove these, plugin should use aggregator directly (which has its own config).
void LocalGribJump::aggregate(const fdb5::Key& key, const eckit::URI& location) {
    NOTIMP;
};

void LocalGribJump::aggregate(const fdb5::Key& key, const eckit::message::Message& msg) {
    NOTIMP;
};


static GribJumpBuilder<LocalGribJump> builder("local");

} // namespace gribjump
