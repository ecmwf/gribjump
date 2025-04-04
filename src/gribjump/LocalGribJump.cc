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

#include <chrono>
#include <set>

#include "eckit/config/Resource.h"
#include "eckit/container/Queue.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/thread/AutoLock.h"

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

LocalGribJump::LocalGribJump(const Config& config) : GribJumpBase(config) {}

LocalGribJump::~LocalGribJump() {}

size_t LocalGribJump::scan(const std::vector<eckit::PathName>& paths) {
    auto [result, report] = Engine().scan(paths);
    report.raiseErrors();
    return result;
}

size_t LocalGribJump::scan(const std::vector<MarsRequest>& requests, bool byfiles) {
    auto [result, report] = Engine().scan(requests, byfiles);
    report.raiseErrors();
    return result;
}

std::vector<std::unique_ptr<ExtractionItem>> LocalGribJump::extract(const eckit::PathName& path,
                                                                    const std::vector<eckit::Offset>& offsets,
                                                                    const std::vector<std::vector<Range>>& ranges) {
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
std::vector<std::vector<std::unique_ptr<ExtractionResult>>> LocalGribJump::extract(ExtractionRequests& requests) {

    auto [results, report] = Engine().extract(requests);
    report.raiseErrors();

    std::vector<std::vector<std::unique_ptr<ExtractionResult>>> extractionResults;
    for (auto& req : requests) {
        auto it = results.find(req.requestString());

        ASSERT(it != results.end());
        std::vector<std::unique_ptr<ExtractionResult>> res;
        for (auto& item : it->second) {
            res.push_back(std::make_unique<ExtractionResult>(item->values(), item->mask()));
        }

        extractionResults.push_back(std::move(res));
    }

    return extractionResults;
}

ResultsMap LocalGribJump::extract(const std::vector<std::string>& requests,
                                  const std::vector<std::vector<Range>>& ranges) {
    ExtractionRequests extractionRequests;

    for (size_t i = 0; i < requests.size(); i++) {
        extractionRequests.push_back(ExtractionRequest(requests[i], ranges[i]));
    }

    auto [results, report] = Engine().extract(extractionRequests);
    report.raiseErrors();
    return std::move(results);
}

std::map<std::string, std::unordered_set<std::string>> LocalGribJump::axes(const std::string& request, int level) {
    return Engine().axes(request, level);
}

static GribJumpBuilder<LocalGribJump> builder("local");

}  // namespace gribjump
