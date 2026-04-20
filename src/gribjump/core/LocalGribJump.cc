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

#include "gribjump/core/LocalGribJump.h"

#include <chrono>
#include <utility>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/Offset.h"
#include "gribjump/api/ExtractionData.h"
#include "gribjump/api/ExtractionItem.h"
#include "gribjump/api/GribJumpFactory.h"
#include "gribjump/core/Engine.h"
#include "gribjump/core/Task.h"
#include "gribjump/info/InfoExtractor.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/jumper/Jumper.h"
#include "gribjump/jumper/JumperFactory.h"
#include "metkit/mars/Parameter.h"

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


std::vector<std::unique_ptr<ExtractionResult>> LocalGribJump::extract(const eckit::PathName& path,
                                                                      const std::vector<eckit::Offset>& offsets,
                                                                      const std::vector<std::vector<Range>>& ranges) {
    // Directly from file, no cache, no queue, no threads

    InfoExtractor extractor;
    std::vector<std::unique_ptr<JumpInfo>> infos = extractor.extract(path, offsets);

    eckit::FileHandle fh(path);
    fh.openForRead();

    std::vector<std::unique_ptr<ExtractionResult>> results;

    for (size_t i = 0; i < offsets.size(); i++) {
        JumpInfo& info = *infos[i];
        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info));
        auto item = std::make_unique<ExtractionItem>(ranges[i]);
        jumper->extract(fh, offsets[i], info, *item);
        auto res = item->result();
        results.push_back(std::move(res));
    }

    fh.close();

    return results;
}

std::vector<std::unique_ptr<ExtractionResult>> collect_results(ExtractionRequests& requests, ResultsMap& results) {
    ASSERT(results.size() == requests.size());

    // Map -> Vector
    std::vector<std::unique_ptr<ExtractionResult>> extractionResults;
    extractionResults.reserve(requests.size());
    for (auto& req : requests) {
        auto it = results.find(req.requestString());
        ASSERT(it != results.end());
        auto res = it->second->result();
        ASSERT(res);
        extractionResults.push_back(std::move(res));
    }
    return extractionResults;
}

std::vector<std::unique_ptr<ExtractionResult>> collect_results(PathExtractionRequests& requests, ResultsMap& results) {
    ASSERT(results.size() == requests.size());

    // Map -> Vector
    std::vector<std::unique_ptr<ExtractionResult>> extractionResults;
    extractionResults.reserve(requests.size());
    for (auto& req : requests) {
        auto it = results.find(req.requestString());
        ASSERT(it != results.end());
        auto res = it->second->result();
        ASSERT(res);
        extractionResults.push_back(std::move(res));
    }
    return extractionResults;
}

std::vector<std::unique_ptr<ExtractionResult>> LocalGribJump::extract(ExtractionRequests& requests) {

    auto [results, report] = Engine().extract(requests);
    report.raiseErrors();

    std::vector<std::unique_ptr<ExtractionResult>> extractionResults = collect_results(requests, results);

    return extractionResults;
}

std::vector<std::unique_ptr<ExtractionResult>> LocalGribJump::extract(PathExtractionRequests& requests) {

    auto [results, report] = Engine().extract(requests);
    report.raiseErrors();

    std::vector<std::unique_ptr<ExtractionResult>> extractionResults = collect_results(requests, results);

    return extractionResults;
}

std::map<std::string, std::unordered_set<std::string>> LocalGribJump::axes(const std::string& request, int level) {
    return Engine().axes(request, level);
}

static GribJumpBuilder<LocalGribJump> builder("local");

}  // namespace gribjump
