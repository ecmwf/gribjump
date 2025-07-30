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
#include "eckit/utils/StringTools.h"

#include "gribjump/LogRouter.h"
#include "metkit/mars/MarsParser.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Forwarder.h"


namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

// Stringify requests and keys alphabetically

Engine::Engine() {}

Engine::~Engine() {}

metkit::mars::MarsRequest Engine::buildRequestMap(ExtractionRequests& requests, ExItemMap& keyToExtractionItem) {
    // Split strings into one unified map
    // We also canonicalise the requests such that their keys are in alphabetical order
    /// @todo: Note that it is not in general possible to arbitrary requests into a single request. In future, we should
    /// look into merging into the minimum number of requests.
    static bool ignoreYearMonth = eckit::Resource<bool>("$GRIBJUMP_IGNORE_YEARMONTH", true);
    std::map<std::string, std::set<std::string>> keyValues;
    bool dropYearMonth = false;
    for (auto& r : requests) {
        const std::string& s = r.requestString();

        /// @todo: this ignoreYearMonth logic is duplicated somewhat in Lister.cc. We should consolidate.
        // If "year" and "month" are present, we must drop them if "date" is also present.
        if (ignoreYearMonth && s.find("year") != std::string::npos && s.find("month") != std::string::npos &&
            s.find("date") != std::string::npos) {
            dropYearMonth = true;
        }

        std::vector<std::string> kvs =
            eckit::StringTools::split(",", s);  /// @todo might be faster to use tokenizer directly.
        std::vector<std::string> kvs_sanitized;
        kvs_sanitized.reserve(kvs.size());

        for (auto& kv : kvs) {
            std::vector<std::string> kv_s = eckit::StringTools::split("=", kv);
            if (kv_s.size() != 2)
                continue;  // ignore verb
            if (dropYearMonth && (kv_s[0] == "year" || kv_s[0] == "month")) {
                continue;
            }
            keyValues[kv_s[0]].insert(kv_s[1]);
            kvs_sanitized.push_back(kv);
        }

        // Canonicalise string by sorting keys
        std::sort(kvs_sanitized.begin(), kvs_sanitized.end());
        std::string canonicalised = "";
        for (auto& kv : kvs_sanitized) {
            canonicalised += kv;
            if (kv != kvs_sanitized.back()) {
                canonicalised += ",";
            }
        }

        ASSERT(keyToExtractionItem.find(canonicalised) == keyToExtractionItem.end());  // no repeats
        r.requestString(canonicalised);

        auto extractionItem = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(r));
        keyToExtractionItem.emplace(canonicalised, std::move(extractionItem));  // 1-to-1-map
    }

    // Construct the union request

    std::string result = "retrieve,";
    size_t i           = 0;
    for (auto& [key, values] : keyValues) {
        result += key + "=";
        if (values.size() == 1) {
            result += *values.begin();
        }
        else {
            size_t j = 0;
            for (auto& value : values) {
                result += value;
                if (j != values.size() - 1) {
                    result += "/";
                }
                j++;
            }
        }
        if (i != keyValues.size() - 1) {
            result += ",";
        }
        i++;
    }

    std::istringstream istream(result);
    metkit::mars::MarsParser parser(istream);
    std::vector<metkit::mars::MarsParsedRequest> unionRequests = parser.parse();
    ASSERT(unionRequests.size() == 1);
    return unionRequests[0];
}


void Engine::buildRequestURIsMap(PathExtractionRequests& requests, ExItemMap& keyToExtractionItem) {
    for (auto& r : requests) {
        const std::string& s = r.requestString();

        auto extractionItem = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(r));

        eckit::URI uri = eckit::URI(r.scheme(), r.path());
        uri.host(r.host());
        uri.port(r.port());
        uri.fragment(std::to_string(r.offset()));
        extractionItem->URI(uri);

        keyToExtractionItem.emplace(s, std::move(extractionItem));  // 1-to-1-map
    }
}

filemap_t Engine::buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem) {
    // Map files to ExtractionItem
    filemap_t filemap = FDBLister::instance().fileMap(unionrequest, keyToExtractionItem);
    return filemap;
}

filemap_t Engine::buildFileMapfromPaths(ExItemMap& keyToExtractionItem) {
    // Map files to ExtractionItem
    filemap_t filemap = FDBLister::instance().fileMapfromPaths(keyToExtractionItem);
    return filemap;
}

TaskReport Engine::scheduleExtractionTasks(filemap_t& filemap, bool forward) {

    if (forward) {
        Forwarder forwarder;
        return forwarder.extract(filemap);
    }

    bool inefficientExtraction = LibGribJump::instance().config().getBool("inefficientExtraction", false);

    TaskGroup taskGroup;

    for (auto& [fname, extractionItems] : filemap) {
        if (extractionItems[0]->isRemote()) {
            if (inefficientExtraction) {
                taskGroup.enqueueTask<InefficientFileExtractionTask>(fname, extractionItems);
            }
            else {
                throw eckit::SeriousBug("Got remote URI from FDB, but forwardExtraction enabled in gribjump config.");
            }
        }
        else {
            taskGroup.enqueueTask<FileExtractionTask>(fname, extractionItems);
        }
    }
    taskGroup.waitForTasks();
    return taskGroup.report();
}

TaskOutcome<ResultsMap> Engine::extract(ExtractionRequests& requests) {

    eckit::Timer timer("Engine::extract", LogRouter::instance().get("timer"));

    ExItemMap keyToExtractionItem;
    metkit::mars::MarsRequest unionreq = buildRequestMap(requests, keyToExtractionItem);

    // Build file map
    filemap_t filemap = buildFileMap(unionreq, keyToExtractionItem);
    MetricsManager::instance().set("elapsed_build_filemap", timer.elapsed());
    timer.reset("Gribjump Engine: Built file map");

    // Schedule tasks
    bool forward      = LibGribJump::instance().config().getBool("forwardExtraction", false);
    TaskReport report = scheduleExtractionTasks(filemap, forward);
    MetricsManager::instance().set("elapsed_tasks", timer.elapsed());
    timer.reset("Gribjump Engine: All tasks finished");

    // Collect results
    ResultsMap results = collectResults(keyToExtractionItem);
    MetricsManager::instance().set("elapsed_collect_results", timer.elapsed());
    timer.reset("Gribjump Engine: Repackaged results");

    return {std::move(results), std::move(report)};
}


TaskOutcome<ResultsMap> Engine::extract(PathExtractionRequests& requests) {

    eckit::Timer timer("Engine::extract", LogRouter::instance().get("timer"));

    ExItemMap keyToExtractionItem;  // Will collect path str uris -> extraction items
    buildRequestURIsMap(requests, keyToExtractionItem);

    // Build file map
    // Will then go from the str uris to collect the path uris and offsets from the requests inside
    filemap_t filemap = buildFileMapfromPaths(keyToExtractionItem);
    MetricsManager::instance().set("elapsed_build_filemap", timer.elapsed());
    timer.reset("Gribjump Engine: Built file map");

    // Schedule tasks: if there is no host and port, set forward to false, otherwise set to true
    bool forward = true;
    if (requests[0].host() == "" and requests[0].port() == 0) {
        forward = false;
    }
    TaskReport report = scheduleExtractionTasks(filemap, forward);
    MetricsManager::instance().set("elapsed_tasks", timer.elapsed());
    timer.reset("Gribjump Engine: All tasks finished");

    // Collect results
    ResultsMap results = collectResults(keyToExtractionItem);
    MetricsManager::instance().set("elapsed_collect_results", timer.elapsed());
    timer.reset("Gribjump Engine: Repackaged results");

    return {std::move(results), std::move(report)};
}

ResultsMap Engine::collectResults(ExItemMap& keyToExtractionItem) {

    // Create map of base request to vector of extraction items. Takes ownership of the ExtractionItems
    ResultsMap results;

    for (auto& [key, ex] : keyToExtractionItem) {
        results[ex->request()] = std::move(ex);
    }

    return results;
}

TaskOutcome<size_t> Engine::scan(const MarsRequests& requests, bool byfiles) {

    std::vector<eckit::URI> uris = FDBLister::instance().URIs(requests);

    /// @todo do we explicitly need this?
    if (uris.empty()) {
        MetricsManager::instance().set("count_scanned_fields", 0);
        return {0, TaskReport()};
    }

    // forwarded scan requests
    if (LibGribJump::instance().config().getBool("forwardScan", false)) {
        Forwarder forwarder;
        return forwarder.scan(uris);
    }

    std::map<eckit::PathName, eckit::OffsetList> filemap = FDBLister::instance().filesOffsets(uris);

    if (byfiles) {  // ignore offsets and scan entire file
        for (auto& [uri, offsets] : filemap) {
            offsets.clear();
        }
    }

    return scheduleScanTasks(filemap);
}

TaskOutcome<size_t> Engine::scan(std::vector<eckit::PathName> files) {

    scanmap_t scanmap;
    for (auto& fname : files) {
        scanmap[fname] = {};
    }

    return scheduleScanTasks(scanmap);
}

TaskOutcome<size_t> Engine::scheduleScanTasks(const scanmap_t& scanmap) {

    std::atomic<size_t> nfields(0);
    TaskGroup taskGroup;
    for (auto& [uri, offsets] : scanmap) {
        taskGroup.enqueueTask<FileScanTask>(uri.path(), offsets, nfields);
    }
    taskGroup.waitForTasks();

    MetricsManager::instance().set("count_scanned_fields", static_cast<size_t>(nfields));

    return {nfields, taskGroup.report()};
}

std::map<std::string, std::unordered_set<std::string>> Engine::axes(const std::string& request, int level) {
    MetricsManager::instance().set("request", request);
    return FDBLister::instance().axes(request, level);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
