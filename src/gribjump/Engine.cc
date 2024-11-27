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

#include "eckit/log/Plural.h"
#include "eckit/utils/StringTools.h"

#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Forwarder.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/jumper/JumperFactory.h"



namespace gribjump {
    
//----------------------------------------------------------------------------------------------------------------------

// Stringify requests and keys alphabetically

Engine::Engine() {}

Engine::~Engine() {}

metkit::mars::MarsRequest Engine::buildRequestMap(ExtractionRequests& requests, ExItemMap& keyToExtractionItem ){
    // Split strings into one unified map
    // We also canonicalise the requests such that their keys are in alphabetical order
    /// @todo: Note that it is not in general possible to arbitrary requests into a single request. In future, we should look into
    /// merging into the minimum number of requests.

    std::map<std::string, std::set<std::string>> keyValues;
    for (auto& r : requests) {
        const std::string& s = r.requestString();
        std::vector<std::string> kvs = eckit::StringTools::split(",", s); /// @todo might be faster to use tokenizer directly.
        for (auto& kv : kvs) {
            std::vector<std::string> kv_s = eckit::StringTools::split("=", kv);
            if (kv_s.size() != 2) continue; // ignore verb
            keyValues[kv_s[0]].insert(kv_s[1]);
        }

        // Canonicalise string by sorting keys
        std::sort(kvs.begin(), kvs.end());
        std::string canonicalised = "";
        for (auto& kv : kvs) {
            canonicalised += kv;
            if (kv != kvs.back()) {
                canonicalised += ",";
            }
        }
        ASSERT(keyToExtractionItem.find(canonicalised) == keyToExtractionItem.end()); // no repeats
        r.requestString(canonicalised);
        auto extractionItem = std::make_unique<ExtractionItem>(canonicalised, r.ranges());
        extractionItem->gridHash(r.gridHash());
        keyToExtractionItem.emplace(canonicalised, std::move(extractionItem)); // 1-to-1-map
    }

    // Construct the union request

    std::string result = "retrieve,";
    size_t i = 0;
    for (auto& [key, values] : keyValues) {
        result += key + "=";
        if (values.size() == 1) {
            result += *values.begin();
        } else {
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

filemap_t Engine::buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem) {
    // Map files to ExtractionItem
    filemap_t filemap = FDBLister::instance().fileMap(unionrequest, keyToExtractionItem);
    return filemap;
}

void Engine::scheduleExtractionTasks(filemap_t& filemap){

    bool forwardExtraction = LibGribJump::instance().config().getBool("forwardExtraction", false);
    if (forwardExtraction) {
        Forwarder forwarder;
        forwarder.extract(filemap);
        return;
    }

    bool inefficientExtraction = LibGribJump::instance().config().getBool("inefficientExtraction", false);

    size_t counter = 0;
    for (auto& [fname, extractionItems] : filemap) {
        if (extractionItems[0]->isRemote()) {
            if (inefficientExtraction) {
                taskGroup_.enqueueTask(new InefficientFileExtractionTask(taskGroup_, counter++, fname, extractionItems));
            } else {
                throw eckit::SeriousBug("Got remote URI from FDB, but forwardExtraction enabled in gribjump config.");
            }
        } else {
            taskGroup_.enqueueTask(new FileExtractionTask(taskGroup_, counter++, fname, extractionItems));
        }
    }
    taskGroup_.waitForTasks();
}

ResultsMap Engine::extract(ExtractionRequests& requests) {

    eckit::Timer timer("Engine::extract");
    
    ExItemMap keyToExtractionItem;
    metkit::mars::MarsRequest unionreq = buildRequestMap(requests, keyToExtractionItem);

    filemap_t filemap = buildFileMap(unionreq, keyToExtractionItem);
    MetricsManager::instance().set("elapsed_build_filemap", timer.elapsed());
    timer.reset("Gribjump Engine: Built file map");

    scheduleExtractionTasks(filemap);
    MetricsManager::instance().set("elapsed_tasks", timer.elapsed());
    timer.reset("Gribjump Engine: All tasks finished");

    ResultsMap results = collectResults(keyToExtractionItem);
    MetricsManager::instance().set("elapsed_collect_results", timer.elapsed());

    timer.reset("Gribjump Engine: Repackaged results");

    return results;
}

ResultsMap Engine::collectResults(ExItemMap& keyToExtractionItem) {
    
    // Create map of base request to vector of extraction items. Takes ownership of the ExtractionItems
    ResultsMap results;

    for (auto& [key, ex] : keyToExtractionItem) {
        results[ex->request()].push_back(std::move(ex));
    }

    return results;
}

size_t Engine::scan(const MarsRequests& requests, bool byfiles) {

    std::vector<eckit::URI> uris = FDBLister::instance().URIs(requests);

    if (uris.empty()) {
        MetricsManager::instance().set("count_scanned_fields", 0);
        return 0;
    }

    // forwarded scan requests
    if (LibGribJump::instance().config().getBool("forwardScan", false)) {
        Forwarder forwarder;
        return forwarder.scan(uris);
    }

    std::map< eckit::PathName, eckit::OffsetList > filemap = FDBLister::instance().filesOffsets(uris);

    if (byfiles) { // ignore offsets and scan entire file
        for (auto& [uri, offsets] : filemap) {
            offsets.clear();
        }
    }
    
    return scan(filemap);
}

size_t Engine::scan(std::vector<eckit::PathName> files) {

    scanmap_t scanmap;
    for (auto& fname : files) {
        scanmap[fname] = {};
    }

    return scan(scanmap);
}

size_t Engine::scan(const scanmap_t& scanmap) {

    size_t counter = 0;
    std::atomic<size_t> nfields(0);
    for (auto& [uri, offsets] : scanmap) {
        taskGroup_.enqueueTask(new FileScanTask(taskGroup_, counter++, uri.path(), offsets, nfields));
    }
    taskGroup_.waitForTasks();

    MetricsManager::instance().set("count_scanned_fields", static_cast<size_t>(nfields));

    return nfields;
}

std::map<std::string, std::unordered_set<std::string> > Engine::axes(const std::string& request, int level) {
    MetricsManager::instance().set("request", request);
    return FDBLister::instance().axes(request, level);
}

void Engine::reportErrors(eckit::Stream& client) {
    taskGroup_.reportErrors(client);
}

void Engine::raiseErrors() {
    taskGroup_.raiseErrors();
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump
