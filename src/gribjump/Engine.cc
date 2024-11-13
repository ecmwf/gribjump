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
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/jumper/JumperFactory.h"



namespace gribjump {
    
//----------------------------------------------------------------------------------------------------------------------

// Stringify requests and keys alphabetically
namespace {
// ----------------------------------------------------------------------------------------------------------------------

bool isRemote(eckit::URI uri) {
    return uri.scheme() == "fdb";
}

} // namespace 
//----------------------------------------------------------------------------------------------------------------------

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

void Engine::forwardRemoteExtraction(filemap_t& filemap) {
    // get servermap from config, which maps fdb remote uri to gribjump server uri
    // format: fdbhost:port -> gjhost:port
    /// @todo: dont parse servermap every request
    const std::map<std::string, std::string>& servermap_str = LibGribJump::instance().config().serverMap();
    ASSERT(!servermap_str.empty());

    if (LibGribJump::instance().debug()) {
        for (auto& [fdb, gj] : servermap_str) {
            LOG_DEBUG_LIB(LibGribJump) << "Servermap: " << fdb << " -> " << gj << std::endl;
        }
    }
    std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint> servermap;
    for (auto& [fdb, gj] : servermap_str) {
        eckit::net::Endpoint fdbEndpoint(fdb);
        eckit::net::Endpoint gjEndpoint(gj);
        servermap[fdbEndpoint] = gjEndpoint;
    }

    // Match servers with files
    std::unordered_map<eckit::net::Endpoint, std::vector<std::string>> serverfiles;
    for (auto& [fname, extractionItems] : filemap) {
        eckit::URI uri = extractionItems[0]->URI();
        eckit::net::Endpoint fdbEndpoint;

        if(!isRemote(uri)) {
            throw eckit::SeriousBug("URI is not remote: " + fname);
        }

        fdbEndpoint = eckit::net::Endpoint(uri.host(), uri.port());

        if (servermap.find(fdbEndpoint) == servermap.end()) {
            throw eckit::SeriousBug("No gribjump endpoint found for fdb endpoint: " + std::string(fdbEndpoint));
        }

        serverfiles[servermap[fdbEndpoint]].push_back(fname);
    }

    // make subfilemaps for each server
    std::unordered_map<eckit::net::Endpoint, filemap_t> serverfilemaps;

    for (auto& [server, files] : serverfiles) {
        filemap_t subfilemap;
        for (auto& fname : files) {
            subfilemap[fname] = filemap[fname];
        }
        serverfilemaps[server] = subfilemap;
    }

    // forward to servers
    size_t counter = 0;
    for (auto& [endpoint, subfilemap] : serverfilemaps) {
        taskGroup_.enqueueTask(new RemoteExtractionTask(taskGroup_, counter++, endpoint, subfilemap));
    }

    taskGroup_.waitForTasks();
}

void Engine::scheduleTasks(filemap_t& filemap){

    bool remoteExtraction = LibGribJump::instance().config().getBool("remoteExtraction", false);
    if (remoteExtraction) {
        forwardRemoteExtraction(filemap);
        return;
    }

    size_t counter = 0;
    for (auto& [fname, extractionItems] : filemap) {
        if (isRemote(extractionItems[0]->URI())) {
            // Only possible if we are using remoteFDB, which requires remoteExtraction to be enabled.
            // We technically do support it via inefficient extraction, but we are disabling this for now.
            // taskGroup_.enqueueTask(new InefficientFileExtractionTask(taskGroup_, counter++, fname, extractionItems));
            throw eckit::SeriousBug("Got remote URI from FDB, but remoteExtraction enabled in gribjump config.");
        }
        else {
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

    scheduleTasks(filemap);
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

    const std::map< eckit::PathName, eckit::OffsetList > filemap = FDBLister::instance().filesOffsets(requests);

    if (byfiles) { // ignore offsets and scan entire file
        std::vector<eckit::PathName> files;
        for (auto& [fname, offsets] : filemap) {
            files.push_back(fname);
        }

        return scan(files);
        
    }

    size_t counter = 0;
    
    for (auto& [fname, offsets] : filemap) {
        taskGroup_.enqueueTask(new FileScanTask(taskGroup_, counter++, fname, offsets));
    }
    taskGroup_.waitForTasks();

    return filemap.size();
}

size_t Engine::scan(std::vector<eckit::PathName> files) {
    size_t counter = 0;
    
    for (auto& fname : files) {
        taskGroup_.enqueueTask(new FileScanTask(taskGroup_, counter++, fname, {}));
    }

    taskGroup_.waitForTasks();

    return files.size();
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
