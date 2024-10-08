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

#include "metkit/mars/MarsExpension.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/jumper/JumperFactory.h"



namespace gribjump {
    
//----------------------------------------------------------------------------------------------------------------------

// Stringify requests and keys alphabetically
namespace 
{
std::string requestToStr(const metkit::mars::MarsRequest& request) {
    std::stringstream ss;
    std::string separator = "";
    std::vector<std::string> keys = request.params();
    std::sort(keys.begin(), keys.end());
    for(const auto& key : keys) {
        ss << separator << key << "=" << request[key];
        separator = ",";
    }
    return ss.str();
}

//----------------------------------------------------------------------------------------------------------------------


class CollectFlattenedRequests : public metkit::mars::FlattenCallback {
public:
    CollectFlattenedRequests(std::vector<metkit::mars::MarsRequest>& flattenedRequests) : flattenedRequests_(flattenedRequests) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        flattenedRequests_.push_back(req);
    }

    std::vector<metkit::mars::MarsRequest>& flattenedRequests_;
};

std::vector<metkit::mars::MarsRequest> flattenRequest(const metkit::mars::MarsRequest& request) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;
    std::vector<metkit::mars::MarsRequest> flattenedRequests;
    
    CollectFlattenedRequests cb(flattenedRequests);
    expansion.flatten(ctx, request, cb);

    LOG_DEBUG_LIB(LibGribJump) << "Base request: " << request << std::endl;

    for (const auto& req : flattenedRequests) {
        LOG_DEBUG_LIB(LibGribJump) << "  Flattened request: " << req << std::endl;
    }

    return flattenedRequests;
}

// Stringify requests, and flatten if necessary

typedef std::map<metkit::mars::MarsRequest, std::vector<std::string>> flattenedKeys_t;

flattenedKeys_t buildFlatKeys(const ExtractionRequests& requests, bool flatten) {
    
    flattenedKeys_t keymap;
    
    for (const auto& req : requests) {
        const metkit::mars::MarsRequest& baseRequest = req.request();
        keymap[baseRequest] = std::vector<std::string>();

        // Assume baseRequest has cardinality >= 1 and may need to be flattened
        if (flatten) {
            std::vector<metkit::mars::MarsRequest> flat = flattenRequest(baseRequest);
            for (const auto& r : flat) {
                keymap[baseRequest].push_back(requestToStr(r));
            }
        }

        // Assume baseRequest has cardinality 1
        else {
            keymap[baseRequest].push_back(requestToStr(baseRequest));
        }

        eckit::Log::debug<LibGribJump>() << "Flattened keys for request " << baseRequest << ": " << keymap[baseRequest] << std::endl;
    }

    return keymap;
}

metkit::mars::MarsRequest unionRequest(const MarsRequests& requests) {

    /// @todo: we should do some check not to merge on keys like class and stream
    metkit::mars::MarsRequest unionRequest = requests.front();
    for(size_t i = 1; i < requests.size(); ++i) {
        unionRequest.merge(requests[i]);
    }
    
    eckit::Log::info() << "Gribjump: Union request is " << unionRequest << std::endl;
    
    return unionRequest;
}

bool isRemote(eckit::URI uri) {
    return uri.scheme() == "fdb";
}

} // namespace 
//----------------------------------------------------------------------------------------------------------------------

Engine::Engine() {}

Engine::~Engine() {}

ExItemMap Engine::buildKeyToExtractionItem(const ExtractionRequests& requests, bool flatten){
    ExItemMap keyToExtractionItem;

    flattenedKeys_t flatKeys = buildFlatKeys(requests, flatten); // Map from base request to {flattened keys}

    LOG_DEBUG_LIB(LibGribJump) << "Built flat keys" << std::endl;

    // Create the 1-to-1 map
    for (size_t i = 0; i < requests.size(); i++) {
        const metkit::mars::MarsRequest& basereq = requests[i].request();
        const std::vector<std::string> keys = flatKeys[basereq];
        for (const auto& key : keys) {
            ASSERT(keyToExtractionItem.find(key) == keyToExtractionItem.end()); /// @todo support duplicated requests?
            auto extractionItem = std::make_unique<ExtractionItem>(basereq, requests[i].ranges());
            extractionItem->gridHash(requests[i].gridHash());
            keyToExtractionItem.emplace(key, std::move(extractionItem)); // 1-to-1-map
        }
    }

    return keyToExtractionItem;
}

filemap_t Engine::buildFileMap(const ExtractionRequests& requests, ExItemMap& keyToExtractionItem) {
    // Map files to ExtractionItem
    eckit::Timer timer("Gribjump Engine: Building file map");

    std::vector<metkit::mars::MarsRequest> marsrequests;
    for (const auto& req : requests) {
        marsrequests.push_back(req.request());
    }

    const metkit::mars::MarsRequest req = unionRequest(marsrequests);
    timer.reset("Gribjump Engine: Flattened requests and constructed union request");

    filemap_t filemap = FDBLister::instance().fileMap(req, keyToExtractionItem);

    return filemap;
}

void Engine::forwardRemoteExtraction(filemap_t& filemap) {
    // get servermap from config, which maps fdb remote uri to gribjump server uri
    // format: fdbhost:port -> gjhost:port
    /// @todo: dont parse servermap every request
    const std::map<std::string, std::string>& servermap_str = LibGribJump::instance().config().serverMap();
    ASSERT(!servermap_str.empty());

    for (auto& [fdb, gj] : servermap_str) {
        LOG_DEBUG_LIB(LibGribJump) << "Servermap: " << fdb << " -> " << gj << std::endl;
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

ResultsMap Engine::extract(const ExtractionRequests& requests, bool flatten) {

    ExItemMap keyToExtractionItem = buildKeyToExtractionItem(requests, flatten); // Owns the ExtractionItems
    filemap_t filemap = buildFileMap(requests, keyToExtractionItem);
    eckit::Timer timer("Engine::extract");

    scheduleTasks(filemap);
    timer.reset("Gribjump Engine: All tasks finished");

    ResultsMap results = collectResults(keyToExtractionItem);
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

    const std::map< eckit::PathName, eckit::OffsetList > files = FDBLister::instance().filesOffsets(requests);

    size_t counter = 0;

    if (byfiles) {
        for (auto& [fname, offsets] : files) {
            taskGroup_.enqueueTask(new FileScanTask(taskGroup_, counter++, fname, offsets));
        }
    }
    else {
        for (auto& [fname, offsets] : files) {
            taskGroup_.enqueueTask(new FileScanTask(taskGroup_, counter++, fname, {}));
        }
    }

    // Wait for the tasks to complete
    taskGroup_.waitForTasks();

    return files.size();
}

std::map<std::string, std::unordered_set<std::string> > Engine::axes(const std::string& request) {
    return FDBLister::instance().axes(request);
}

void Engine::reportErrors(eckit::Stream& client) {
    taskGroup_.reportErrors(client);
}

void Engine::raiseErrors() {
    taskGroup_.raiseErrors();
}
void Engine::updateMetrics(Metrics& metrics) {
    metrics.nTasks = taskGroup_.nTasks();
    metrics.nFailedTasks = taskGroup_.nErrors();
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump
