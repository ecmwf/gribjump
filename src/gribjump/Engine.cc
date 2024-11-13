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
#if 0

// Stringify requests, and flatten if necessary

typedef std::map<metkit::mars::MarsRequest, std::vector<std::string>> flattenedKeys_t;

flattenedKeys_t buildFlatKeys(const ExtractionRequests& requests, bool flatten) {
    
    flattenedKeys_t keymap;
    
    // ASSERT(!flatten); // polytope already gives flat requests

    // for (const auto& req : requests) {
    //     const std::string& baseRequest = req.request_string();
    //     ASSERT(!baseRequest.empty());
    //     keymap[baseRequest] = std::vector<std::string>();
    //     keymap[baseRequest].push_back(baseRequest);    
    // }
    // return keymap;

    // // debug/
    ASSERT(false);
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
#endif 0
// ----------------------------------------------------------------------------------------------------------------------
std::string unionise(const std::vector<ExtractionRequest>& requests) {
    // Take many marsrequest-like strings and combine them into one string.
    // Note, makes some assumptions:
    // 1. Each string is unique
    // 2. For each key there is a single value. E.g., "step=1/2/3" should be pre-split into three strings.
    // 3. Does not check if the string is sensible.
    // takes a vector of strings, each like "retrieve,expver=xxxx,class=od,date=20241110,domain=g,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=0"
    // The result is a string, that should be parsable my mars.

    // split the string into key value pairs by comma
    std::map<std::string, std::set<std::string>> keyValues;
    for (auto& r : requests) {
        const std::string& s = r.request_string();
        std::vector<std::string> kvs = eckit::StringTools::split(",", s); // might be faster to use tokenizer directly.
        for (auto& kv : kvs) {
            std::vector<std::string> kv_s = eckit::StringTools::split("=", kv);
            if (kv_s.size() != 2) continue; // ignore verb
            keyValues[kv_s[0]].insert(kv_s[1]);
        }

        // Important! Canonicalise string so that we can use it to match with fdb. We do this by sorting the keys.
        // e.g. expver=xxxx,class=od,date=20241110,domain=g,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=0
        // becomes class=od,date=20241110,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=0
        std::sort(kvs.begin(), kvs.end());
        std::string canonicalised = "";
        for (auto& kv : kvs) {
            // skip if it is "retrieve"
            if (kv.find("retrieve") != std::string::npos) continue;
            canonicalised += kv;
            if (kv != kvs.back()) {
                canonicalised += ",";
            }
        }
        // r.request_string(canonicalised);
        NOTIMP;
    }

    // now construct a string with all the values:
    // expver=xxxx,class=od,date=20241110,domain=g,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=0/1/2/3/4
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

    return result;
}
// ----------------------------------------------------------------------------------------------------------------------

bool isRemote(eckit::URI uri) {
    return uri.scheme() == "fdb";
}

} // namespace 
//----------------------------------------------------------------------------------------------------------------------

Engine::Engine() {}

Engine::~Engine() {}


// metkit::mars::MarsRequest unionRequest(const ExtractionRequests& requests) {

//     /// @todo: we should do some check not to merge on keys like class and stream
//     // metkit::mars::MarsRequest unionRequest = requests.front().request();
//     // for(size_t i = 1; i < requests.size(); ++i) {
//     //     unionRequest.merge(requests[i].request());
//     // }
//     std::string unionRequestStr = unionise(requests);
//     std::istringstream istream(unionRequestStr);
//     metkit::mars::MarsParser parser(istream);
//     std::vector<metkit::mars::MarsParsedRequest> unionRequests = parser.parse();
//     ASSERT(unionRequests.size() == 1);
//     metkit::mars::MarsRequest unionRequest = unionRequests[0];
//     eckit::Log::info() << "Gribjump: Union request is " << unionRequest << std::endl;
    
//     MetricsManager::instance().set("union_request", unionRequestStr);
    
//     return unionRequest;
// }

metkit::mars::MarsRequest combinedFoo(const ExtractionRequests& requests, ExItemMap& keyToExtractionItem ){

    // Split strings into one unified map
    std::map<std::string, std::set<std::string>> keyValues;
    for (auto& r : requests) {
        const std::string& s = r.request_string();
        std::vector<std::string> kvs = eckit::StringTools::split(",", s); // might be faster to use tokenizer directly.
        for (auto& kv : kvs) {
            std::vector<std::string> kv_s = eckit::StringTools::split("=", kv);
            if (kv_s.size() != 2) continue; // ignore verb
            keyValues[kv_s[0]].insert(kv_s[1]);
        }

        // Important! Canonicalise string by sorting keys
        std::sort(kvs.begin(), kvs.end());
        std::string canonicalised = "";
        for (auto& kv : kvs) {
            // skip if it is "retrieve"
            if (kv.find("retrieve") != std::string::npos) continue;
            canonicalised += kv;
            if (kv != kvs.back()) {
                canonicalised += ",";
            }
        }
        ASSERT(keyToExtractionItem.find(canonicalised) == keyToExtractionItem.end()); /// no repeats
        auto extractionItem = std::make_unique<ExtractionItem>(canonicalised, r.ranges()); // TODO: XXX we're giving it the mars request instead of the request string
        extractionItem->gridHash(r.gridHash());
        keyToExtractionItem.emplace(canonicalised, std::move(extractionItem)); // 1-to-1-map
    }

    // --- construct the union request

        // now construct a string with all the values:
    // expver=xxxx,class=od,date=20241110,domain=g,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=0/1/2/3/4
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

ExItemMap Engine::buildKeyToExtractionItem(const ExtractionRequests& requests, bool flatten){

    ExItemMap keyToExtractionItem;
    ASSERT(!flatten);
    // It is already flat, and we already have strings.
    for (size_t i = 0; i < requests.size(); i++) {
        const std::string& key = requests[i].request_string();
        ASSERT(keyToExtractionItem.find(key) == keyToExtractionItem.end()); /// no repeats
        auto extractionItem = std::make_unique<ExtractionItem>(requests[i].request_string(), requests[i].ranges()); // TODO: XXX we're giving it the mars request instead of the request string
        extractionItem->gridHash(requests[i].gridHash());
        keyToExtractionItem.emplace(key, std::move(extractionItem)); // 1-to-1-map
    }

    return keyToExtractionItem;


    // Code that assumes we might need to flatten, which requires you to be using mars requests not strings:
    ASSERT(false);
    // flattenedKeys_t flatKeys = buildFlatKeys(requests, flatten); // Map from base request to {flattened keys}

    // LOG_DEBUG_LIB(LibGribJump) << "Built flat keys" << std::endl;

    // // Create the 1-to-1 map
    // for (size_t i = 0; i < requests.size(); i++) {
    //     const metkit::mars::MarsRequest& basereq = requests[i].request();
    //     const std::vector<std::string> keys = flatKeys[basereq];
    //     for (const auto& key : keys) {
    //         ASSERT(keyToExtractionItem.find(key) == keyToExtractionItem.end()); /// @todo support duplicated requests?
    //         auto extractionItem = std::make_unique<ExtractionItem>(basereq, requests[i].ranges());
    //         extractionItem->gridHash(requests[i].gridHash());
    //         keyToExtractionItem.emplace(key, std::move(extractionItem)); // 1-to-1-map
    //     }
    // }

    // return keyToExtractionItem;
}

filemap_t Engine::buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem) {
    // Map files to ExtractionItem
    eckit::Timer timer("Gribjump Engine: Building file map");

    // const metkit::mars::MarsRequest req = unionRequest(requests);
    MetricsManager::instance().set("debug_elapsed_union_request", timer.elapsed());
    timer.reset("Gribjump Engine: Flattened requests and constructed union request");

    filemap_t filemap = FDBLister::instance().fileMap(unionrequest, keyToExtractionItem);

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

ResultsMap Engine::extract(const ExtractionRequests& requests) {

    eckit::Timer timer("Engine::extract");
    
    // // Combine these?
    // ExItemMap keyToExtractionItem = buildKeyToExtractionItem(requests, flatten); // Owns the ExtractionItems
    // const metkit::mars::MarsRequest unionreq = unionRequest(requests);
    
    ExItemMap keyToExtractionItem;
    metkit::mars::MarsRequest unionreq = combinedFoo(requests, keyToExtractionItem);

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

    // NOTIMP;
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
