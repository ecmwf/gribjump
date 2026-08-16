/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"

#include "gribjump/Config.h"
#include "gribjump/GribJumpException.h"
#include "gribjump/FileLister.h"
#include "gribjump/MarsListerClient.h"
#include "gribjump/Metrics.h"
#include "gribjump/URIHelper.h"

namespace gribjump {

//  ------------------------------------------------------------------

// @todo: move this configure logic into ConfigOptions.
FileLister& FileLister::instance() {
    static std::string type = LibGribJump::instance().config().getString("lister.type", "fdb");
    
    if (type == "fdb") {
        // @todo
        // std::string config_path = LibGribJump::instance().config().getString("lister.config", "");
        // if it isnt set, or is empty, then FDB will use its default behaviour.
        // if it is set, but doesnt exist, it is an error.
        return FDBLister::instance();
    }
    else if (type == "mars") {
        std::string uri = LibGribJump::instance().config().getString("lister.uri", "");
        if (uri.empty()) {
            throw eckit::SeriousBug("FileLister type is set to 'mars' but no URI provided in config. Please set 'lister.uri' to the host:port of the MarsLister server.");
        }
        eckit::net::Endpoint endpoint(uri);
        static MarsListerClient inst(endpoint.host(), endpoint.port());
        return inst;
    } 
    else {
        throw eckit::SeriousBug("Unknown lister type: " + type);
    }
}

FileLister::FileLister() {}

FileLister::~FileLister() {}

filemap_t FileLister::fileMap(const ExItemMap& reqToExtractionItem) {
    filemap_t filemap;
    for (const auto& [key, extractionItemPtr] : reqToExtractionItem) {
        ExtractionItem* extractionItem = extractionItemPtr.get();
        eckit::PathName fname = extractionItem->URI().path();
        insertFileMap(filemap, fname, extractionItem);
    }

    logFileMap(filemap);
    return filemap;
}

//  ------------------------------------------------------------------

FDBLister& FDBLister::instance() {
    static FDBLister instance;
    return instance;
}

FDBLister::FDBLister() : allowMissing_(ConfigOptions::instance().allowMissing()) {}

FDBLister::~FDBLister() {}

std::vector<eckit::URI> FDBLister::list(const std::vector<metkit::mars::MarsRequest> requests) {

    std::vector<eckit::URI> uris;
    fdb5::FDB fdb;
    for (auto& request : requests) {

        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb.list(fdbreq, true);

        fdb5::ListElement elem;
        while (listIter.next(elem)) {
            uris.push_back(elem.location().uri());
        }
    }

    return uris;
}


std::string fdbkeyToStr(const fdb5::Key& key) {
    std::stringstream ss;
    std::string separator      = "";
    std::set<std::string> keys = key.keys();

    // Special case: If date is present, ignore year and month as they are aliases.
    static bool ignoreYearMonth = ConfigOptions::instance().ignoreYearMonth();
    if (ignoreYearMonth && keys.find("date") != keys.end()) {
        keys.erase("year");
        keys.erase("month");
    }

    for (const auto& k : keys) {
        const std::string& value = key.get(k);

        if (value.empty()) {
            continue;
        }

        ss << separator << k << "=" << value;
        separator = ",";
    }
    return ss.str();
}

void FileLister::insertFileMap(filemap_t& filemap, const eckit::PathName& fname, ExtractionItem* item) {
    auto it = filemap.find(fname);
    if (it == filemap.end()) {
        filemap.emplace(fname, std::vector<ExtractionItem*>{item});
    }
    else {
        it->second.push_back(item);
    }
}

void FileLister::logFileMap(const filemap_t& filemap) {
    if (LibGribJump::instance().debug()) {
        LOG_DEBUG_LIB(LibGribJump) << "File map: " << std::endl;
        for (const auto& file : filemap) {
            LOG_DEBUG_LIB(LibGribJump) << "  file=" << file.first << ", Offsets=[";
            for (const auto& extractionItem : file.second) {
                LOG_DEBUG_LIB(LibGribJump) << extractionItem->offset() << ", ";
            }
            LOG_DEBUG_LIB(LibGribJump) << "]" << std::endl;
        }
    }
}

// i.e. do all of the listing work I want...
filemap_t FDBLister::fileMap(const metkit::mars::MarsRequest& unionRequest, const ExItemMap& reqToExtractionItem) {
    filemap_t filemap;

    MetricsManager::instance().addRequest(unionRequest);

    fdb5::FDBToolRequest fdbreq(unionRequest);

    fdb5::FDB fdb;
    auto listIter = fdb.list(fdbreq, true);

    size_t fdb_count = 0;
    size_t count     = 0;
    fdb5::ListElement elem;
    while (listIter.next(elem)) {
        fdb_count++;

        std::string key = fdbkeyToStr(elem.combinedKey());

        // If key not in map, not related to the request
        if (reqToExtractionItem.find(key) == reqToExtractionItem.end())
            continue;

        // Set the URI in the ExtractionItem
        eckit::URI uri                 = elem.location().fullUri();
        ExtractionItem* extractionItem = reqToExtractionItem.at(key).get();
        extractionItem->URI(uri);

        insertFileMap(filemap, uri.path(), extractionItem);

        count++;
    }

    LOG_DEBUG_LIB(LibGribJump) << "FDB found " << fdb_count << " fields. Matched " << count << " fields in "
                               << filemap.size() << " files" << std::endl;
    if (count != reqToExtractionItem.size()) {
        eckit::Log::warning() << "Warning: Number of fields matched (" << count
                              << ") does not match number of keys in extractionItem map (" << reqToExtractionItem.size()
                              << ")" << std::endl;
        if (!allowMissing_) {
            std::stringstream ss;
            ss << "Matched " << count << " fields but " << reqToExtractionItem.size() << " were requested."
               << std::endl;
            ss << "Union request: " << unionRequest << std::endl;
            throw DataNotFoundException(ss.str());
        }
    }

    logFileMap(filemap);

    return filemap;
}

std::map<eckit::PathName, eckit::OffsetList> FDBLister::filesOffsets(
    const std::vector<metkit::mars::MarsRequest>& requests) {
    return filesOffsets(URIs(requests));
}

std::map<eckit::PathName, eckit::OffsetList> FDBLister::filesOffsets(const std::vector<eckit::URI>& uris) {
    std::map<eckit::PathName, eckit::OffsetList> files;
    for (auto& uri : uris) {
        eckit::PathName path = uri.path();
        eckit::Offset offset = URIHelper::offset(uri);
        auto it              = files.find(path);
        if (it == files.end()) {
            eckit::OffsetList offsets;
            offsets.push_back(offset);
            files.emplace(path, offsets);
        }
        else {
            it->second.push_back(offset);
        }
    }
    return files;
}

std::vector<eckit::URI> FDBLister::URIs(const std::vector<metkit::mars::MarsRequest>& requests) {
    std::vector<eckit::URI> uris;
    fdb5::FDB fdb;
    for (auto& request : requests) {
        fdb5::FDBToolRequest fdbreq(request);
        auto listIter = fdb.list(fdbreq, true);
        fdb5::ListElement elem;
        while (listIter.next(elem)) {
            uris.push_back(elem.location().fullUri());
        }
    }
    return uris;
}

std::map<std::string, std::unordered_set<std::string>> FDBLister::axes(const std::string& request, int level) {
    std::vector<fdb5::FDBToolRequest> requests =
        fdb5::FDBToolRequest::requestsFromString(request, std::vector<std::string>(), true);
    ASSERT(requests.size() == 1);  // i.e. assume string is a single request.

    const fdb5::FDBToolRequest& r = requests.front();
    MetricsManager::instance().addRequest(r.request());

    return axes(r, level);
}

std::map<std::string, std::unordered_set<std::string>> FDBLister::axes(const fdb5::FDBToolRequest& request, int level) {
    std::map<std::string, std::unordered_set<std::string>> values;

    LOG_DEBUG_LIB(LibGribJump) << "Using FDB's (new) axes impl" << std::endl;

    fdb5::FDB fdb;
    fdb5::IndexAxis ax = fdb.axes(request, level);
    ax.sort();
    std::map<std::string, eckit::DenseSet<std::string>> fdbValues = ax.map();

    for (const auto& kv : fdbValues) {
        // {
        // Ignore if the value is a single empty string
        // e.g. FDB returns "levellist:{''}" for levtype=sfc.
        // Required for consistency with the old axes impl.
        // if (kv.second.empty() || (kv.second.size() == 1 && kv.second.find("") != kv.second.end())) {
        //     continue;
        // }
        // }
        values[kv.first] = std::unordered_set<std::string>(kv.second.begin(), kv.second.end());
    }


    return values;
}

//  ------------------------------------------------------------------


}  // namespace gribjump
