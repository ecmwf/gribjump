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
#include "eckit/log/Log.h"

#include "gribjump/GribJumpException.h"
#include "gribjump/Lister.h"
#include "gribjump/URIHelper.h"

#include <iostream>

namespace gribjump {

//  ------------------------------------------------------------------

Lister::Lister() {}

Lister::~Lister() {}

//  ------------------------------------------------------------------

FDBLister& FDBLister::instance() {
    static FDBLister instance;
    return instance;
}

FDBLister::FDBLister() :
    allowMissing_(eckit::Resource<bool>("allowMissing;$GRIBJUMP_ALLOW_MISSING",
                                        LibGribJump::instance().config().getBool("allowMissing", false))) {}

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
    static bool ignoreYearMonth = eckit::Resource<bool>("$GRIBJUMP_IGNORE_YEARMONTH", true);
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

// i.e. do all of the listing work I want...
filemap_t FDBLister::fileMap(const metkit::mars::MarsRequest& unionRequest, const ExItemMap& reqToExtractionItem) {
    filemap_t filemap;
    std::cout << "HAVE BEEN HERE?? inside the lister" << std::endl;

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
        eckit::URI uri = elem.location().fullUri();
        std::cout << "WHAT IS THE URI??" << uri << std::endl;
        eckit::Log::warning() << "WHAT IS THE URI??" << uri << std::endl;
        ExtractionItem* extractionItem = reqToExtractionItem.at(key).get();
        extractionItem->URI(uri);

        // Add to filemap
        eckit::PathName fname = uri.path();
        auto it               = filemap.find(fname);
        if (it == filemap.end()) {
            std::vector<ExtractionItem*> extractionItems;
            extractionItems.push_back(extractionItem);
            filemap.emplace(fname, extractionItems);
        }
        else {
            it->second.push_back(extractionItem);
        }

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

    // if (LibGribJump::instance().debug()) {
    if (true) {
        LOG_DEBUG_LIB(LibGribJump) << "File map: " << std::endl;
        for (const auto& file : filemap) {
            std::cout << "  file=" << file.first << ", Offsets=[" << std::endl;
            LOG_DEBUG_LIB(LibGribJump) << "  file=" << file.first << ", Offsets=[";
            for (const auto& extractionItem : file.second) {
                std::cout << extractionItem->offset() << ",";
                LOG_DEBUG_LIB(LibGribJump) << extractionItem->offset() << ", ";
            }
            std::cout << "]" << std::endl;
            LOG_DEBUG_LIB(LibGribJump) << "]" << std::endl;
        }
    }

    return filemap;
}

filemap_t FDBLister::fileMapfromPaths(const ExItemMap& reqToExtractionItem) {
    filemap_t filemap;
    for (const auto& [key, extractionItemPtr] : reqToExtractionItem) {
        // key is a std::string, assumed to represent a URI string
        eckit::URI uri(key);

        std::cout << "WHAT IS THE URI??" << uri << std::endl;
        std::cout << "WHAT IS THE URI??" << uri.scheme() << std::endl;
        std::cout << "WHAT IS THE URI??" << uri.path() << std::endl;

        ExtractionItem* extractionItem = reqToExtractionItem.at(key).get();
        extractionItem->URI(uri);

        // Add to filemap
        eckit::PathName fname = uri.path();
        auto it               = filemap.find(fname);
        if (it == filemap.end()) {
            std::vector<ExtractionItem*> extractionItems;
            extractionItems.push_back(extractionItem);
            filemap.emplace(fname, extractionItems);
        }
        else {
            it->second.push_back(extractionItem);
        }
    }

    if (true) {
        LOG_DEBUG_LIB(LibGribJump) << "File map: " << std::endl;
        for (const auto& file : filemap) {
            std::cout << "  file=" << file.first << ", Offsets=[" << std::endl;
            LOG_DEBUG_LIB(LibGribJump) << "  file=" << file.first << ", Offsets=[";
            for (const auto& extractionItem : file.second) {
                std::cout << extractionItem->offset() << ",";
                LOG_DEBUG_LIB(LibGribJump) << extractionItem->offset() << ", ";
            }
            std::cout << "]" << std::endl;
            LOG_DEBUG_LIB(LibGribJump) << "]" << std::endl;
        }
    }

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
            // LOG_DEBUG_LIB(LibGribJump) << "URIS are: " << std::endl;
            // LOG_DEBUG_LIB(LibGribJump) << elem.location().fullUri() << std::endl;
            // std::cout << "HAVE BEEN HERE??" << std::endl;
            uris.push_back(elem.location().fullUri());
        }
    }
    return uris;
}

std::map<std::string, std::unordered_set<std::string>> FDBLister::axes(const std::string& request, int level) {
    std::vector<fdb5::FDBToolRequest> requests =
        fdb5::FDBToolRequest::requestsFromString(request, std::vector<std::string>(), true);
    ASSERT(requests.size() == 1);  // i.e. assume string is a single request.

    return axes(requests.front(), level);
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
