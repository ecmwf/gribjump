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

#include "eckit/filesystem/PathName.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/log/Log.h"
#include "eckit/log/TimeStamp.h"

#include "gribjump/GribInfoCache.h"
#include "gribjump/LibGribJump.h"


namespace gribjump {

GribInfoCache::~GribInfoCache() {}

GribInfoCache::GribInfoCache(){}

GribInfoCache::GribInfoCache(eckit::PathName dir) : cacheDir_(dir) {
    ASSERT(cacheDir_.exists());
}

bool GribInfoCache::contains(const fdb5::FieldLocation& loc) {

    const std::string fdbfilename = loc.uri().path().baseName();
    const std::string offset = std::to_string(loc.offset());  
    const std::string key = fdbfilename + "." + offset;

    // Check if field in cache
    if( cache_.count(key) != 0 ) {
        return true;
    }

    // Check if gribinfo cache file exists (i.e. manifest is not stale)
    eckit::PathName infopath = cacheDir_ / fdbfilename + ".gj";
    if (!infopath.exists()) {
        return false;
    }

    // Field should be cached on disk, but is not in memory.
    eckit::Log::debug<LibGribJump>() << "Loading " << infopath << " into cache" << std::endl;
    eckit::FileStream s(infopath, "r");
    std::map<std::string, JumpInfo> cache;
    s >> cache;
    s.close();
    cache_.merge(cache);

    // This can only happen if gribinfo file is somehow incomplete.
    ASSERT(cache_.count(key) != 0);

    return true;
}

const JumpInfo& GribInfoCache::get(const fdb5::FieldLocation& loc) {
    // NB: No checks, fails if not in cache. Assumes contains() has been called.
    const std::string fdbfilename = loc.uri().path().baseName();
    const std::string offset = std::to_string(loc.offset());  
    const std::string key = fdbfilename + "." + offset;

    return cache_.at(key);
}

void GribInfoCache::print(std::ostream& s) const {
    // Print the manifest, then the cache
    s << "GribInfoCache[";
    s << "cacheDir=" << cacheDir_ << std::endl;
    s << "cache=" << std::endl;
    for (auto& entry : cache_) {
        s << entry.first << " -> " << entry.second << std::endl;
    }
    s << "]";
}


}  // namespace gribjump
