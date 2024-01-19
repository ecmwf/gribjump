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
#include "eckit/config/Resource.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/exception/Exceptions.h"

#include "gribjump/GribInfoCache.h"
#include "gribjump/LibGribJump.h"


namespace gribjump {

    
GribInfoCache& GribInfoCache::instance() {
    static GribInfoCache instance_;
    return instance_;
}

GribInfoCache::~GribInfoCache() {}

GribInfoCache::GribInfoCache() {

    std::string cache = eckit::Resource<std::string>("gribJumpCacheDir;$GRIBJUMP_CACHE_DIR", "");

    if(cache.empty()) {
        cacheDir_ = eckit::PathName();
        cacheEnabled_ = false;
        return;
    }

    cacheDir_ = eckit::PathName(cache);
    cacheEnabled_ = true;

    if (!cacheDir_.exists()) {
        throw eckit::BadValue("Cache directory " + cacheDir_ + " does not exist");
    }
}

std::string toStr(const fdb5::FieldLocation& loc) {
    return loc.uri().path().baseName() + "." + std::to_string(loc.offset());
}  

void GribInfoCache::insert(const fdb5::FieldLocation& loc, const JumpInfo& info){

    std::lock_guard<std::mutex> lock(mutex_);

    if (!cacheEnabled_) return;
    std::cout << "XXX inserting " << toStr(loc) << std::endl;
    cache_.insert(std::make_pair(toStr(loc), info));
}

bool GribInfoCache::contains(const fdb5::FieldLocation& loc) {

    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!cacheEnabled_) return false;

    const std::string key = toStr(loc);

    // Check if field in cache
    if( cache_.count(key) != 0 ) {
        return true;
    }

    // Check if gribinfo cache file exists (i.e. manifest is not stale)
    const std::string fdbfilename = loc.uri().path().baseName();
    eckit::PathName infopath = cacheDir_ / fdbfilename + ".gj";
    if (!infopath.exists()) {
        return false;
    }

    // Field should be cached on disk, but is not in memory.
    LOG_DEBUG_LIB(LibGribJump) << "Loading " << infopath << " into cache" << std::endl;
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
    std::lock_guard<std::mutex> lock(mutex_);

    // no checks, fails if not in cache. Assumes contains() has been called.
    return cache_.at(toStr(loc));
}

void GribInfoCache::print(std::ostream& s) const {
    std::lock_guard<std::mutex> lock(mutex_);
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
