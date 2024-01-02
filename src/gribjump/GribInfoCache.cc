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

#include "gribjump/GribInfoCache.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/log/Log.h"
#include "gribjump/LibGribJump.h"


namespace gribjump {

GribInfoCache::~GribInfoCache() {}

GribInfoCache::GribInfoCache(){}

GribInfoCache::GribInfoCache(eckit::PathName dir) : cacheDir_(dir) {
    eckit::PathName path = cacheDir_ / "gj.manifest";
    ASSERT(path.exists());
    eckit::FileStream s(path, "r");
    s >> manifest_;
    s.close();

}

void GribInfoCache::preload() {
    for (auto& entry : manifest_) {
        eckit::PathName infopath = entry.second;
        eckit::FileStream s(infopath, "r");
        std::map<std::string, JumpInfo> cache;
        s >> cache;
        s.close();
        cache_.merge(cache);
    }
}

bool GribInfoCache::contains(const fdb5::FieldLocation& loc) {

    std::stringstream ss;
    ss << loc;
    std::string fieldloc = ss.str();

    // Check if field in cache
    if( cache_.count(fieldloc) != 0 ) {
        return true;
    }

    // Check if field's filename is in manifest
    eckit::PathName fdbpath = loc.uri().path();
    auto el = manifest_.find(fdbpath);
    if (el == manifest_.end()) {
        return false;
    }

    // Check if gribinfo cache file exists (i.e. manifest is not stale)
    eckit::PathName infopath = el->second;
    if (!infopath.exists()) {
        return false;
    }

    // This field is in the cache, but not in memory. Load it.
    eckit::Log::debug<LibGribJump>() << "Merging " << infopath << " with cache" << std::endl;
    eckit::FileStream s(infopath, "r");
    std::map<std::string, JumpInfo> cache;
    s >> cache;
    s.close();
    cache_.merge(cache);

    // This can only happen if gribinfo file is somehow incomplete.
    ASSERT(cache_.count(fieldloc) != 0);

    return true;
}

const JumpInfo& GribInfoCache::get(const fdb5::FieldLocation& loc) {
    // NB: No checks, fails if not in cache. Assumes contains() has been called.
    std::stringstream ss;
    ss << loc;
    std::string fieldloc = ss.str();
    return cache_.at(fieldloc);
}

void GribInfoCache::print(std::ostream& s) const {
    // Print the manifest, then the cache
    s << "GribInfoCache[";
    s << "cacheDir=" << cacheDir_ << std::endl;
    s << "#entries=" << manifest_.size() << std::endl;
    for (auto& entry : manifest_) {
        s << entry.first << " -> " << entry.second << std::endl;
    }
    s << "cache=" << std::endl;
    for (auto& entry : cache_) {
        s << entry.first << " -> " << entry.second << std::endl;
    }
    s << "]";
}

}  // namespace gribjump
