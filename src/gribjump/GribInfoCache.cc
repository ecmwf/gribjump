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
/// @author Tiago Quintino

#include "gribjump/GribInfoCache.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/config/Resource.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/exception/Exceptions.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/GribHandleData.h"

namespace gribjump {

const char* file_ext = ".gj";

std::string toStr(const fdb5::FieldLocation& loc) {
    return loc.uri().path().baseName() + "." + std::to_string(loc.offset());
}  

std::string to_key(const eckit::PathName& basepath, const eckit::Offset& offset) {
    return basepath + "." + std::to_string(offset);
}

//----------------------------------------------------------------------------------------------------------------------

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

eckit::PathName GribInfoCache::infoFilePath(const eckit::PathName& path) const {
    return cacheDir_ / path.baseName() + file_ext;
}

void GribInfoCache::insert(const fdb5::FieldLocation& loc, const JumpInfo& info){
    if (!cacheEnabled_) return;
    
    LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << toStr(loc) << std::endl;
    
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.insert(std::make_pair(toStr(loc), info));
}

bool GribInfoCache::contains(const fdb5::FieldLocation& loc) {
    
    if (!cacheEnabled_) return false;

    const std::string key = toStr(loc);

    { 
        std::lock_guard<std::mutex> lock(mutex_);
        // Check if field in cache
        if( cache_.count(key) != 0 ) {
            return true;
        }
    }

    // Check if gribinfo cache file exists (i.e. manifest is not stale)
    eckit::PathName infopath = infoFilePath(loc.uri().path());
    if (!infopath.exists()) {
        LOG_DEBUG_LIB(LibGribJump) << "GribInfoCache file " << infopath << " does not exist" << std::endl;
        return false;
    }

    // Field is cached on disk, but is not in memory.
    LOG_DEBUG_LIB(LibGribJump) << "Loading " << infopath << " into cache" << std::endl;
    eckit::FileStream s(infopath, "r");
    std::map<std::string, JumpInfo> cache;
    s >> cache;
    s.close();
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.merge(cache);

        // This can only happen if gribinfo file is somehow incomplete or corrupted
        if(cache_.find(key) == cache_.end()) {
            LOG_DEBUG_LIB(LibGribJump) << "GribInfoCache file " << infopath << " does not contain " << key << "after merge with disk cache" << std::endl;
            return false;
        }
    }

    return true;
}

const JumpInfo& GribInfoCache::get(const fdb5::FieldLocation& loc) {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.at(toStr(loc)); // fails if not in cache
}


void GribInfoCache::scan(const eckit::PathName& fdbpath, const std::vector<eckit::Offset>& offs) {

    // this will be executed in parallel so we dont lock mutex here
    // we will rely on each method to lock mutex when needed

    if (!GribInfoCache::instance().enabled()) return;

    std::vector<eckit::Offset> offsets(offs);
    std::sort(offsets.begin(), offsets.end());

    const eckit::PathName cachePath = GribInfoCache::instance().infoFilePath(fdbpath);

    cache_t cached_infos;

    // if cache exists, merge with existing cache
    if(cachePath.exists()) {
        eckit::FileStream s(cachePath, "r");
        s >> cached_infos;
        s.close();
    }

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " at " << eckit::Plural(offsets.size(), "offset") << std::endl;

    eckit::DataHandle* handle = fdbpath.fileHandle();
    JumpHandle dataSource(handle);

    auto base = fdbpath.baseName();
    bool dirty = false;
    
    for (const auto& offset : offsets) {
        
        std::string key = to_key(base, offset);

        if(cached_infos.find(key) != cached_infos.end()) {
            continue; // already in cache
        }

        dataSource.seek(offset);
        JumpInfo info = dataSource.extractInfo();
        cached_infos[key] = info;
        dirty = true;
    }

    if(!dirty) {
        LOG_DEBUG_LIB(LibGribJump) << "No new fields scanned in " << fdbpath << std::endl;
        return;
    }

    // create a unique filename for the cache file before (atomically) moving it into place

    eckit::PathName uniqPath = eckit::PathName::unique(cachePath);

    LOG_DEBUG_LIB(LibGribJump) << "Writing GribInfo to temporary file " << uniqPath << std::endl;
    eckit::FileStream s(uniqPath, "w");
    s << cached_infos;
    s.close();

    // atomically move the file into place
    eckit::PathName::rename(uniqPath, cachePath);

    LOG_DEBUG_LIB(LibGribJump) << "Generated GribInfo for " << offsets.size() << " new fields in " << cachePath << std::endl;
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
