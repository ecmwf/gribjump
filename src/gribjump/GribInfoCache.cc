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

const char* file_ext = ".gribjump";

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

GribInfoCache::~GribInfoCache() {
    for (auto& entry : cache_) {
        delete entry.second;
    }
}

GribInfoCache::GribInfoCache() {

    static_assert(sizeof(off_t) == sizeof(eckit::Offset), "off_t and eckit::Offset must be the same size");

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

eckit::PathName GribInfoCache::cacheFilePath(const eckit::PathName& path) const {
    return cacheDir_ / path.baseName() + file_ext;
}

void GribInfoCache::insert(const fdb5::FieldLocation& loc, JumpInfo* info) {
    if (!cacheEnabled_) return;
    insert(loc.uri().path().baseName(), loc.offset(), info);
}

GribInfoCache::InfoCache&  GribInfoCache::getFileCache(const filename_t& f) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(f);
    if(it == cache_.end()) {
        InfoCache* infocache = new InfoCache();
        cache_.insert(std::make_pair(f, infocache));
        return *infocache;
    }
    return *(it->second);
}

void GribInfoCache::insert(const filename_t& f, off_t offset, JumpInfo* info) {
    LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << f << ":" << offset << std::endl;
    InfoCache& filecache = getFileCache(f);
    std::lock_guard<std::recursive_mutex> lock(filecache.mutex_);
    filecache.infocache_.insert(std::make_pair(offset, JumpInfoHandle(info)));
}

bool GribInfoCache::loadIntoCache(const eckit::PathName& cachePath, GribInfoCache::InfoCache& cache) {
    if (cachePath.exists()) {
        
        LOG_DEBUG_LIB(LibGribJump) << "Loading " << cachePath << " into cache" << std::endl;
        eckit::FileStream s(cachePath, "r");
        infocache_t c;
        s >> c;
        s.close();

        std::lock_guard<std::recursive_mutex> lock(cache.mutex_);
        cache.infocache_.merge(c);
        return true;
    }   

    LOG_DEBUG_LIB(LibGribJump) << "GribInfoCache file " << cachePath << " does not exist" << std::endl;
    return false;
}


JumpInfo* GribInfoCache::get(const fdb5::FieldLocation& loc) {
    
    if (!cacheEnabled_) return nullptr;

    filename_t f = loc.uri().path().baseName();
    InfoCache& filecache = getFileCache(f);

    off_t offset = loc.offset();

    // return it in memory cache
    {   
        std::lock_guard<std::recursive_mutex> lock(filecache.mutex_);
        auto it = filecache.infocache_.find(offset);
        if(it != filecache.infocache_.end()) return it->second.get();
    }
    
    // cache miss, load cache file into memory, maybe it has info for this field
    eckit::PathName cachePath = cacheFilePath(loc.uri().path());
    bool loaded = loadIntoCache(cachePath, filecache); 

    // somthing was loaded, check if it contains the field we want
    if(loaded) {
        std::lock_guard<std::recursive_mutex> lock(filecache.mutex_);
        auto it = filecache.infocache_.find(offset);
        if(it != filecache.infocache_.end()) return it->second.get();
        LOG_DEBUG_LIB(LibGribJump) << "GribInfoCache file " << cachePath << " does not contain JumpInfo for field at offset " << offset << std::endl;
    }

    return nullptr;
}

void GribInfoCache::insert(const eckit::PathName& path, const std::vector<JumpInfo*>& infos) {
    // this will be executed in parallel so we dont lock main mutex_ here
    // we will rely on each method to lock mutex when needed

    if (!cacheEnabled_) return;

    auto base = path.baseName();
    auto cachePath = cacheFilePath(base);

    // if cache exists load so we can merge with memory cache
    InfoCache& filecache = getFileCache(base);
    bool loaded = loadIntoCache(cachePath, filecache); 

    std::lock_guard<std::recursive_mutex> lock(filecache.mutex_);
    infocache_t& cache = filecache.infocache_;
    for (auto info : infos) {
        off_t offset = info->offset();
        cache.insert(std::make_pair(offset, JumpInfoHandle(info)));
    }

    // create a unique filename for the cache file before (atomically) moving it into place

    eckit::PathName uniqPath = eckit::PathName::unique(cachePath);

    LOG_DEBUG_LIB(LibGribJump) << "Writing GribInfo to temporary file " << uniqPath << std::endl;
    eckit::FileStream s(uniqPath, "w");
    s << cache;
    s.close();

    // atomically move the file into place
    eckit::PathName::rename(uniqPath, cachePath);

    LOG_DEBUG_LIB(LibGribJump) << "Inserted GribInfo for " << infos.size() << " new fields in " << cachePath << std::endl;
}


void GribInfoCache::scan(const eckit::PathName& fdbpath, const std::vector<eckit::Offset>& offs) {

    // this will be executed in parallel so we dont lock main mutex_ here
    // we will rely on each method to lock mutex when needed

    if (!cacheEnabled_) return;

    auto base = fdbpath.baseName();
    auto cachePath = cacheFilePath(base);

    // if cache exists load so we can merge with memory cache
    InfoCache& filecache = getFileCache(base);
    bool loaded = loadIntoCache(cachePath, filecache); 

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " at " << eckit::Plural(offs.size(), "offset") << std::endl;

    JumpHandle dataSource(fdbpath.fileHandle()); // takes ownership of handle
    
    std::vector<eckit::Offset> offsets(offs);
    std::sort(offsets.begin(), offsets.end()); // reorder offsets so we can seek through file in order
    
    bool dirty = false;
    std::lock_guard<std::recursive_mutex> lock(filecache.mutex_);
    infocache_t& cache = filecache.infocache_;
    for (const auto& offset : offsets) {
        
        if(cache.find(offset) != cache.end()) {
            continue; // already in cache, we assume is correct
        }

        dataSource.seek(offset);
        JumpInfo* info = dataSource.extractInfo();
        cache.insert(std::make_pair(offset, JumpInfoHandle(info)));
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
    s << cache;
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
