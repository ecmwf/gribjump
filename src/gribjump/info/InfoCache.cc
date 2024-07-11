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


#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/config/Resource.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/exception/Exceptions.h"

#include "gribjump/info/InfoCache.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/info/InfoExtractor.h"

namespace gribjump {

const char* file_ext = ".gribjump";

//----------------------------------------------------------------------------------------------------------------------

InfoCache& InfoCache::instance() {
    static InfoCache instance_;
    return instance_;
}

InfoCache::~InfoCache() {
    for (auto& entry : cache_) {
        delete entry.second;
    }
}

InfoCache::InfoCache(): cacheDir_(eckit::PathName()) {

    static_assert(sizeof(off_t) == sizeof(eckit::Offset), "off_t and eckit::Offset must be the same size"); // dont think this is required anymore

    shadowCache_ = eckit::Resource<bool>("gribJumpFDBShadow;$GRIBJUMP_FDB_SHADOW", false);

    if (shadowCache_) return;

    std::string cache = eckit::Resource<std::string>("gribJumpCacheDir;$GRIBJUMP_CACHE_DIR", "");
    LOG_DEBUG_LIB(LibGribJump) << "Cache directory " << cache << std::endl;

    if(cache.empty()) {
        persistentCache_ = false; 

        LOG_DEBUG_LIB(LibGribJump) << "Warning, cache persistence is disabled" << std::endl;

        return;
    }

    cacheDir_ = eckit::PathName(cache);

    if (!cacheDir_.exists()) {
        throw eckit::BadValue("Cache directory " + cacheDir_ + " does not exist");
    }
}

eckit::PathName InfoCache::cacheFilePath(const eckit::PathName& path) const {
    if (shadowCache_) {
        return path + file_ext;
    }

    return cacheDir_ / path.baseName() + file_ext;
}

// FileCache& InfoCache::getFileCache(const filename_t& f) {
FileCache& InfoCache::getFileCache(const eckit::PathName& path) { 
    std::lock_guard<std::mutex> lock(mutex_);
    const filename_t& f = path.baseName();
    auto it = cache_.find(f);
    if(it != cache_.end()) return *(it->second);
    
    eckit::Log::info() << "New InfoCache entry for file " << f << std::endl;
    eckit::PathName cachePath = cacheFilePath(path);
    eckit::Log::info() << "Full cache path: " << cachePath << std::endl;
    FileCache* filecache = new FileCache(cachePath); // this will load the cache file into memory if it exists
    cache_.insert(std::make_pair(f, filecache));
    return *filecache;
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::URI& uri) {

    eckit::PathName path = uri.path();
    eckit::Offset offset = std::stoll(uri.fragment());

    return get(path, offset);
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::PathName& path, const eckit::Offset offset) {

    FileCache& filecache = getFileCache(path);

    // return it if in memory cache
    {   
        std::shared_ptr<JumpInfo> info = filecache.find(offset);
        if (info) return info;

        LOG_DEBUG_LIB(LibGribJump) << "InfoCache file " << path << " does not contain JumpInfo for field at offset " << offset << std::endl;

    }

    // Extract explicitly

    InfoExtractor extractor;
    std::shared_ptr<JumpInfo> info = extractor.extract(path, offset);

    filecache.insert(offset, info);

    return info;
}

std::vector<std::shared_ptr<JumpInfo>> InfoCache::get(const eckit::PathName& path, const eckit::OffsetList& offsets) {

    FileCache& filecache = getFileCache(path);

    std::vector<eckit::Offset> missingOffsets;

    for (const auto& offset : offsets) {
        if (!filecache.find(offset)) {
            missingOffsets.push_back(offset);
        }
    }

    if (!missingOffsets.empty()) {
        
        std::sort(missingOffsets.begin(), missingOffsets.end());
        InfoExtractor extractor;

        std::vector<std::unique_ptr<JumpInfo>> infos = extractor.extract(path, missingOffsets);
        for (size_t i = 0; i < infos.size(); i++) {
            filecache.insert(missingOffsets[i], std::move(infos[i]));
        }
    }

    std::vector<std::shared_ptr<JumpInfo>> result;

    for (const auto& offset : offsets) {
        std::shared_ptr<JumpInfo> info = filecache.find(offset);
        ASSERT(info);
        result.push_back(info);
    }

    return result;
}

// XXX: is this true? We do allow creation of infos via other means.
// I think I have too many methods that do the same thing now, could do with a cleanup.

// maybe insert should be private.
// Only InfoCache will do insertions. Client code can only "get", which may in the process insert into cache

void InfoCache::insert(const eckit::PathName& path, const eckit::Offset offset, std::shared_ptr<JumpInfo>  info) {
    LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << path << ":" << offset << std::endl;
    FileCache& filecache = getFileCache(path);
    filecache.insert(offset, info);
}


// void InfoCache::insert(const eckit::PathName& path, std::vector<std::shared_ptr<JumpInfo>> infos) {
//     LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << path << "" << infos.size() << " fields" << std::endl;
//     FileCache& filecache = getFileCache(path);
//     filecache.insert(infos);
// }


void InfoCache::persist(bool merge){
    if (!persistentCache_) {
        LOG_DEBUG_LIB(LibGribJump) << "Warning, InfoCache::persist called but cache persistence is disabled. Returning." << std::endl;
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    size_t filecount = 0;
    for (auto& [filename, filecache] : cache_) {
        filecache->persist(merge);
        filecount++;
    }

    LOG_DEBUG_LIB(LibGribJump) << "Persisted " << filecount << " files" << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "cache_ size " << cache_.size() << std::endl;
}

void InfoCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (auto& entry : cache_) {
        delete entry.second;
    }

    cache_.clear();
}

void InfoCache::scan(const eckit::PathName& fdbpath, const std::vector<eckit::Offset>& offsets) {

    // this will be executed in parallel so we dont lock main mutex_ here
    // we will rely on each method to lock mutex when needed

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " at " << eckit::Plural(offsets.size(), "offsets") << std::endl;


    // if cache exists load so we can merge with memory cache
    FileCache& filecache = getFileCache(fdbpath);

    // Find which offsets are not already in file cache
    std::vector<eckit::Offset> newOffsets;

    for (const auto& offset : offsets) {
        if(!filecache.find(offset)) {
            newOffsets.push_back(offset);
        }
    }

    if (newOffsets.empty()) {
        LOG_DEBUG_LIB(LibGribJump) << "No new fields to scan in " << fdbpath << std::endl;
        return;
    }

    std::sort(newOffsets.begin(), newOffsets.end());

    InfoExtractor extractor;
    std::vector<std::unique_ptr<JumpInfo>> uinfos = extractor.extract(fdbpath, newOffsets);
    // std::vector<std::shared_ptr<JumpInfo>> infos;
    // infos.reserve(uinfos.size());
    // std::move(uinfos.begin(), uinfos.end(), std::back_inserter(infos));
    // filecache.insert(infos);
    for (size_t i = 0; i < uinfos.size(); i++) {
        filecache.insert(newOffsets[i], std::move(uinfos[i]));
    }
    
    if (persistentCache_) {
        filecache.persist();
    }

}

void InfoCache::scan(const eckit::PathName& fdbpath) {


    LOG_DEBUG_LIB(LibGribJump) << "Scanning whole file " << fdbpath << std::endl;

    // if cache exists load so we can merge with memory cache
    FileCache& filecache = getFileCache(fdbpath);

    InfoExtractor extractor;
    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> uinfos = extractor.extract(fdbpath); /* This needs to give use the offsets too*/
    for (size_t i = 0; i < uinfos.size(); i++) {
        filecache.insert(uinfos[i].first, std::move(uinfos[i].second));
    }

    if (persistentCache_) {
        filecache.persist();
    }

}

void InfoCache::print(std::ostream& s) const {
    std::lock_guard<std::mutex> lock(mutex_);
    // Print the manifest, then the cache
    s << "InfoCache[";
    s << "cacheDir=" << cacheDir_ << std::endl;
    s << "cache=" << std::endl;
    for (auto& entry : cache_) {
        s << entry.first << " -> " << entry.second << std::endl;
    }
    s << "]";
}


// ------------------------------------------------------------------------------------------------------

FileCache::FileCache(const eckit::PathName& path): path_(path) {
    if (path_.exists()) {
        
        LOG_DEBUG_LIB(LibGribJump) << "Loading file cache from " << path_ << std::endl;

        eckit::FileStream s(path_, "r");
        decode(s);
        s.close();
    } else {
        LOG_DEBUG_LIB(LibGribJump) << "Cache file " << path_ << " does not exist" << std::endl;
    }
}

FileCache::FileCache(eckit::Stream& s) {
    decode(s);
}

FileCache::~FileCache() {
}

void FileCache::encode(eckit::Stream& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    s << map_.size();
    for (auto& entry : map_) {
        s << entry.first;
        s << *entry.second;
    }
}

void FileCache::decode(eckit::Stream& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t size;
    s >> size;
    for (size_t i = 0; i < size; i++) {
        eckit::Offset offset;
        s >> offset;
        std::unique_ptr<JumpInfo> info(eckit::Reanimator<JumpInfo>::reanimate(s));

        map_.insert(std::make_pair(offset, std::move(info)));
    }
}

void FileCache::merge(FileCache& other) {
    std::lock_guard<std::mutex> lock(mutex_);
    other.lock();
    for (auto& entry : other.map()) {
        map_.insert(entry);
    }
    other.unlock();
}

void FileCache::persist(bool merge) {


    if (merge && path_.exists()) {
        // Load an existing cache and merge with this
        // Note: if same entry exists in both, the one in *this will be used
        FileCache other(path_);
        this->merge(other);
    }

    // create a unique filename for the cache file before (atomically) moving it into place

    eckit::PathName uniqPath = eckit::PathName::unique(path_);

    LOG_DEBUG_LIB(LibGribJump) << "Writing GribInfo to temporary file " << uniqPath << std::endl;
    eckit::FileStream s(uniqPath, "w");
    encode(s);
    s.close();

    // atomically move the file into place
    LOG_DEBUG_LIB(LibGribJump) << "Moving temp file cache to " << path_ << std::endl;
    eckit::PathName::rename(uniqPath, path_);
}

void FileCache::insert(eckit::Offset offset, std::shared_ptr<JumpInfo> info) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.insert(std::make_pair(offset, info));
}


std::shared_ptr<JumpInfo> FileCache::find(eckit::Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(offset);
    if (it != map_.end()) {
        return it->second;
    }
    return nullptr;
}

size_t FileCache::count() {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

void FileCache::print(std::ostream& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    s << "FileCache[" << path_ << " (" << map_.size() << " entries)]:" << std::endl;
    for (auto& entry : map_) {
        s << "  Offset:" << entry.first << " -> " << *entry.second << std::endl;
    }
}

}  // namespace gribjump
