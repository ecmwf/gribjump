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
#include "gribjump/GribJumpException.h"

namespace gribjump {

const char* file_ext = ".gribjump";

//----------------------------------------------------------------------------------------------------------------------

InfoCache& InfoCache::instance() {
    static InfoCache instance_;
    return instance_;
}

InfoCache::~InfoCache() {
}

InfoCache::InfoCache(): 
    cacheDir_(eckit::PathName()), 
    cache_(eckit::Resource<int>("gribjumpCacheSize", LibGribJump::instance().config().getInt("cache.size", 64))),
    lazy_(eckit::Resource<bool>("gribjumpLazyInfo", LibGribJump::instance().config().getBool("cache.lazy", true))) {

    const Config& config = LibGribJump::instance().config();

    bool enabled = config.getBool("cache.enabled", true);
    if (!enabled) {
        persistentCache_ = false; 
        LOG_DEBUG_LIB(LibGribJump) << "Cache disabled" << std::endl;
        return;
    }

    std::string cache_str = config.getString("cache.directory", "");
    shadowCache_ = config.getBool("cache.shadowfdb", cache_str.empty());
    if (shadowCache_) {
        LOG_DEBUG_LIB(LibGribJump) << "Shadow FDB cache enabled" << std::endl;    
        return;
    }

    // Shadow FDB has been explicitly disabled
    if (cache_str.empty()) {
        throw eckit::BadValue("Cache directory not set");
    }

    cacheDir_ = eckit::PathName(cache_str);
    if (!cacheDir_.exists()) {
        throw eckit::BadValue("Cache directory " + cacheDir_ + " does not exist");
    }

    LOG_DEBUG_LIB(LibGribJump) << "Using cache directory: " << cacheDir_ << std::endl;
}

eckit::PathName InfoCache::cacheFilePath(const eckit::PathName& path) const {
    if (shadowCache_) {
        return path + file_ext;
    }

    return cacheDir_ / path.baseName() + file_ext;
}

std::shared_ptr<FileCache> InfoCache::getFileCache(const eckit::PathName& path, bool load) { 
    std::lock_guard<std::mutex> lock(mutex_);

    const filename_t& f = path.baseName();

    if (cache_.exists(f)) {
        return cache_.get(f);
    }

    eckit::PathName cachePath = cacheFilePath(path);
    LOG_DEBUG_LIB(LibGribJump) << "New InfoCache entry for file " << f << " at " << cachePath << std::endl;
    
    auto filecache = std::make_shared<FileCache>(cachePath, load);
    cache_.put(f, filecache);
    return filecache;
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::URI& uri) {

    eckit::PathName path = uri.path();
    eckit::Offset offset = std::stoll(uri.fragment());

    return get(path, offset);
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::PathName& path, const eckit::Offset offset) {

    std::shared_ptr<FileCache> filecache = getFileCache(path);
    filecache->reloadMissing({offset});

    // return it if in memory cache
    {   
        std::shared_ptr<JumpInfo> info = filecache->find(offset);
        if (info) return info;

        LOG_DEBUG_LIB(LibGribJump) << "InfoCache file " << path << " does not contain JumpInfo for field at offset " << offset << std::endl;
    }

    // Extract explicitly
    if (!lazy_) {
        throw JumpInfoExtractionDisabled("No JumpInfo found for path " + path + " at offset " + std::to_string(offset));
    }

    InfoExtractor extractor;
    std::shared_ptr<JumpInfo> info = extractor.extract(path, offset);

    filecache->insert(offset, info);

    return info;
}

std::vector<std::shared_ptr<JumpInfo>> InfoCache::get(const eckit::PathName& path, const eckit::OffsetList& offsets) {

    std::shared_ptr<FileCache> filecache = getFileCache(path);
    std::vector<eckit::Offset> missingOffsets = filecache->reloadMissing(offsets);

    if (!missingOffsets.empty()) {
        if (!lazy_) {
            std::stringstream ss;
            ss << "Missing JumpInfo for " << eckit::Plural(missingOffsets.size(), "offset") << " in " << path;
            throw JumpInfoExtractionDisabled(ss.str());
        }
        std::sort(missingOffsets.begin(), missingOffsets.end());
        InfoExtractor extractor;

        std::vector<std::unique_ptr<JumpInfo>> infos = extractor.extract(path, missingOffsets);
        for (size_t i = 0; i < infos.size(); i++) {
            filecache->insert(missingOffsets[i], std::move(infos[i]));
        }
    }

    std::vector<std::shared_ptr<JumpInfo>> result;

    for (const auto& offset : offsets) {
        std::shared_ptr<JumpInfo> info = filecache->find(offset);
        ASSERT(info);
        result.push_back(info);
    }

    return result;
}

// XXX: is this true? We do allow creation of infos via other means.
// I think I have too many methods that do the same thing now, could do with a cleanup.

// maybe insert should be private.
// Only InfoCache will do insertions. Client code can only "get", which may in the process insert into cache

void InfoCache::insert(const eckit::PathName& path, const eckit::Offset offset, std::shared_ptr<JumpInfo> info) {
    LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << path << ":" << offset << std::endl;
    std::shared_ptr<FileCache> filecache = getFileCache(path, false);
    filecache->insert(offset, info);
}

void InfoCache::flush(bool append) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [filename, filecache] : cache_) {
        filecache->flush(append);
    }
}

void InfoCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}

size_t InfoCache::scan(const eckit::PathName& fdbpath, const std::vector<eckit::Offset>& offsets) {

    // this will be executed in parallel so we dont lock main mutex_ here
    // we will rely on each method to lock mutex when needed

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " at " << eckit::Plural(offsets.size(), "offsets") << std::endl;

    // if cache exists load so we can merge with memory cache
    std::shared_ptr<FileCache> filecache = getFileCache(fdbpath);
    filecache->load();

    // Find which offsets are not already in file cache
    std::vector<eckit::Offset> newOffsets;

    for (const auto& offset : offsets) {
        if(!filecache->find(offset)) {
            newOffsets.push_back(offset);
        }
    }

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " found " << newOffsets.size() << " new fields not already in cache" << std::endl;

    if (newOffsets.empty()) {
        LOG_DEBUG_LIB(LibGribJump) << "No new fields to scan in " << fdbpath << std::endl;
        return 0;
    }

    std::sort(newOffsets.begin(), newOffsets.end());

    InfoExtractor extractor;
    std::vector<std::unique_ptr<JumpInfo>> infos = extractor.extract(fdbpath, newOffsets);
    // std::vector<std::shared_ptr<JumpInfo>> infos;
    // infos.reserve(uinfos.size());
    // std::move(uinfos.begin(), uinfos.end(), std::back_inserter(infos));
    // filecache->insert(infos);
    for (size_t i = 0; i < infos.size(); i++) {
        filecache->insert(newOffsets[i], std::move(infos[i]));
    }
    
    if (persistentCache_) {
        filecache->write();
    }

    return infos.size();
}

size_t InfoCache::scan(const eckit::PathName& fdbpath) {

    LOG_DEBUG_LIB(LibGribJump) << "Scanning whole file " << fdbpath << std::endl;

    // if cache exists load so we can merge with memory cache
    std::shared_ptr<FileCache> filecache = getFileCache(fdbpath);
    filecache->load();

    InfoExtractor extractor;
    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> infos = extractor.extract(fdbpath);
    for (size_t i = 0; i < infos.size(); i++) {
        filecache->insert(infos[i].first, std::move(infos[i].second));
    }

    if (persistentCache_) {
        filecache->write();
    }

    return infos.size();
}

void InfoCache::print(std::ostream& s) const {
    std::lock_guard<std::mutex> lock(mutex_);

    s << "InfoCache[";
    s << "cacheDir=" << cacheDir_ << std::endl;
    s << "cache=" << std::endl;
    for (const auto& [filename, filecache] : cache_) {
        s << "  " << filename << ": ";
        filecache->print(s);
    }
    s << "]";
}


// ------------------------------------------------------------------------------------------------------

FileCache::FileCache(const eckit::PathName& path, bool autoload): path_(path) {
    if (autoload) load();
}

FileCache::~FileCache() {
}

void FileCache::load() {
    if (loaded()) return;

    if (path_.exists()) {
        LOG_DEBUG_LIB(LibGribJump) << "Loading file cache from " << path_ << std::endl;
        fromFile(path_);
    } else {
        LOG_DEBUG_LIB(LibGribJump) << "Cache file " << path_ << " does not exist" << std::endl;
    }

    loaded_ = true;
}

// e.g. if the file on disk has been updated
void FileCache::reload() {
    clear();
    load();
}

void FileCache::encode(eckit::Stream& s) {
    std::lock_guard<std::mutex> lock(mutex_);

    s << currentVersion_;
    for (auto& entry : map_) {
        s.startObject();
        s << entry.first;
        s << *entry.second;
        s.endObject();
    }
}

void FileCache::decode(eckit::Stream& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    s >> version_;
    ASSERT(version_ == currentVersion_);

    size_t count = 0;
    while (s.next()) {
        eckit::Offset offset;
        s >> offset;
        std::unique_ptr<JumpInfo> info(eckit::Reanimator<JumpInfo>::reanimate(s));
        map_.insert(std::make_pair(offset, std::move(info)));
        count++;
    }
    LOG_DEBUG_LIB(LibGribJump) << "Loaded " << count << " entries from stream" << std::endl;
}

void FileCache::toNewFile(eckit::PathName path){
    eckit::FileStream s(path, "w");
    encode(s);
    s.close();
}

void FileCache::appendToFile(eckit::PathName path) {
    // NB: Non-atomic. Gribjump does not support concurrent writes to the same file from different processes.
    if (!path.exists()) {
        toNewFile(path);
        return;
    }
    
    LOG_DEBUG_LIB(LibGribJump) << "FileCache appending to file " << path << std::endl;
    // NB: appending does not re-write version information.
    std::lock_guard<std::mutex> lock(mutex_);
    eckit::FileStream s(path, "a");
    for (auto& entry : map_) {
        s.startObject();
        s << entry.first;
        s << *entry.second;
        s.endObject();
    }
    s.close();
}

void FileCache::fromFile(eckit::PathName path) {
    eckit::FileStream s(path, "r");
    decode(s);
    s.close();
}

void FileCache::merge(FileCache& other) {
    std::lock_guard<std::mutex> lock(mutex_);
    other.lock();
    // NB: If there are duplicates, the entries from *this will take precedence over other
    for (auto& entry : other.map()) {
        map_.insert(entry);
    }
    other.unlock();
}

void FileCache::write() {

    // create a unique filename for the cache file before (atomically) moving it into place
    eckit::PathName uniqPath = eckit::PathName::unique(path_);

    LOG_DEBUG_LIB(LibGribJump) << "Writing GribInfo to temporary file " << uniqPath << std::endl;
    toNewFile(uniqPath);

    // atomically move the file into place
    LOG_DEBUG_LIB(LibGribJump) << "Moving temp file cache to " << path_ << std::endl;
    eckit::PathName::rename(uniqPath, path_);
}

void FileCache::flush(bool append) {
    if (append){
        appendToFile(path_);
    }
    else {
        write();
    }
    clear();
}

void FileCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    loaded_ = false;
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

// Reload the cache if any are missing offsets e.g. because the file has been updated
std::vector<eckit::Offset> FileCache::reloadMissing(const eckit::OffsetList& offsets) {

    // Check if any are missing and reload if necessary
    size_t j=offsets.size();
    for (size_t i = 0; i < offsets.size(); i++) {
        const auto& offset = offsets[i];
        if (!find(offset)) {
            reload();
            j=i;
            break;
        }
    }

    // Find if any are still missing. We assume previously found offsets have not been removed.
    std::vector<eckit::Offset> missingOffsets;
    for (size_t i = j; i < offsets.size(); i++) {
        const auto& offset = offsets[i];
        if (!find(offset)) {
            missingOffsets.push_back(offset);
        }
    }

    return missingOffsets;
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
