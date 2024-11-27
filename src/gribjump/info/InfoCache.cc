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

namespace {
    std::string cachekey(const eckit::PathName& path, const eckit::Offset& offset) {
        return path.baseName() + std::to_string(offset);
    }
}

//----------------------------------------------------------------------------------------------------------------------

InfoCache& InfoCache::instance() {
    static InfoCache instance_;
    return instance_;
}

InfoCache::~InfoCache() {
}

InfoCache::InfoCache(): 
    cacheDir_(eckit::PathName()), 
    infocache_(eckit::Resource<int>("gribjumpCacheSize", LibGribJump::instance().config().getInt("cache.size", 1024))),
    lazy_(eckit::Resource<bool>("gribjumpLazyInfo", LibGribJump::instance().config().getBool("cache.lazy", true))) {

    const Config& config = LibGribJump::instance().config();

    bool enabled = config.getBool("cache.enabled", true);
    if (!enabled) {
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

std::shared_ptr<IndexFile> InfoCache::getIndexFile(const eckit::PathName& path) { 

    const filename_t& f = path.baseName();
    eckit::PathName cachePath = cacheFilePath(path);
    auto filecache = std::make_shared<IndexFile>(cachePath, false);
    return filecache;
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::URI& uri) {

    eckit::PathName path = uri.path();
    eckit::Offset offset = std::stoll(uri.fragment());

    return get(path, offset);
}

std::shared_ptr<JumpInfo> InfoCache::get(const eckit::PathName& path, const eckit::Offset offset) {
    return get(path, eckit::OffsetList{offset})[0];
}

std::map<eckit::Offset, std::shared_ptr<JumpInfo>> InfoCache::getCached(const eckit::PathName& path, const eckit::OffsetList& offsets) {
    std::lock_guard<std::mutex> lock(infomutex_);
    std::map<eckit::Offset, std::shared_ptr<JumpInfo>> result;
    for (const auto& offset : offsets) {
        std::string key = cachekey(path, offset);
        if (infocache_.exists(key)) {
            result[offset] = infocache_.get(key);
        }
    }
    return result;
}

void InfoCache::putCache(const eckit::PathName& path, const eckit::OffsetList& offset, std::vector<std::shared_ptr<JumpInfo>>& infos) {
    std::lock_guard<std::mutex> lock(infomutex_);
    for (size_t i = 0; i < offset.size(); i++) {
        infocache_.put(cachekey(path, offset[i]), infos[i]);
    }
}

std::vector<std::shared_ptr<JumpInfo>> InfoCache::get(const eckit::PathName& path, const eckit::OffsetList& offsets) {

    std::map<eckit::Offset, std::shared_ptr<JumpInfo>> result = getCached(path, offsets);

    if (result.size() != offsets.size()) {

        // Find the missing offsets and read from index file
        std::vector<eckit::Offset> fileOffsets;
        for (const auto& offset : offsets) {
            if (result.find(offset) == result.end()) {
                fileOffsets.push_back(offset);
            }
        }

        // Open the index file and find the missing offsets
        std::shared_ptr<IndexFile> indexFile = getIndexFile(path);
        std::map<eckit::Offset, std::shared_ptr<JumpInfo>> fileinfos = indexFile->get(fileOffsets);

        std::vector<eckit::Offset> missingOffsets;
        for (const auto& offset : fileOffsets) {
            if (fileinfos.find(offset) == fileinfos.end()) {
                missingOffsets.push_back(offset);
                continue;
            }
            result[offset] = fileinfos[offset];
        }

        // If some offsets are also missing from index file, extract from data file directly.
        if (!missingOffsets.empty()) {
            if (!lazy_) {
                std::stringstream ss;
                ss << "Missing JumpInfo for " << eckit::Plural(missingOffsets.size(), "offset") << " in " << path;
                throw JumpInfoExtractionDisabled(ss.str());
            }
            InfoExtractor extractor;
            std::vector<std::unique_ptr<JumpInfo>> uinfos = extractor.extract(path, missingOffsets);
            for (size_t i = 0; i < uinfos.size(); i++) {
                std::shared_ptr<JumpInfo> info = std::move(uinfos[i]);
                ASSERT(info);
                result[missingOffsets[i]] = info;
            }
        }
    }

    ASSERT(result.size() == offsets.size());

    std::vector<std::shared_ptr<JumpInfo>> vec;
    vec.reserve(offsets.size());
    for (const auto& offset : offsets) {
        vec.push_back(result[offset]);
    }
    
    putCache(path, offsets, vec);
    return vec;
}

void InfoCache::insert(const eckit::PathName& path, const eckit::Offset offset, std::shared_ptr<JumpInfo> info) {
    LOG_DEBUG_LIB(LibGribJump) << "GribJumpCache inserting " << path << ":" << offset << std::endl;
    std::lock_guard<std::mutex> lock(stageMutex_);

    eckit::PathName filePath = cacheFilePath(path);
    if (stagedFiles_.find(filePath) != stagedFiles_.end()) {
        stagedFiles_[filePath]->insert(offset, info);
        return;
    }

    std::shared_ptr<IndexFile> filecache = getIndexFile(path);
    filecache->insert(offset, info);
    stagedFiles_[filePath] = filecache;
}

void InfoCache::flush(bool append) {
    std::lock_guard<std::mutex> lock(stageMutex_);
    for (auto& [filename, filecache] : stagedFiles_) {
        filecache->flush(append);
    }
    stagedFiles_.clear();
}

void InfoCache::clear() {
    std::lock_guard<std::mutex> lock(infomutex_);
    infocache_.clear();
}

size_t InfoCache::scan(const eckit::PathName& fdbpath, const std::vector<eckit::Offset>& offsets) {

    // this will be executed in parallel so we dont lock main mutex_ here
    // we will rely on each method to lock mutex when needed

    LOG_DEBUG_LIB(LibGribJump) << "Scanning " << fdbpath << " at " << eckit::Plural(offsets.size(), "offsets") << std::endl;

    std::shared_ptr<IndexFile> filecache = getIndexFile(fdbpath);
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
    
    filecache->write();

    return infos.size();
}

/// @todo maybe merge this with the above method
size_t InfoCache::scan(const eckit::PathName& fdbpath) {

    LOG_DEBUG_LIB(LibGribJump) << "Scanning whole file " << fdbpath << std::endl;

    std::shared_ptr<IndexFile> filecache = getIndexFile(fdbpath);
    filecache->reload();

    InfoExtractor extractor;
    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> infos = extractor.extract(fdbpath);
    for (size_t i = 0; i < infos.size(); i++) {
        filecache->insert(infos[i].first, std::move(infos[i].second));
    }

    filecache->write();

    return infos.size();
}

void InfoCache::print(std::ostream& s) const {
    std::lock_guard<std::mutex> lock(infomutex_);

    s << "InfoCache[";
    s << "cacheDir=" << cacheDir_ << std::endl;
    s << "cache=" << std::endl;
    for (const auto& [key, info] : infocache_) {
        s << "  " << key << ": ";
        info->print(s);
    }
    s << "]";
}


// ------------------------------------------------------------------------------------------------------

IndexFile::IndexFile(const eckit::PathName& path, bool autoload): path_(path) {
    if (autoload) load();
}

IndexFile::~IndexFile() {
}

void IndexFile::load() {
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
void IndexFile::reload() {
    clear();
    load();
}

void IndexFile::encode(eckit::Stream& s) const {
    std::lock_guard<std::mutex> lock(mutex_);

    s << currentVersion_;
    for (auto& entry : map_) {
        s.startObject();
        s << entry.first;
        s << *entry.second;
        s.endObject();
    }
}

void IndexFile::decode(eckit::Stream& s) {
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

void IndexFile::toNewFile(const eckit::PathName& path) const {
    ASSERT(path.extension() == file_ext);
    eckit::FileStream s(path, "w");
    encode(s);
    s.close();
}

void IndexFile::appendToFile(const eckit::PathName& path) const {
    ASSERT(path.extension() == file_ext);
    // NB: Non-atomic. Gribjump does not support concurrent writes to the same file from different processes.
    if (!path.exists()) {
        toNewFile(path);
        return;
    }
    
    LOG_DEBUG_LIB(LibGribJump) << "IndexFile appending to file " << path << std::endl;
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

void IndexFile::fromFile(const eckit::PathName& path) {
    eckit::FileStream s(path, "r");
    decode(s);
    s.close();
}

void IndexFile::merge(IndexFile& other) {
    std::lock_guard<std::mutex> lock(mutex_);
    other.lock();
    // NB: If there are duplicates, the entries from *this will take precedence over other
    for (auto& entry : other.map()) {
        map_.insert(entry);
    }
    other.unlock();
}

void IndexFile::write() {

    // create a unique filename for the cache file before (atomically) moving it into place
    eckit::PathName uniqPath = eckit::PathName::unique(path_) + file_ext;
    toNewFile(uniqPath);

    // atomically move the file into place
    LOG_DEBUG_LIB(LibGribJump) << "IndexFile writing to file " << path_ << std::endl;
    
    ASSERT(path_.extension() == file_ext);
    eckit::PathName::rename(uniqPath, path_);
}

void IndexFile::flush(bool append) {
    if (append){
        appendToFile(path_);
    }
    else {
        write();
    }
    clear();
}

void IndexFile::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    loaded_ = false;
}

void IndexFile::insert(eckit::Offset offset, std::shared_ptr<JumpInfo> info) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.insert(std::make_pair(offset, info));
}


std::shared_ptr<JumpInfo> IndexFile::find(eckit::Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(offset);
    if (it != map_.end()) {
        return it->second;
    }
    return nullptr;
}

size_t IndexFile::count() {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

void IndexFile::print(std::ostream& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    s << "IndexFile[" << path_ << " (" << map_.size() << " entries)]:" << std::endl;
    for (auto& entry : map_) {
        s << "  Offset:" << entry.first << " -> " << *entry.second << std::endl;
    }
}

std::map<eckit::Offset, std::shared_ptr<JumpInfo>> IndexFile::get(const eckit::OffsetList& offsets) {
    load();
    std::map<eckit::Offset, std::shared_ptr<JumpInfo>> result;
    for (const auto& offset : offsets) {
        if (auto info = find(offset)) {
            result.insert(std::make_pair(offset, info));
        }
    }
    return result;    
}

}  // namespace gribjump
