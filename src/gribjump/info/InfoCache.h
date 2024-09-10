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

#pragma once

#include <map>

#include "eckit/serialisation/Stream.h"
#include "eckit/filesystem/URI.h"
#include "eckit/serialisation/FileStream.h"

#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/LRUCache.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {

class FileCache;
class InfoCache {

private: // types

    using filename_t = std::string; //< key is fieldlocation's path basename
    using cache_t = LRUCache<filename_t, std::shared_ptr<FileCache>>; //< map fieldlocation's to gribinfo

public:

    static InfoCache& instance();

    /// @brief Scans grib file at provided offsets and populates cache
    /// @param path full path to grib file
    /// @param offsets list of offsets to at which GribInfo should be extracted
    void scan(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);

    void scan(const eckit::PathName& path); // < scan all fields in a file


    /// Inserts a JumpInfo entry
    /// @param info JumpInfo to insert, takes ownership
    void insert(const eckit::PathName& path, const eckit::Offset offset, std::shared_ptr<JumpInfo> info);

    /// Get JumpInfo from memory cache
    /// @return JumpInfo, null if not found
    std::shared_ptr<JumpInfo>  get(const eckit::PathName& path, const eckit::Offset offset);
    std::shared_ptr<JumpInfo>  get(const eckit::URI& uri);

    std::vector<std::shared_ptr<JumpInfo>> get(const eckit::PathName& path, const eckit::OffsetList& offsets); // this version will generate on the fly... inconsistent?

    void flush(bool append);
    void clear();

    void print(std::ostream& s) const;

private: // methods

    InfoCache();
    
    ~InfoCache();

    std::shared_ptr<FileCache> getFileCache(const eckit::PathName& f, bool load=true);

    eckit::PathName cacheFilePath(const eckit::PathName& path) const;

private: // members

    eckit::PathName cacheDir_;

    mutable std::mutex mutex_; //< mutex for cache_
    cache_t cache_;

    bool persistentCache_ = true;

    bool shadowCache_ = false; //< if true, cache files are persisted next to the original data files (e.g. in FDB)
                               //  This takes precedence over cacheDir_.
};

// Holds JumpInfo objects belonging to single file.
class FileCache {

    using infomap_t = std::map<eckit::Offset, std::shared_ptr<JumpInfo>>;
    friend class InfoCache; 

public:

    FileCache(const eckit::PathName& path, bool load=true);

    ~FileCache();

    void load();
    void print(std::ostream& s);
    bool loaded() const { return loaded_; }

    // For tests only
    size_t size() const { return map_.size(); }

private: // Methods are only intended to be called from InfoCache

    void encode(eckit::Stream& s);

    void decode(eckit::Stream& s);

    void merge(FileCache& other);

    void write();
    void flush(bool append);
    void clear();

    void insert(eckit::Offset offset, std::shared_ptr<JumpInfo> info);

    void toNewFile(eckit::PathName path);
    void appendToFile(eckit::PathName path);
    void fromFile(eckit::PathName path);

    // wrapper around map_.find()
    std::shared_ptr<JumpInfo> find(eckit::Offset offset);

    size_t count();

    void lock() { mutex_.lock(); }
    void unlock() { mutex_.unlock(); }

    const infomap_t& map() const { return map_; }

private:

    static constexpr uint8_t currentVersion_ = 1;
    uint8_t version_;

    eckit::PathName path_;
    bool loaded_ = false;
    std::mutex mutex_; //< mutex for map_
    infomap_t map_;
};

}  // namespace gribjump
