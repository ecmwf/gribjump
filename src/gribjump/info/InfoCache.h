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

#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/serialisation/Stream.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/LRUCache.h"

namespace gribjump {

class IndexFile;
class InfoCache {

private:  // types

    using filename_t   = std::string;                                        //< key is fieldlocation's path basename
    using fileoffset_t = std::string;                                        // filename+offset
    using infocache_t  = LRUCache<fileoffset_t, std::shared_ptr<JumpInfo>>;  //< map fieldlocation's to gribinfo

public:

    static InfoCache& instance();

    /// @brief Scans grib file at provided offsets and populates cache
    /// @param path full path to grib file
    /// @param offsets list of offsets to at which GribInfo should be extracted
    size_t scan(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);

    // if merge is true, we only generate jumpinfos for offsets that are not already in the cache file
    // if merge is false, we generate an entirely new cache file
    size_t scan(const eckit::PathName& path, bool merge = true);  // < scan all fields in a file


    /// Inserts a JumpInfo entry
    /// @param info JumpInfo to insert, takes ownership
    void insert(const eckit::PathName& path, const eckit::Offset offset, std::shared_ptr<JumpInfo> info);

    /// Get JumpInfo from memory cache
    /// @return JumpInfo, null if not found
    std::shared_ptr<JumpInfo> get(const eckit::PathName& path, const eckit::Offset offset);
    std::shared_ptr<JumpInfo> get(const eckit::URI& uri);

    std::vector<std::shared_ptr<JumpInfo>> get(
        const eckit::PathName& path,
        const eckit::OffsetList& offsets);  // this version will generate on the fly... inconsistent?

    void flush(bool append);
    void clear();

    void print(std::ostream& s) const;

private:  // methods

    InfoCache();

    ~InfoCache();

    std::shared_ptr<IndexFile> getIndexFile(const eckit::PathName& f);

    eckit::PathName cacheFilePath(const eckit::PathName& path) const;

    std::map<eckit::Offset, std::shared_ptr<JumpInfo>> getCached(const eckit::PathName& path,
                                                                 const eckit::OffsetList& offsets);
    void putCache(const eckit::PathName& path, const eckit::OffsetList& offset,
                  std::vector<std::shared_ptr<JumpInfo>>& infos);

private:  // members

    eckit::PathName cacheDir_;

    mutable std::mutex stageMutex_;  //< mutex for stagedFiles_
    std::map<filename_t, std::shared_ptr<IndexFile>>
        stagedFiles_;  ///, Files which we are actively appending to (plugin)

    mutable std::mutex infomutex_;
    ;  //< mutex for infocache_
    infocache_t infocache_;

    bool lazy_;  //< if true, cache.get may construct JumpInfo on the fly

    bool shadowCache_ = false;  //< if true, cache files are persisted next to the original data files (e.g. in FDB)
                                //  This takes precedence over cacheDir_.
};

// Holds JumpInfo objects belonging to single file.
class IndexFile {

    using infomap_t = std::map<eckit::Offset, std::shared_ptr<JumpInfo>>;
    friend class InfoCache;

public:

    IndexFile(const eckit::PathName& path, bool load = true);

    ~IndexFile();

    void load();
    void reload();
    void print(std::ostream& s);
    bool loaded() const { return loaded_; }

    // For tests only
    size_t size() const { return map_.size(); }

    std::map<eckit::Offset, std::shared_ptr<JumpInfo>> get(const eckit::OffsetList& offsets);

private:  // Methods are only intended to be called from InfoCache

    void encode(eckit::Stream& s) const;

    void decode(eckit::Stream& s);

    void merge(IndexFile& other);

    void write();
    void flush(bool append);
    void clear();

    void insert(eckit::Offset offset, std::shared_ptr<JumpInfo> info);

    void toNewFile(const eckit::PathName& path) const;
    void appendToFile(const eckit::PathName& path) const;
    void fromFile(const eckit::PathName& path);

    // wrapper around map_.find()
    std::shared_ptr<JumpInfo> find(eckit::Offset offset);

    size_t count();

    void lock() { mutex_.lock(); }
    void unlock() { mutex_.unlock(); }

    const infomap_t& map() const { return map_; }

    eckit::OffsetList offsets() const {
        eckit::OffsetList offsets;
        for (const auto& entry : map_) {
            offsets.push_back(entry.first);
        }
        return offsets;
    }

private:

    static constexpr uint8_t currentVersion_ = 1;
    uint8_t version_;

    eckit::PathName path_;
    bool loaded_ = false;
    mutable std::mutex mutex_;  //< mutex for map_
    infomap_t map_;
};

}  // namespace gribjump
