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

#include "fdb5/LibFdb5.h"

#include "gribjump/info/JumpInfo.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {


class GribInfoCache {

private: // types

    class FileCache {

        // owns the JumpInfo objects, all of which correspond to a single file.

    public:

        FileCache(const eckit::PathName& path): path_(path) {
            if (path_.exists()) {
                
                LOG_DEBUG_LIB(LibGribJump) << "Loading file cache from " << path_ << std::endl;

                eckit::FileStream s(path_, "r");
                decode(s);
                s.close();
            } else {
                eckit::Log::warning() << "Cache file " << path_ << " does not exist" << std::endl;
            }
        }

        FileCache(eckit::Stream& s) {
            decode(s);
        }

        ~FileCache() {
            std::lock_guard<std::recursive_mutex> lock(mutex_); // we're in trouble if we're being destructed while being used
            for (auto& entry : map_) {
                delete entry.second; 
            }
        }

        void encode(eckit::Stream& s) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            s << map_.size();
            for (auto& entry : map_) {
                s << entry.first;
                s << *entry.second;
            }
        }

        void decode(eckit::Stream& s) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            size_t size;
            s >> size;
            for (size_t i = 0; i < size; i++) {
                eckit::Offset offset;
                s >> offset;
                JumpInfo* info = eckit::Reanimator<JumpInfo>::reanimate(s);

                map_.insert(std::make_pair(offset, info));
            }
        }

        void merge(FileCache& other) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            other.lock();
            for (auto& entry : other.map()) {
                map_.insert(entry);
            }
            other.unlock();
        }

        void persist(bool merge=true) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);

            if (merge && path_.exists()) {
                // Load an existing cache and merge with this
                // Note: if same entry exists in both, the one in *this will be used
                FileCache other(path_);
                this->merge(other);
            }


            // create a unique filename for the cache file before (atomically) moving it into place
            /// @todo Chris ^ Why is this bit necessary?

            eckit::PathName uniqPath = eckit::PathName::unique(path_);

            LOG_DEBUG_LIB(LibGribJump) << "Writing GribInfo to temporary file " << uniqPath << std::endl;
            eckit::FileStream s(uniqPath, "w");
            encode(s);
            s.close();

            // atomically move the file into place
            LOG_DEBUG_LIB(LibGribJump) << "Moving temp file cache to " << path_ << std::endl;
            eckit::PathName::rename(uniqPath, path_);
        }

        void insert(eckit::Offset offset, JumpInfo* info) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            map_.insert(std::make_pair(offset, info));
        }


        void insert(std::vector<JumpInfo*> infos) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            for (auto& info : infos) {
                map_.insert(std::make_pair(info->msgStartOffset(), info));
            }
        }

        // wrapper around map_.find()
        JumpInfo* find(eckit::Offset offset) {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            auto it = map_.find(offset);
            if (it != map_.end()) {
                return it->second;
            }
            return nullptr;
        }

        size_t count() {
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            return map_.size();
        }

        void lock() { mutex_.lock(); }
        void unlock() { mutex_.unlock(); }

        const std::map<eckit::Offset, JumpInfo*>& map() const { return map_; }

    private:
        eckit::PathName path_;

    private:
        std::recursive_mutex mutex_; //< mutex for infocache_ // XXX Why recursive
        std::map<eckit::Offset, JumpInfo*> map_;
    };

    // typedef std::map<off_t, JumpInfoHandle> infocache_t; //< map fieldlocation's to gribinfo
    // typedef std::map<off_t, NewJumpInfo*> infocache_t; //< map fieldlocation's to gribinfo

    // struct InfoCache {
    //     std::recursive_mutex mutex_; //< mutex for infocache_        
    //     infocache_t infocache_;      //< map offsets in file to gribinfo
    // };

    typedef std::string filename_t;               //< key is fieldlocation's path basename
    // typedef std::map<filename_t, InfoCache*> cache_t; //< map fieldlocation's to gribinfo
    typedef std::map<filename_t, FileCache*> cache_t; //< map fieldlocation's to gribinfo

public:

    static GribInfoCache& instance();

    /// @brief Scans grib file at provided offsets and populates cache
    /// @param path full path to grib file
    /// @param offsets list of offsets to at which GribInfo should be extracted
    void scan(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);

    void scan(const eckit::PathName& path); // < scan all fields in a file


    /// Inserts a JumpInfo entry
    /// @param info JumpInfo to insert, takes ownership
    void insert(const eckit::PathName& path, const eckit::Offset offset, JumpInfo* info); // offset is redundant since it is in info
    void insert(const eckit::PathName& path, std::vector<JumpInfo*> infos);

    /// Get JumpInfo from memory cache
    /// @return JumpInfo, null if not found
    JumpInfo* get(const eckit::PathName& path, const eckit::Offset offset);
    JumpInfo* get(const eckit::URI& uri);

    std::vector<JumpInfo*> get(const eckit::PathName& path, const eckit::OffsetList& offsets); // this version will generate on the fly... inconsistent?

    void persist(bool merge=true);
    void clear();

    void print(std::ostream& s) const;

private: // methods

    GribInfoCache();
    
    ~GribInfoCache();

    FileCache& getFileCache(const filename_t& f);

    eckit::PathName cacheFilePath(const eckit::PathName& path) const;

private: // members

    eckit::PathName cacheDir_;

    mutable std::mutex mutex_; //< mutex for cache_
    cache_t cache_;

    bool persistentCache_ = false;
};

}  // namespace gribjump
