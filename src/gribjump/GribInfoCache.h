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

#include "fdb5/database/FieldLocation.h"

#include "gribjump/GribInfo.h"

namespace gribjump {

class GribInfoCache {

private: // types

    typedef std::map<off_t, JumpInfoHandle> infocache_t; //< map fieldlocation's to gribinfo

    struct InfoCache {
        std::recursive_mutex mutex_; //< mutex for infocache_        
        infocache_t infocache_;      //< map offsets in file to gribinfo
    };

    typedef std::string filename_t;               //< key is fieldlocation's path basename
    typedef std::map<filename_t, InfoCache*> cache_t; //< map fieldlocation's to gribinfo

public:

    static GribInfoCache& instance();

    /// @brief Scans grib file at provided offsets and populates cache
    /// @param path full path to grib file
    /// @param offsets list of offsets to at which GribInfo should be extracted
    void scan(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);

    /// @brief Inserts JumpInfos associated to the path
    void insert(const eckit::PathName& path, const std::vector<JumpInfo*>& infos);

    bool contains(const fdb5::FieldLocation& loc);

    /// Inserts a JumpInfo entry
    /// @param loc field location
    /// @param info JumpInfo to insert, takes ownership
    void insert(const fdb5::FieldLocation& loc, JumpInfo* info);
    void insert(const eckit::URI& uri, const eckit::Offset offset, JumpInfo* info);
    void insert(const eckit::PathName& path, const eckit::Offset offset, JumpInfo* info);

    /// Get JumpInfo from memory cache
    /// @return JumpInfo, null if not found
    JumpInfo* get(const fdb5::FieldLocation& loc);
    JumpInfo* get(const eckit::URI& uri, const eckit::Offset offset);
    JumpInfo* get(const eckit::PathName& path, const eckit::Offset offset);


    void print(std::ostream& s) const;

    bool enabled() const { return cacheEnabled_; }

private: // methods

    GribInfoCache();

    ~GribInfoCache();

    InfoCache& getFileCache(const filename_t& f);

    eckit::PathName cacheFilePath(const eckit::PathName& path) const;

    /// Inserts a JumpInfo entry
    /// @param f filename
    /// @param offset offset in file
    /// @param info JumpInfo to insert, takes ownership
    void insert(const filename_t& f, off_t offset, JumpInfo* info);

    bool loadIntoCache(const eckit::PathName& cachePath, InfoCache& cache);

private: // members

    eckit::PathName cacheDir_;

    mutable std::mutex mutex_; //< mutex for cache_
    cache_t cache_;

    bool cacheEnabled_ = false;
};

}  // namespace gribjump
