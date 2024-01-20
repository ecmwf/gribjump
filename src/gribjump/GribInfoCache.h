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

public:

    static GribInfoCache& instance();

    /// @brief Scans grib file and populates cache
    /// @param path full path to grib file
    /// @param offsets list of offsets to at which GribInfo should be extracted
    static void scan(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);

    bool contains(const fdb5::FieldLocation& loc);

    void insert(const fdb5::FieldLocation& loc, const JumpInfo& info);

    /// Get gribinfo from memory cache
    /// Assumes contains() has been called
    /// @return JumpInfo
    const JumpInfo& get(const fdb5::FieldLocation& loc);

    void print(std::ostream& s) const;

    bool enabled() const { return cacheEnabled_; }

private:

    GribInfoCache();

    ~GribInfoCache();

    eckit::PathName infoFilePath(const eckit::PathName& path) const;

private:

    typedef std::string key_t; //< key is fieldlocation's path and offset
    typedef std::map<key_t, JumpInfo> cache_t; //< map fieldlocation's to gribinfo

    mutable std::mutex mutex_; //< mutex for cache_

    eckit::PathName cacheDir_;

    cache_t cache_;

    bool cacheEnabled_ = false;
};

}  // namespace gribjump
