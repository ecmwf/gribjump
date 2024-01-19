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

    bool contains(const fdb5::FieldLocation& loc);

    void insert(const fdb5::FieldLocation& loc, const JumpInfo& info);

    // Get gribinfo from memory
    const JumpInfo& get(const fdb5::FieldLocation& loc);

    void print(std::ostream& s) const;

private:

    GribInfoCache();

    ~GribInfoCache();

private:

    mutable std::mutex mutex_;

    eckit::PathName cacheDir_;

    std::map<std::string, JumpInfo> cache_; //< map fieldlocation's full name to gribinfo

    bool cacheEnabled_ = false;
};

}  // namespace gribjump
