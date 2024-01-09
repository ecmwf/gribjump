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
    GribInfoCache();
    GribInfoCache(eckit::PathName dir);

    ~GribInfoCache();

    // Check if fieldlocation is in memory OR on disk (via manifest)
    // Will add to memory if not already there
    bool contains(const fdb5::FieldLocation& loc);

    // Get gribinfo from memory
    const JumpInfo& get(const fdb5::FieldLocation& loc);

    // Preload all gribinfos listed in manifest into memory
    void preload();

    void print(std::ostream& s) const;

    // Manifest maintenance
    bool lookup(const std::string& fdbfilename) const;
    void append(const std::string& fdbfilename, const std::string& gribinfofilename);
    void removeOld(int days);
    void dump() const;

private:

    eckit::PathName cacheDir_;
    
    // fieldlocation's fdb filename -> gribinfo filename
    std::map<std::string, std::string> manifest_; 

    // fieldlocation's full name -> gribinfo
    std::map<std::string, JumpInfo> cache_;

};

}  // namespace gribjump
