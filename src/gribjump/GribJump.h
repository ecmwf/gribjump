/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifndef metkit_GribJump_H
#define metkit_GribJump_H

#include "gribjump/GribInfo.h"
#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"


namespace gribjump {

using PolyOutput = std::tuple<std::vector<std::vector<double>>, std::vector<std::vector<std::bitset<64>>>>;
using Range = std::tuple<size_t, size_t>;
using PolyRequest = std::vector<std::tuple<metkit::mars::MarsRequest, std::vector<Range>>>;

// Gribjump API
class GribJump : public eckit::NonCopyable {
public: 
    GribJump();
    ~GribJump();
    std::vector<std::vector<PolyOutput>> extractRemote(PolyRequest);
    std::vector<std::vector<PolyOutput>> extract(PolyRequest);
    std::vector<PolyOutput> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges);
    
    PolyOutput directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfo info) const;

    JumpInfo extractInfo(eckit::DataHandle* handle) const;

    bool isCached(std::string) const {return false;} // todo implement caching

private:
    // std::map<Key, std::tuple<FieldLocation*, JumpInfo> > cache_; // not imp
};


} // namespace GribJump
#endif // fdb5_gribjump_LibGribJump_H