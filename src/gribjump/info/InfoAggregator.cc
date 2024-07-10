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

#include "gribjump/info/InfoAggregator.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump {

namespace {
URI convert(const eckit::URI& uri){
    return uri.asRawString();
}

} // anonymous namespace

void InfoAggregator::add(const fdb5::Key& key, const eckit::message::Message& message){

    JumpInfo* info = extractor_.extract(message);

    std::lock_guard<std::mutex> lock(mutex_);

    keyToJumpInfo[key] = info;

    if (keyToLocation.find(key) != keyToLocation.end()){
        const URI& location = keyToLocation[key];
        locationToJumpInfo[location] = info;
        count_++;
    }
}

// Store the location provided by fdb callback
void InfoAggregator::add(const fdb5::Key& key, const eckit::URI& location_in){
    URI location = convert(location_in);
    
    std::lock_guard<std::mutex> lock(mutex_);

    eckit::Offset offset(std::stoll(location_in.fragment()));
    locationToPathAndOffset[location] = {location_in.path(), offset};

    keyToLocation[key] = location;

    if (keyToJumpInfo.find(key) != keyToJumpInfo.end()){
        JumpInfo* info = keyToJumpInfo[key];
        locationToJumpInfo[location] = info;
        count_++;
    }
}

void InfoAggregator::flush(){
    // By calling flush, you are promising that you have added key,location,message in equal measure.
    // If we need to wait, then wait before flushing.
    // But if this is called *after* fdb flush, then we should be good (all location callbacks will have called).
    LOG_DEBUG_LIB(LibGribJump) << "GribJump::InfoAggregator::flush()" << std::endl;

    // debug print sizes
    LOG_DEBUG_LIB(LibGribJump) << "keyToLocation.size() = " << keyToLocation.size() << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "keyToJumpInfo.size() = " << keyToJumpInfo.size() << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "locationToJumpInfo.size() = " << locationToJumpInfo.size() << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "count_ = " << count_ << std::endl;

    ASSERT(keyToLocation.size() == count_);
    ASSERT(keyToJumpInfo.size() == count_);
    ASSERT(locationToJumpInfo.size() == count_);


    // Give to the gribjump cache to persist
    // Might need to do some cleverness to make sure we match the files to the correct JumpInfo.
    // Also... normally the cache would take ownership of the infos, this might not be sensible here.
    // Maybe we don't really want to use the cache as is.

    /*cache.persist(locationToJumpInfo)*/

    InfoCache& cache = InfoCache::instance();

    // NB: Being null is not an error, it just means the packing was not supported.
    size_t countNull = 0;
    for (const auto& [location, info] : locationToJumpInfo){
        if (!info) {
            countNull++;
            continue; // Skip nulls. TODO: In general, how do we want to handle nulls?
        }
        eckit::PathName path = locationToPathAndOffset[location].first;
        eckit::Offset offset = locationToPathAndOffset[location].second;
        // Perhaps use unique ptr and then std move here.
        cache.insert(path, offset, info); // Takes ownership of info // We could just create this instead of the locationToJumpInfo (if so, null handling needs to be inside cache...)

        LOG_DEBUG_LIB(LibGribJump) << "GribJump::InfoAggregator::flush: Inserting info" << " at " << path << " offset " << offset << std::endl;
    }

    cache.persist();

    LOG_DEBUG_LIB(LibGribJump)<< "GribJump::InfoAggregator::flush: " << countNull << " JumpInfo were null of " << count_ << std::endl;

    // Clear the maps
    // I don't think this is needed, this object should be destroyed after flush.
    keyToLocation.clear();
    keyToJumpInfo.clear();
    locationToJumpInfo.clear();
    locationToPathAndOffset.clear();
    count_ = 0;
}

} // namespace gribjump