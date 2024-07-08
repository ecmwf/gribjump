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
#include "eckit/io/DataHandle.h"

#include "gribjump/info/InfoAggregator.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump {

void InfoAggregator::add(const Key& key, eckit::DataHandle& handle){
    
    handle.openForRead();
    JumpInfo* info(InfoFactory::instance().build(handle, 0)); // memory handle starts at 0

    std::lock_guard<std::mutex> lock(mutex_);

    keyToJumpInfo[key] = info;

    if (keyToLocation.find(key) != keyToLocation.end()){
        const eckit::URI& uri = keyToLocation[key];
        /* We could probably release the lock now, before calling insert */
        insert(uri, info);
    }
}

// Store the location provided by fdb callback
void InfoAggregator::add(const Key& key, const eckit::URI& uri){
    std::lock_guard<std::mutex> lock(mutex_);

    keyToLocation[key] = uri;

    if (keyToJumpInfo.find(key) != keyToJumpInfo.end()){
        /* We could probably release the lock now, before calling insert */
        insert(uri, keyToJumpInfo[key]);
    }
}

// Give to the cache
void InfoAggregator::insert(const eckit::URI& uri, JumpInfo* info){
    eckit::Offset offset(std::stoll(uri.fragment()));
    eckit::PathName path = uri.path();

    InfoCache::instance().insert(path, offset, info); // cache handles its own locking

    count_++;
}

void InfoAggregator::flush(){
    // By calling flush, you are promising that you have added key,location,message in equal measure.
    // If we need to wait, then wait before flushing.
    // But if this is called *after* fdb flush, then we should be good (all location callbacks will have called).
    LOG_DEBUG_LIB(LibGribJump) << "GribJump::InfoAggregator::flush()" << std::endl;

    // Ensure both sides of the add have been called in equal measure
    ASSERT(keyToLocation.size() == count_);
    ASSERT(keyToJumpInfo.size() == count_);

    InfoCache::instance().persist();
}

} // namespace gribjump