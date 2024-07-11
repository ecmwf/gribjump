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

void InfoAggregator::add(const eckit::URI& uri, eckit::DataHandle& handle, eckit::Offset offset){

    handle.openForRead();
    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(handle, offset)); // memory handle starts at 0
    handle.close();

    insert(uri, std::move(info));
}

// Give to the cache
void InfoAggregator::insert(const eckit::URI& uri, std::unique_ptr<JumpInfo> info){
    eckit::Offset offset(std::stoll(uri.fragment()));
    eckit::PathName path = uri.path();

    InfoCache::instance().insert(path, offset, info.release()); // InfoCache takes ownership // TODO: use std move, whenever I merge develop in here.

}

void InfoAggregator::flush(){
    LOG_DEBUG_LIB(LibGribJump) << "GribJump::InfoAggregator::flush()" << std::endl;

    InfoCache::instance().persist();
}

} // namespace gribjump