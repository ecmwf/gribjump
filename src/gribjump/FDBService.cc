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

#include "gribjump/FDBService.h"

#include "eckit/log/Log.h"

#include "gribjump/GribJump.h"

namespace gribjump {

FDBService& FDBService::instance() {
    static FDBService instance_;
    return instance_;
}

FDBService::FDBService() {
}

FDBService::~FDBService() {
}

std::vector<eckit::URI> FDBService::fieldLocations(const metkit::mars::MarsRequest& request) {
    eckit::AutoLock<FDBService> lock(this);

    std::vector<eckit::URI> locations;

    fdb5::FDBToolRequest fdbreq(request);
    auto listIter = fdb_.list(fdbreq, false);
    fdb5::ListElement elem;
    while (listIter.next(elem)) {
            const fdb5::FieldLocation& loc = elem.location();
            locations.push_back(loc.fullUri());
            LOG_DEBUG_LIB(LibGribJump) << loc << std::endl;
        }
    return locations;
}

} // namespace gribjump
