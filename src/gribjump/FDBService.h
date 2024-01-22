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

#include <cstddef>
#include <mutex>

#include "eckit/thread/AutoLock.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

namespace gribjump {

class FDBService {
public:

    static FDBService& instance();

    void lock() { mutex_.lock(); locked_ = true; }
    void unlock() { mutex_.unlock(); locked_ = false; }

    fdb5::FDB& fdb() { ASSERT(locked_); return fdb_; }

    std::vector<eckit::URI> fieldLocations(const metkit::mars::MarsRequest& request);

private:

    FDBService();

    ~FDBService();

private:

    std::mutex mutex_;
    bool locked_ = false;

    fdb5::FDB fdb_;
};

} // namespace gribjump