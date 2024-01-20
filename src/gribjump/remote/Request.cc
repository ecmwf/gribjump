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

#include "eckit/log/Log.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/remote/Request.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

Task::Task() {}

Task::~Task() {}

//----------------------------------------------------------------------------------------------------------------------

Request::Request(eckit::Stream& stream) : client_(stream) {    
}

Request::~Request() {}

void Request::notify() {
    std::lock_guard<std::mutex> lock(m_);
    counter_++;
    cv_.notify_one();
}

void Request::waitForTasks() {
    ASSERT(ntasks_ > 0);
    LOG_DEBUG_LIB(LibGribJump) << "Waiting for tasks ..." << std::endl;
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [&]{return counter_ == ntasks_;});
}

}  // namespace gribjump
