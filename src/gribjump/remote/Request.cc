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
#include "eckit/log/Plural.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/remote/Request.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

Task::Task(size_t taskid) : taskid_(taskid) {}

Task::~Task() {}

//----------------------------------------------------------------------------------------------------------------------

Request::Request(eckit::Stream& stream) : client_(stream) {    
}

Request::~Request() {}

void Request::notify(size_t taskid) {
    std::lock_guard<std::mutex> lock(m_);
    taskStatus_[taskid] = Task::Status::DONE;
    counter_++;
    cv_.notify_one();
}

void Request::notifyError(size_t taskid, const std::string& s) {
    std::lock_guard<std::mutex> lock(m_);
    taskStatus_[taskid] = Task::Status::FAILED;
    errors_.push_back(s);
    counter_++;
    cv_.notify_one();
}

void Request::waitForTasks() {
    ASSERT(taskStatus_.size() > 0);
    LOG_DEBUG_LIB(LibGribJump) << "Waiting for " << eckit::Plural(taskStatus_.size(), "task") << "..." << std::endl;
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [&]{return counter_ == taskStatus_.size();});
}

void Request::reportErrors() {
    client_ << errors_.size();
    for (const auto& s : errors_) {
        client_ << s;
    }
}

}  // namespace gribjump
