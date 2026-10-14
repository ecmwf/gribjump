/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/TaskWait.h"

#include <utility>

namespace gribjump {

thread_local TaskWaitScope* TaskWaitScope::current_ = nullptr;

TaskWaitScope::TaskWaitScope(std::function<void()> check) : check_(std::move(check)), previous_(current_) {
    current_ = this;
}

TaskWaitScope::~TaskWaitScope() {
    current_ = previous_;
}

bool TaskWaitScope::active() {
    return current_ && bool(current_->check_);
}

void TaskWaitScope::check() {
    if (active()) {
        current_->check_();
    }
}

}  // namespace gribjump
