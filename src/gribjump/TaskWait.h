/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <functional>

namespace gribjump {

/// Optional caller-side check for synchronous Task waits. Throwing cancels pending
/// Tasks; active Tasks are drained before the exception is rethrown.
/// Used on the pybind API.
class TaskWaitScope {
public:

    explicit TaskWaitScope(std::function<void()> check);
    ~TaskWaitScope();

    TaskWaitScope(const TaskWaitScope&)            = delete;
    TaskWaitScope& operator=(const TaskWaitScope&) = delete;

    static bool active();
    static void check();

private:

    std::function<void()> check_;
    TaskWaitScope* previous_;
    static thread_local TaskWaitScope* current_;
};

}  // namespace gribjump
