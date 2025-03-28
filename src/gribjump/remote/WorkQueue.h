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

#pragma once

#include <thread>

#include "eckit/container/Queue.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/Task.h"
#include "gribjump/remote/WorkItem.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class WorkQueue : private eckit::NonCopyable {
public:

    static WorkQueue& instance();  // singleton

    ~WorkQueue();

    void push(Task* task);

protected:

    WorkQueue();

private:

    eckit::Queue<WorkItem> queue_;
    std::vector<std::thread> workers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
