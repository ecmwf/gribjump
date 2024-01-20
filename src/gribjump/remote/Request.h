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

#include <mutex>

#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

/// Unit of work to be executed in a worker thread
/// Wrapped by WorkItem
class Task {
public:

    Task();

    virtual ~Task();

    /// executes the task to completion
    virtual void execute(GribJump& gj) = 0;

    /// notifies the completion of the task
    virtual void notify() = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class Request {
public:

    Request(eckit::Stream& stream);

    virtual ~Request();

    /// Enqueue tasks to be executed to complete this request
    virtual void enqueueTasks() = 0;

    /// Wait for all queued tasks to be executed
    virtual void waitForTasks();

    /// Reply to the client with the results of the request
    virtual void replyToClient() = 0;

    /// Notify that a task has been completed
    /// potentially completing all the work for this request
    virtual void notify();

protected: 
    
    int ntasks_  = 0; // must be set by derived class
    int counter_ = 0; // should be incremented by notify()
    
    std::mutex m_;
    std::condition_variable cv_;
    
    eckit::Stream& client_;
};

} // namespace gribjump
