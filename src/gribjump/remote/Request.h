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

class Request;

/// Unit of work to be executed in a worker thread
/// Wrapped by WorkItem
class Task {
public:

    enum Status {
        DONE = 0,
        PENDING = 1,
        FAILED = 2
    };

    Task(size_t, Request* r);

    virtual ~Task();

    size_t id() const { return taskid_; }

    /// executes the task to completion
    virtual void execute(GribJump& gj) = 0;

    /// notifies the completion of the task
    virtual void notify();

    /// notifies the error in execution of the task
    virtual void notifyError(const std::string& s);

private:
    size_t taskid_; //< Task id within parent request
    Request* clientRequest_; //< Parent request
};

//----------------------------------------------------------------------------------------------------------------------

class Request {
public: // methods

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
    virtual void notify(size_t taskid);

    /// Notify that a task has finished with error
    /// potentially completing all the work for this request
    virtual void notifyError(size_t taskid, const std::string& s);

protected: // methods

    void reportErrors();

protected: // members

    int counter_ = 0;  //< incremented by notify() or notifyError()

    std::mutex m_;
    std::condition_variable cv_;

    std::vector<size_t> taskStatus_; //< stores tasks status, must be initialised by derived class
    
    std::vector<std::string> errors_; //< stores error messages, empty if no errors

    eckit::Stream& client_;
};

} // namespace gribjump
