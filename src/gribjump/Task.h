/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "eckit/serialisation/Stream.h"

#include "gribjump/ExtractionItem.h"
#include "gribjump/GribJump.h"

namespace gribjump {

class TaskGroup;

//----------------------------------------------------------------------------------------------------------------------

/// Unit of work to be executed in a worker thread
/// Wrapped by WorkItem
class Task {
public:

    enum Status {
        DONE = 0,
        PENDING,
        FAILED,
        EXECUTING,
        CANCELLED,
    };

    Task(TaskGroup& taskGroup, size_t id);

    virtual ~Task();

    size_t id() const { return taskid_; }

    /// executes the task to completion
    virtual void execute() final;

    /// notifies the completion of the task
    void notify();

    /// notifies that the task was cancelled before execution e.g. because of an error in a related task
    void notifyCancelled();

    /// notifies the error in execution of the task
    void notifyError(const std::string& s);

    /// cancels the task. If execute() is called after this, it will return immediately.
    void cancel();

    /// Write description of task to eckit::Log::status() for monitoring
    virtual void info() const = 0;

protected:

    virtual void executeImpl() = 0;

protected:

    TaskGroup& taskGroup_;  //< Groups like-tasks to be executed in parallel
    size_t taskid_;         //< Task id within parent request
    std::atomic<Status> status_ = Status::PENDING;
};

//----------------------------------------------------------------------------------------------------------------------

// TaskReport contains error messages and other information produced by a TaskGroup, and methods to either
// report them to a client or raise an exception.
// TaskGroup will return a TaskReport object to calling code.
class TaskReport {

public:

    TaskReport();
    TaskReport(std::vector<std::string>&& errors);

    void reportErrors(eckit::Stream& client) const;
    void raiseErrors() const;

private:

    std::vector<std::string> errors_;  //< stores error messages, empty if no errors
};

//----------------------------------------------------------------------------------------------------------------------
//
class TaskGroup {
public:

    TaskGroup() = default;

    /// Notify that a task has been completed
    void notify(size_t taskid);

    /// Notify that a task has finished with error
    void notifyError(size_t taskid, const std::string& s);

    /// Notify that a task was cancelled
    void notifyCancelled(size_t taskid);

    /// Enqueue tasks on the global task queue
    template <typename TaskType, typename... Args>
    void enqueueTask(Args&&... args) {
        enqueueTask(new TaskType(*this, tasks_.size(), std::forward<Args>(args)...));
    }

    /// Wait for all queued tasks to be executed
    void waitForTasks();

    /// Report on errors and other status information about executed tasks.
    /// Calling code may use this to report to a client or raise an exception.
    TaskReport report() {
        std::lock_guard<std::mutex> lock(m_);
        ASSERT(done_);
        return TaskReport(std::move(errors_));
    }

    size_t nTasks() const {
        std::lock_guard<std::mutex> lock(m_);
        return tasks_.size();
    }

    size_t nErrors() const {
        std::lock_guard<std::mutex> lock(m_);
        return errors_.size();
    }

    void info() const;

private:

    void enqueueTask(Task* task);

    void cancelTasks();

private:

    int nComplete_       = 0;      //< incremented when a task completes
    int nCancelledTasks_ = 0;      //< incremented by notifyCancelled()
    int logcounter_      = 1;      //< used to log progress
    int logincrement_    = 1;      //< used to log progress
    bool waiting_        = false;  //< true if waiting for tasks to complete
    bool done_           = false;  //< true if all tasks have completed

    mutable std::mutex m_;
    std::condition_variable cv_;

    std::vector<std::shared_ptr<Task>> tasks_;
    std::vector<std::string> errors_;  //< stores error messages, empty if no errors
};

//----------------------------------------------------------------------------------------------------------------------

class FileExtractionTask : public Task {
public:

    // Each extraction item is assumed to be for the same file.

    FileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname,
                       ExtractionItems& extractionItems);

    void executeImpl() override;

    virtual void extract();

    virtual void info() const override;

protected:

    eckit::PathName fname_;
    ExtractionItems& extractionItems_;
    bool ignoreGrid_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

// InefficientFileExtractionTask extracts from the file, but by reading entire messages into memory first.
// Ideally, never need this, but currently required for remotefdb.
// Because it reads the full message, we do not check the cache for the infos, instead we create them on the fly.
class InefficientFileExtractionTask : public FileExtractionTask {
public:

    InefficientFileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname,
                                  ExtractionItems& extractionItems);

    void extract() override;

    virtual void info() const override;
};

//----------------------------------------------------------------------------------------------------------------------
// Task that forwards the work to a remote server, based on the URI of the extraction item.
class ForwardExtractionTask : public Task {
public:

    ForwardExtractionTask(TaskGroup& taskgroup, const size_t id, eckit::net::Endpoint endpoint, filemap_t& filemap);

    void executeImpl() override;

    virtual void info() const override;

private:

    eckit::net::Endpoint endpoint_;
    filemap_t& filemap_;
};

// Task that forwards the work to a remote server, based on the URI of the extraction item.
class ForwardScanTask : public Task {
public:

    ForwardScanTask(TaskGroup& taskgroup, const size_t id, eckit::net::Endpoint endpoint, scanmap_t& scanmap,
                    std::atomic<size_t>& nfields_);

    void executeImpl() override;

    virtual void info() const override;

private:

    eckit::net::Endpoint endpoint_;
    scanmap_t& scanmap_;
    std::atomic<size_t>& nfields_;
};

//----------------------------------------------------------------------------------------------------------------------

class FileScanTask : public Task {
public:

    // Each extraction item is assumed to be for the same file.

    FileScanTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname,
                 const std::vector<eckit::Offset>& offsets, std::atomic<size_t>& nfields);

    void executeImpl() override;

    void scan();

    virtual void info() const override;

private:

    eckit::PathName fname_;
    std::vector<eckit::Offset> offsets_;
    std::atomic<size_t>& nfields_;
};


}  // namespace gribjump