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
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
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

    /// The extraction items this task produced results into, for the streaming
    /// (v4) harvest path to send and free. Returns nullptr for tasks that do not
    /// produce streamable extraction results (e.g. scan/forward tasks).
    virtual const ExtractionItems* streamableItems() const { return nullptr; }

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

    /// The collected error messages (empty if none). Used by the v4 streaming
    /// reply path to build the END-chunk error trailer.
    const std::vector<std::string>& errors() const { return errors_; }

private:

    std::vector<std::string> errors_;  //< stores error messages, empty if no errors
};

//----------------------------------------------------------------------------------------------------------------------
//
class TaskGroup {
public:

    TaskGroup() : ctx_{ContextManager::instance().context()} {}

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

    /// Streaming harvest (v4 reply path): block until the next successfully
    /// completed task is available and return its id, or return nullopt once
    /// every task has completed and the completed-queue is drained. Mutually
    /// exclusive with waitForTasks() -- a TaskGroup uses one or the other.
    std::optional<size_t> popCompleted();

    // -- Backpressure: bound produced-but-not-yet-sent result bytes ------------
    // The threshold defaults to unlimited, so these are inert (overBudget() is
    // always false, waitIfOverBudget() returns immediately, and the WorkQueue
    // serves this group exactly as before) unless a streaming consumer opts in
    // via setByteThreshold(). The buffered (local) path is therefore unaffected.

    void setByteThreshold(size_t bytes) { byteThreshold_ = bytes; }

    /// Account for result bytes produced but not yet sent to the client.
    void addOutstanding(size_t bytes) { outstandingBytes_.fetch_add(bytes); }

    /// Account for result bytes that have been sent and freed. Wakes any task
    /// paused in waitIfOverBudget() and lets the WorkQueue reconsider this group
    /// once it drops back to/under budget.
    void releaseOutstanding(size_t bytes);

    /// True while more result bytes are outstanding than the configured budget.
    /// Lock-free; used by the WorkQueue to skip an over-budget group.
    bool overBudget() const { return outstandingBytes_.load() > byteThreshold_; }

    /// True when a finite byte budget has been set (streaming path), so tasks
    /// know to account for produced bytes. Unlimited by default, so the buffered
    /// (local) path skips all byte accounting and is byte-identical.
    bool backpressureEnabled() const { return byteThreshold_ != std::numeric_limits<size_t>::max(); }

    /// The streamable extraction items of a completed task (by id), for the
    /// streaming harvest to send and free. id is the task index within the group.
    const ExtractionItems* streamableItems(size_t id) {
        std::lock_guard<std::mutex> lock(m_);
        return tasks_.at(id)->streamableItems();
    }

    /// Block while this group is over its byte budget. Used to pause a running
    /// task between items so a single large file cannot outrun the consumer.
    void waitIfOverBudget();

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

    const LogContext& context() const { return ctx_; }

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

    std::deque<size_t> completed_;  //< ids of DONE tasks awaiting harvest (guarded by m_)

    std::atomic<size_t> outstandingBytes_{0};                    //< produced-but-not-yet-sent result bytes
    size_t byteThreshold_ = std::numeric_limits<size_t>::max();  //< unlimited by default (no backpressure)
    std::condition_variable budgetCv_;                           //< signalled when outstandingBytes_ drops

    const LogContext& ctx_;  //< required for propagating context in forwarding tasks.
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

    const ExtractionItems* streamableItems() const override { return &extractionItems_; }

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