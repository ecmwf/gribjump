
#pragma once

#include <mutex>
#include <condition_variable>
#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"
#include "gribjump/ExtractionItem.h"

namespace gribjump {
    
class TaskGroup;

//----------------------------------------------------------------------------------------------------------------------

/// Unit of work to be executed in a worker thread
/// Wrapped by WorkItem
class Task {
public:

    enum Status {
        DONE = 0,
        PENDING = 1,
        FAILED = 2
    };

    Task(TaskGroup& taskGroup, size_t id);

    virtual ~Task();

    size_t id() const { return taskid_; }

    /// executes the task to completion
    virtual void execute() = 0;

    /// notifies the completion of the task
    virtual void notify();

    /// notifies the error in execution of the task
    virtual void notifyError(const std::string& s);

protected:
    
    TaskGroup& taskGroup_; //< Groups like-tasks to be executed in parallel
    size_t taskid_; //< Task id within parent request
};

//----------------------------------------------------------------------------------------------------------------------

class TaskGroup {
public:

    TaskGroup();

    ~TaskGroup();

    /// Notify that a task has been completed
    /// potentially completing all the work for this request
    void notify(size_t taskid);

    /// Notify that a task has finished with error
    /// potentially completing all the work for this request
    void notifyError(size_t taskid, const std::string& s);

    /// Enqueue tasks to be executed to complete this request
    void enqueueTask(Task* task);
    
    /// Wait for all queued tasks to be executed
    void waitForTasks();

    void reportErrors(eckit::Stream& client_);

private:

    int counter_ = 0;  //< incremented by notify() or notifyError()

    std::mutex m_;
    std::condition_variable cv_;
    
    std::vector<Task*> tasks_; //< stores tasks status, must be initialised by derived class
    std::vector<size_t> taskStatus_;
    std::vector<std::string> errors_; //< stores error messages, empty if no errors
};

//----------------------------------------------------------------------------------------------------------------------

class FileExtractionTask : public Task {
public:

    // Each extraction item is assumed to be for the same file.

    FileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, std::vector<ExtractionItem*>& extractionItems);

    void execute() override;

    void extract();

private:
    eckit::PathName fname_;
    std::vector<ExtractionItem*>& extractionItems_;
};

//----------------------------------------------------------------------------------------------------------------------

class FileScanTask : public Task {
public:

    // Each extraction item is assumed to be for the same file.

    FileScanTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, const std::vector<eckit::Offset>& offsets);

    void execute() override;

    void scan();

private:
    eckit::PathName fname_;
    std::vector<eckit::Offset> offsets_;
};


} // namespace gribjump