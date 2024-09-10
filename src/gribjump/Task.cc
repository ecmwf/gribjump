/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/Length.h"

#include "fdb5/api/FDB.h"

#include "gribjump/Task.h"
#include "gribjump/info/InfoCache.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/jumper/JumperFactory.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/info/InfoFactory.h"

namespace gribjump {

static std::string thread_id_str() {
    auto id = std::this_thread::get_id();
    std::stringstream ss;
    ss << id;
    return ss.str();
}

//----------------------------------------------------------------------------------------------------------------------

Task::Task(TaskGroup& taskGroup, size_t taskid) : taskGroup_(taskGroup), taskid_(taskid) {
}

Task::~Task() {}

void Task::notify() {
    taskGroup_.notify(id());
}

void Task::notifyError(const std::string& s) {
    taskGroup_.notifyError(id(), s);
}

//----------------------------------------------------------------------------------------------------------------------

TaskGroup::TaskGroup() {}

TaskGroup::~TaskGroup() {
    for (auto& t : tasks_) {
        delete t;
    }
}

void TaskGroup::notify(size_t taskid) {
    std::lock_guard<std::mutex> lock(m_);
    taskStatus_[taskid] = Task::Status::DONE;
    counter_++;

    // Logging progress
    if (waiting_) {
        if (counter_ == logcounter_) {
            eckit::Log::info() << "Gribjump Progress: " << counter_ << " of " << taskStatus_.size() << " tasks complete" << std::endl;
            logcounter_ += logincrement_;
        }
    }

    cv_.notify_one();
}


void TaskGroup::notifyError(size_t taskid, const std::string& s) {
    std::lock_guard<std::mutex> lock(m_);
    taskStatus_[taskid] = Task::Status::FAILED;
    errors_.push_back(s);
    counter_++;
    cv_.notify_one();
}

void TaskGroup::enqueueTask(Task* task) {
    taskStatus_.push_back(Task::Status::PENDING);
    WorkItem w(task);
    WorkQueue& queue = WorkQueue::instance();
    queue.push(w);
    LOG_DEBUG_LIB(LibGribJump) << "Queued task " <<  tasks_.size() << std::endl;
}

void TaskGroup::waitForTasks() {
    ASSERT(taskStatus_.size() > 0); // todo Might want to allow for "no tasks" case, though be careful with the lock / counter.
    LOG_DEBUG_LIB(LibGribJump) << "Waiting for " << eckit::Plural(taskStatus_.size(), "task") << "..." << std::endl;
    std::unique_lock<std::mutex> lock(m_);

    waiting_ = true;
    logincrement_ = taskStatus_.size() / 10;
    if (logincrement_ == 0) {
        logincrement_ = 1;
    }

    cv_.wait(lock, [&]{return counter_ == taskStatus_.size();});
    waiting_ = false;
    LOG_DEBUG_LIB(LibGribJump) << "All tasks complete" << std::endl;
}

void TaskGroup::reportErrors(eckit::Stream& client_) {
    client_ << errors_.size();
    for (const auto& s : errors_) {
        client_ << s;
    }
}

//----------------------------------------------------------------------------------------------------------------------

FileExtractionTask::FileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, ExtractionItems& extractionItems) :
    Task(taskgroup, id),
    fname_(fname),
    extractionItems_(extractionItems) {
}

void FileExtractionTask::execute()  {
    const std::string thread_id = thread_id_str();
    eckit::Timer full_timer("Thread total time. Thread: " + thread_id, eckit::Log::debug());

    // Sort extractionItems_ by offset
    std::sort(extractionItems_.begin(), extractionItems_.end(), [](const ExtractionItem* a, const ExtractionItem* b) {
        return a->offset() < b->offset();
    });

    extract();

    notify();
}

void FileExtractionTask::extract() {

    // Get infos
    std::vector<eckit::Offset> offsets;

    for (auto& extractionItem : extractionItems_) {
        offsets.push_back(extractionItem->offset());
    }

    std::vector<std::shared_ptr<JumpInfo>> infos = InfoCache::instance().get(fname_, offsets);

    // Extract
    eckit::FileHandle fh(fname_);

    fh.openForRead();

    for (size_t i = 0; i < extractionItems_.size(); i++) {
        ExtractionItem* extractionItem = extractionItems_[i];
        const JumpInfo& info = *infos[i];

        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info)); // todo, dont build a new jumper for each info.
        jumper->extract(fh, offsets[i], info, *extractionItem);
    }

    fh.close();
}

//----------------------------------------------------------------------------------------------------------------------


//----------------------------------------------------------------------------------------------------------------------
InefficientFileExtractionTask::InefficientFileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, ExtractionItems& extractionItems):
    FileExtractionTask(taskgroup, id, fname, extractionItems) {
}

void InefficientFileExtractionTask::extract() {

    fdb5::FDB fdb;

    // One message at a time
    for (auto& extractionItem : extractionItems_) {
        eckit::URI uri = extractionItem->URI();

        if (uri.scheme() != "fdb") {
            throw eckit::SeriousBug("InefficientFileExtractionTask::extract() called with non-fdb URI");
        }

        std::string s = uri.query("length");
        ASSERT(!s.empty());
        eckit::Length length(std::stoll(s));

        eckit::Buffer buffer(length);
        eckit::MemoryHandle memHandle(buffer);

        std::unique_ptr<eckit::DataHandle> remoteHandle(fdb.read(uri));

        {
            // eckit::AutoLock lock(taskGroup_.debugMutex_); // Force single-threaded execution
            long read = 0;
            long toRead = length;
            while ((read = remoteHandle->read(buffer, toRead)) != 0) {
                toRead -= read;
            }
        }
            
        // Straight to factory, don't even check the cache
        memHandle.openForRead();
        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(memHandle, 0));
        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(*info));
        jumper->extract(memHandle, 0, *info, *extractionItem);

    }
}

//----------------------------------------------------------------------------------------------------------------------


FileScanTask::FileScanTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, const std::vector<eckit::Offset>& offsets) :
    Task(taskgroup, id),
    fname_(fname),
    offsets_(offsets) {
}

void FileScanTask::execute() {
    eckit::Timer timer;
    eckit::Timer full_timer("Thread total time. Thread: " + thread_id_str());

    std::sort(offsets_.begin(), offsets_.end());

    scan();

    notify();
}

void FileScanTask::scan() {

    if (offsets_.size() == 0) {
        InfoCache::instance().scan(fname_);
        return;
    }

    InfoCache::instance().scan(fname_, offsets_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump