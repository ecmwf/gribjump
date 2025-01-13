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
#include "eckit/io/AutoCloser.h"
#include "eckit/config/Resource.h"

#include "fdb5/api/FDB.h"

#include "gribjump/Task.h"
#include "gribjump/info/InfoCache.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/jumper/JumperFactory.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/remote/RemoteGribJump.h"

namespace gribjump {

constexpr bool cancelOnFirstError = true; ///@todo make this configurable

//----------------------------------------------------------------------------------------------------------------------

Task::Task(TaskGroup& taskGroup, size_t taskid) : taskGroup_(taskGroup), taskid_(taskid) {
}

Task::~Task() {}

void Task::notify() {
    status_ = Status::DONE;
    taskGroup_.notify(id());
}

void Task::notifyError(const std::string& s) {
    status_ = Status::FAILED;
    taskGroup_.notifyError(id(), s);
}

void Task::notifyCancelled() {
    status_ = Status::CANCELLED;
    taskGroup_.notifyCancelled(id());
}

void Task::execute() {
    // atomically set status to executing, but only if it is currently pending (i.e. not cancelled)
    Status expected = Status::PENDING;
    if (!status_.compare_exchange_strong(expected, Status::EXECUTING)) {
        return;
    }
    info();
    executeImpl();
    notify();
}

void Task::cancel() {
    // atomically set status to cancelled, but only if it is pending
    Status expected = Status::PENDING;
    status_.compare_exchange_strong(expected, Status::CANCELLED);
}

//----------------------------------------------------------------------------------------------------------------------

void TaskGroup::notify(size_t taskid) {
    std::lock_guard<std::mutex> lock(m_);
    nComplete_++;

    // Logging progress
    if (waiting_) {
        if (nComplete_ == logcounter_) {
            eckit::Log::info() << "Gribjump Progress: " << nComplete_ << " of " << tasks_.size() << " tasks complete" << std::endl;
            logcounter_ += logincrement_;
        }
    }

    cv_.notify_one();
    info();
}

void TaskGroup::notifyCancelled(size_t taskid) {
    std::lock_guard<std::mutex> lock(m_);
    nComplete_++;
    nCancelledTasks_++;
    cv_.notify_one();
    info();
}

void TaskGroup::notifyError(size_t taskid, const std::string& s) {
    std::lock_guard<std::mutex> lock(m_);
    errors_.push_back(s);
    nComplete_++;
    cv_.notify_one();
    info();

    if (cancelOnFirstError) {
        cancelTasks();
    }
}

void TaskGroup::info() const {
    eckit::Log::status() << nComplete_ << " of " << tasks_.size() << " tasks complete" << std::endl;
}

// Note: This will only affect tasks that have not yet started. Cancelled tasks will call notifyCancelled() when they are executed.
// NB: We do not lock a mutex as this will be called from notifyError()
void TaskGroup::cancelTasks() {
    for (auto& task : tasks_) {
        task->cancel();
    }
}

void TaskGroup::enqueueTask(Task* task) {
    {
        std::lock_guard<std::mutex> lock(m_);
        tasks_.push_back(std::unique_ptr<Task>(task)); // TaskGroup takes ownership of its tasks
    }
   
    WorkQueue::instance().push(task); /// @note Can block, so release the lock first

    LOG_DEBUG_LIB(LibGribJump) << "Queued task " <<  tasks_.size() << std::endl;
}

void TaskGroup::waitForTasks() {
    std::unique_lock<std::mutex> lock(m_);
    ASSERT(tasks_.size() > 0);
    LOG_DEBUG_LIB(LibGribJump) << "Waiting for " << eckit::Plural(tasks_.size(), "task") << "..." << std::endl;

    waiting_ = true;
    logincrement_ = tasks_.size() / 10;
    if (logincrement_ == 0) {
        logincrement_ = 1;
    }

    cv_.wait(lock, [&]{return nComplete_ == tasks_.size();});
    waiting_ = false;
    done_ = true;
    LOG_DEBUG_LIB(LibGribJump) << "All tasks complete" << std::endl;

    MetricsManager::instance().set("count_tasks", tasks_.size());
    MetricsManager::instance().set("count_failed_tasks", errors_.size());
    MetricsManager::instance().set("count_cancelled_tasks", nCancelledTasks_);

    if (errors_.size() > 0) {
        MetricsManager::instance().set("first_error", errors_[0]);
    }
}

//----------------------------------------------------------------------------------------------------------------------

TaskReport::TaskReport() {}

TaskReport::TaskReport(std::vector<std::string>&& errors) : errors_(std::move(errors)) {}

void TaskReport::reportErrors(eckit::Stream& client) const {
    client << errors_.size();
    for (const auto& s : errors_) {
        client << s;
    }
}

void TaskReport::raiseErrors() const {
    if (errors_.size() > 0) {
        std::stringstream ss;
        ss << "Encountered " << eckit::Plural(errors_.size(), "error") << " during task execution:" << std::endl;
        for (const auto& s : errors_) {
            ss << s << std::endl;
        }
        throw eckit::SeriousBug(ss.str());
    }
}
//----------------------------------------------------------------------------------------------------------------------

FileExtractionTask::FileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, ExtractionItems& extractionItems) :
    Task(taskgroup, id),
    fname_(fname),
    extractionItems_(extractionItems),
    ignoreGrid_(eckit::Resource<bool>("$GRIBJUMP_IGNORE_GRID", LibGribJump::instance().config().getBool("ignoreGridHash", false)))
    {
}

void FileExtractionTask::executeImpl()  {

    // Sort extractionItems_ by offset
    std::sort(extractionItems_.begin(), extractionItems_.end(), [](const ExtractionItem* a, const ExtractionItem* b) {
        return a->offset() < b->offset();
    });

    extract();
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
    eckit::AutoCloser<eckit::FileHandle> closer(fh);

    for (size_t i = 0; i < extractionItems_.size(); i++) {
        ExtractionItem* extractionItem = extractionItems_[i];
        const JumpInfo& info = *infos[i];

        const std::string& expectedHash = extractionItem->gridHash();

        if (!ignoreGrid_ && expectedHash.empty()) {
            throw eckit::BadValue("Grid hash was not specified in request but is required. (Extraction item " + std::to_string(i) + " in file " + fname_ + ")");
        }
        if (!ignoreGrid_ && (expectedHash != info.md5GridSection())) {
            throw eckit::BadValue("Grid hash mismatch for extraction item " + std::to_string(i) + " in file " + fname_ + ". Request specified: " + expectedHash + ", JumpInfo contains: " + info.md5GridSection());
        }

        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info)); // todo, dont build a new jumper for each info.
        jumper->extract(fh, offsets[i], info, *extractionItem);
    }
}

void FileExtractionTask::info() const {
    eckit::Log::status() << "Extract " << extractionItems_.size() << " items from " << fname_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

// Forward the work to a remote server, and wait for the results.
ForwardExtractionTask::ForwardExtractionTask(TaskGroup& taskgroup, const size_t id, eckit::net::Endpoint endpoint, filemap_t& filemap) :
    Task(taskgroup, id),
    endpoint_(endpoint),
    filemap_(filemap)
{}

void ForwardExtractionTask::executeImpl(){

    RemoteGribJump remoteGribJump(endpoint_);
    remoteGribJump.forwardExtract(filemap_);
}

void ForwardExtractionTask::info() const {
    eckit::Log::status() << "Forward extract to " << endpoint_ << "nfiles=" << filemap_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardScanTask::ForwardScanTask(TaskGroup& taskgroup, const size_t id, eckit::net::Endpoint endpoint, scanmap_t& scanmap, std::atomic<size_t>& nfields):
    Task(taskgroup, id),
    endpoint_(endpoint),
    scanmap_(scanmap),
    nfields_(nfields) {
}

void ForwardScanTask::executeImpl(){

    RemoteGribJump remoteGribJump(endpoint_);
    nfields_ += remoteGribJump.forwardScan(scanmap_);
}

void ForwardScanTask::info() const {
    eckit::Log::status() << "Forward scan to " << endpoint_ << std::endl;
}

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

void InefficientFileExtractionTask::info() const {
    eckit::Log::status() << "Inefficiently extract " << extractionItems_.size() << " items from " << fname_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------


FileScanTask::FileScanTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, const std::vector<eckit::Offset>& offsets, std::atomic<size_t>& nfields) :
    Task(taskgroup, id),
    fname_(fname),
    offsets_(offsets),
    nfields_(nfields){
}

void FileScanTask::executeImpl() {

    std::sort(offsets_.begin(), offsets_.end());
    scan();
}

void FileScanTask::scan() {

    if (offsets_.size() == 0) {
        nfields_ += InfoCache::instance().scan(fname_);
        return;
    }

    nfields_ += InfoCache::instance().scan(fname_, offsets_);
}

void FileScanTask::info() const {
    eckit::Log::status() << "Scan " << offsets_.size() << " offsets in " << fname_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump