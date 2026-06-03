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

#include "gribjump/remote/WorkQueue.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"

#include "gribjump/Config.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Task.h"

namespace gribjump {

WorkQueue& WorkQueue::instance() {
    static WorkQueue wq;
    return wq;
}

WorkQueue::~WorkQueue() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        closed_ = true;
    }
    cv_.notify_all();

    for (auto& w : workers_) {
        w.join();
    }
}

WorkQueue::WorkQueue() {
    int nthreads = ConfigOptions::instance().numThreads();
    eckit::Log::info() << "Starting " << eckit::Plural(nthreads, "thread") << " (round-robin work queue)" << std::endl;
    for (int i = 0; i < nthreads; ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }
}

void WorkQueue::workerLoop() {
    LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " starting" << std::endl;

    for (;;) {
        eckit::Log::status() << "Waiting for job" << std::endl;
        WorkItem item;
        if (!popNext(item)) {
            LOG_DEBUG_LIB(LibGribJump)
                << "Thread " << std::this_thread::get_id() << " stopping (queue closed)" << std::endl;
            break;
        }

        LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " new job" << std::endl;
        try {
            item.run();
        }
        catch (const std::exception& e) {
            LOG_DEBUG_LIB(LibGribJump)
                << "Thread " << std::this_thread::get_id() << " exception: " << e.what() << std::endl;
            item.error(e.what());
        }
        catch (...) {
            LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " unknown exception" << std::endl;
            item.error("Unknown exception");
        }
    }
}

bool WorkQueue::popNext(WorkItem& item) {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [&] { return closed_ || !rrOrder_.empty(); });

    if (rrOrder_.empty()) {
        // closed_ must be true here
        return false;
    }

    // Round-robin: serve the group at the front, then rotate it to the back
    // (if it still has tasks) or remove it (if drained).
    TaskGroup* group = rrOrder_.front();
    rrOrder_.pop_front();

    auto it = groupQueues_.find(group);
    ASSERT(it != groupQueues_.end());
    ASSERT(!it->second.empty());

    Task* task = it->second.front();
    it->second.pop_front();

    if (it->second.empty()) {
        groupQueues_.erase(it);
    }
    else {
        rrOrder_.push_back(group);
    }

    item = WorkItem(task);
    return true;
}

void WorkQueue::push(TaskGroup* group, Task* task) {
    ASSERT(group != nullptr);
    ASSERT(task != nullptr);

    {
        std::lock_guard<std::mutex> lock(mtx_);
        ASSERT(!closed_);

        auto [it, inserted] = groupQueues_.try_emplace(group);
        if (inserted) {
            rrOrder_.push_back(group);
        }
        it->second.push_back(task);
    }

    cv_.notify_one();
}

}  // namespace gribjump
