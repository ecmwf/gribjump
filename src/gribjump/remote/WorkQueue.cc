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

#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/config/Resource.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/LibGribJump.h"


namespace gribjump {

WorkQueue& WorkQueue::instance() {
    static WorkQueue wq;
    return wq;
}

WorkQueue::~WorkQueue() {
    queue_.close();

    for (auto& w : workers_) {
        w.join();
    }

}

WorkQueue::WorkQueue() : queue_(eckit::Resource<size_t>("$GRIBJUMP_QUEUESIZE;gribjumpQueueSize", 1024)) {
    int nthreads = eckit::Resource<size_t>("$GRIBJUMP_THREADS;gribjumpThreads", LibGribJump::instance().config().getInt("threads", 1));
    eckit::Log::info() << "Starting " << eckit::Plural(nthreads, "thread") << std::endl;
    for (int i = 0; i < nthreads; ++i) {
        workers_.emplace_back([this] {

            LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " starting" << std::endl;
            // GribJump gj = GribJump(); // one per thread
            
            for (;;) {
                eckit::Log::status() << "Waiting for job" << std::endl;
                WorkItem item;
                if (queue_.pop(item) == -1) {
                    LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " stopping (queue closed)" << std::endl;
                    break;
                }
                LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " new job" << std::endl;
                try {
                    item.run();
                }
                catch (const std::exception& e) {
                    LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " exception: " << e.what() << std::endl;
                    item.error(e.what());
                }
                catch (...) {
                    LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " unknown exception" << std::endl;
                    item.error("Unknown exception");
                }
            }
        });
    }
}

void WorkQueue::push(Task* task) {
    WorkItem item(task);
    queue_.push(item);
}

}  // namespace gribjump
