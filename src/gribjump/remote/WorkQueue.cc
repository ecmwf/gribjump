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

WorkQueue::WorkQueue() : queue_(eckit::Resource<size_t>("$GRIBJUMP_QUEUESIZE", 1024)) {
    int nthreads = eckit::Resource<size_t>("$GRIBJUMP_THREADS", 32);
    eckit::Log::info() << "Starting " << eckit::Plural(nthreads, "thread") << std::endl;
    for (int i = 0; i < nthreads; ++i) {
        workers_.emplace_back([this] {

            LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " starting" << std::endl;
            GribJump gj = GribJump(); // one per thread
            
            for (;;) {
                WorkItem item;
                queue_.pop(item);
                LOG_DEBUG_LIB(LibGribJump) << "Thread " << std::this_thread::get_id() << " new job" << std::endl;
                try {
                    item.run(gj);
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

void WorkQueue::push(WorkItem& item) {
    queue_.push(item);
}

}  // namespace gribjump
