/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "gribjump/ExtractionData.h"
#include "gribjump/remote/WorkItem.h"

namespace gribjump {

class Task;
class TaskGroup;

//----------------------------------------------------------------------------------------------------------------------

/// Round-robin, multi-queue work scheduler shared by all worker threads.
///
/// Each TaskGroup has its own internal FIFO queue. Worker threads pop tasks
/// by visiting the groups in round-robin order, such that a single very large TaskGroup does not block tasks in other
/// groups.
class WorkQueue {
public:

    static WorkQueue& instance();  // singleton

    WorkQueue(const WorkQueue&)            = delete;
    WorkQueue& operator=(const WorkQueue&) = delete;
    WorkQueue(WorkQueue&&)                 = delete;
    WorkQueue& operator=(WorkQueue&&)      = delete;

    ~WorkQueue();

    /// Enqueue a task belonging to the given task group.
    void push(TaskGroup* group, Task* task);

    /// Remove all of a group's still-queued tasks from the scheduler.
    //  Used when a request is abandoned (e.g. the client disconnected).
    void cancelGroup(TaskGroup* group);

    /// Wake worker threads to re-evaluate which groups are servable.
    /// Called when a group drops back under its byte budget so its previously-skipped tasks can be dispatched again.
    void reconsider();

protected:

    WorkQueue();

private:

    void workerLoop();

    /// Pop one task from the next group in round-robin order. Blocks until a
    /// task is available. Returns false once the queue has been closed.
    bool popNext(WorkItem& item);

private:

    mutable std::mutex mtx_;      //< rule of thumb: do not hold at the same time as TaskGroup::m_
    std::condition_variable cv_;  //< signalled when tasks become available or the queue is closed
    bool closed_ = false;

    /// Per-group FIFO of pending tasks. A group is only present here while it
    /// has at least one queued task; it is erased once drained and re-added on
    /// the next push.
    std::unordered_map<TaskGroup*, std::deque<Task*>> groupQueues_;

    /// Round-robin order of groups with pending tasks. Each TaskGroup appears
    /// at most once. The front is the next group to be served.
    std::list<TaskGroup*> rrOrder_;

    std::vector<std::thread> workers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
