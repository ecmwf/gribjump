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
/// Each TaskGroup gets its own internal FIFO queue. Worker threads pop tasks
/// by visiting the groups in round-robin order, so a single very large
/// TaskGroup cannot block tasks belonging to other groups.
///
/// Notes on fairness:
/// - Round-robin is per-task: a worker takes one task from group A, the next
///   worker takes one from group B, etc.
/// - A new TaskGroup is always admitted (its first task bypasses the global
///   backpressure cap), so a saturating producer cannot prevent newcomers
///   from entering the rotation.
/// - Subsequent pushes by an already-admitted group respect the global cap.
class WorkQueue {
public:

    static WorkQueue& instance();  // singleton

    WorkQueue(const WorkQueue&)            = delete;
    WorkQueue& operator=(const WorkQueue&) = delete;
    WorkQueue(WorkQueue&&)                 = delete;
    WorkQueue& operator=(WorkQueue&&)      = delete;

    ~WorkQueue();

    /// Enqueue a task belonging to the given task group.
    /// May block if the queue is at capacity and the group is already admitted.
    void push(TaskGroup* group, Task* task);

protected:

    WorkQueue();

private:

    /// Worker loop: pops tasks in round-robin order across groups and runs them.
    void workerLoop();

    /// Pop one task from the next group in round-robin order.
    /// Returns false if the queue has been closed and is empty.
    bool popNext(WorkItem& item);

private:

    mutable std::mutex mtx_;
    std::condition_variable cvPop_;   //< signalled when tasks become available or the queue is closed
    std::condition_variable cvPush_;  //< signalled when slots free up or the queue is closed

    bool closed_       = false;
    size_t totalTasks_ = 0;
    size_t maxSize_;

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
