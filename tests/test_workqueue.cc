/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Unit tests for the round-robin WorkQueue scheduler.
///
/// These tests verify that the WorkQueue dispatches tasks fairly across
/// TaskGroups in round-robin order, and in particular that a large
/// TaskGroup cannot block a smaller one from making progress.
///
/// The tests rely on running with a single worker thread so that the
/// dispatch order is deterministic; this is enforced via the
/// GRIBJUMP_THREADS=1 environment variable set in the test's CMakeLists.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include "gribjump/Task.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// Test helpers

/// Shared log of (group_label, sequence) pairs in order of dispatch.
struct DispatchLog {
    std::mutex m;
    std::vector<std::pair<std::string, size_t>> entries;

    void record(const std::string& label, size_t seq) {
        std::lock_guard<std::mutex> lock(m);
        entries.emplace_back(label, seq);
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(m);
        return entries.size();
    }
};

/// Minimal Task that records its identity when executed. Used to observe
/// the order in which the scheduler dispatches tasks.
class DummyTask : public Task {
public:

    DummyTask(TaskGroup& g, size_t id, std::string label, size_t seq, DispatchLog& log) :
        Task(g, id), label_(std::move(label)), seq_(seq), log_(log) {}

    void executeImpl() override { log_.record(label_, seq_); }

    void info() const override {}

private:

    std::string label_;
    size_t seq_;
    DispatchLog& log_;
};

/// Task that signals when it starts executing and then blocks until released.
/// Used to hold the (single) worker thread while we enqueue test tasks, so
/// that the scheduler's dispatch order is observable rather than racy with
/// the producer.
class BlockerTask : public Task {
public:

    BlockerTask(TaskGroup& g, size_t id, std::mutex& m, std::condition_variable& cv, bool& started, bool& release) :
        Task(g, id), m_(m), cv_(cv), started_(started), release_(release) {}

    void executeImpl() override {
        {
            std::lock_guard<std::mutex> lock(m_);
            started_ = true;
        }
        cv_.notify_all();

        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [&] { return release_; });
    }

    void info() const override {}

private:

    std::mutex& m_;
    std::condition_variable& cv_;
    bool& started_;
    bool& release_;
};

/// RAII helper: enqueues a BlockerTask on its own TaskGroup, waits for it to
/// start executing (occupying the worker), and releases it on destruction
/// (or via release()). While the blocker is held, the caller can freely
/// enqueue tasks into other TaskGroups without any of them being dispatched.
class WorkerGate {
public:

    WorkerGate() {
        group_.enqueueTask<BlockerTask>(std::ref(m_), std::ref(cv_), std::ref(started_), std::ref(release_));
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [&] { return started_; });
    }

    ~WorkerGate() {
        release();
        group_.waitForTasks();
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(m_);
            if (release_)
                return;
            release_ = true;
        }
        cv_.notify_all();
    }

private:

    TaskGroup group_;
    std::mutex m_;
    std::condition_variable cv_;
    bool started_ = false;
    bool release_ = false;
};

/// Task that throws when executed, to drive the error path.
class ThrowingTask : public Task {
public:

    ThrowingTask(TaskGroup& g, size_t id) : Task(g, id) {}

    void executeImpl() override { throw eckit::SeriousBug("boom"); }

    void info() const override {}
};

//-----------------------------------------------------------------------------
// Tests

CASE("round_robin_large_does_not_block_small") {
    // Push a large group (A, 6 tasks) followed entirely by a small group
    // (B, 3 tasks). Under the old FIFO queue, B would have to wait for the
    // whole of A. With round-robin, the worker should interleave them.

    DispatchLog log;
    TaskGroup groupA;
    TaskGroup groupB;

    {
        WorkerGate gate;  // worker is now stuck inside the blocker

        for (size_t i = 0; i < 6; ++i) {
            groupA.enqueueTask<DummyTask>(std::string("A"), i, std::ref(log));
        }
        for (size_t i = 0; i < 3; ++i) {
            groupB.enqueueTask<DummyTask>(std::string("B"), i, std::ref(log));
        }
        // gate releases here; the worker then drains the queue in RR order
    }

    groupA.waitForTasks();
    groupB.waitForTasks();

    const std::vector<std::pair<std::string, size_t>> expected = {
        {"A", 0}, {"B", 0}, {"A", 1}, {"B", 1}, {"A", 2}, {"B", 2}, {"A", 3}, {"A", 4}, {"A", 5},
    };

    EXPECT_EQUAL(log.entries.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQUAL(log.entries[i].first, expected[i].first);
        EXPECT_EQUAL(log.entries[i].second, expected[i].second);
    }

    // The small group must finish before the large group.
    auto lastIndexOf = [&](const std::string& label) {
        size_t idx = 0;
        for (size_t i = 0; i < log.entries.size(); ++i) {
            if (log.entries[i].first == label)
                idx = i;
        }
        return idx;
    };
    EXPECT(lastIndexOf("B") < lastIndexOf("A"));
}

CASE("round_robin_three_equal_groups") {
    // Three groups of equal size pushed back-to-back. With round-robin we
    // expect a perfect A,B,C,A,B,C,... interleaving regardless of push order.

    DispatchLog log;
    TaskGroup groupA;
    TaskGroup groupB;
    TaskGroup groupC;

    const size_t N = 4;

    {
        WorkerGate gate;

        for (size_t i = 0; i < N; ++i)
            groupA.enqueueTask<DummyTask>(std::string("A"), i, std::ref(log));
        for (size_t i = 0; i < N; ++i)
            groupB.enqueueTask<DummyTask>(std::string("B"), i, std::ref(log));
        for (size_t i = 0; i < N; ++i)
            groupC.enqueueTask<DummyTask>(std::string("C"), i, std::ref(log));
    }

    groupA.waitForTasks();
    groupB.waitForTasks();
    groupC.waitForTasks();

    EXPECT_EQUAL(log.entries.size(), 3 * N);

    std::vector<std::pair<std::string, size_t>> expected;
    expected.reserve(3 * N);
    for (size_t i = 0; i < N; ++i) {
        expected.emplace_back("A", i);
        expected.emplace_back("B", i);
        expected.emplace_back("C", i);
    }

    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQUAL(log.entries[i].first, expected[i].first);
        EXPECT_EQUAL(log.entries[i].second, expected[i].second);
    }
}

CASE("round_robin_unequal_groups_drains_in_order") {
    // Groups of sizes 1, 2, 5 pushed in that order. Expected dispatch:
    //   A0, B0, C0, B1, C1, C2, C3, C4
    // i.e. each group rotates until it is exhausted, then drops out.

    DispatchLog log;
    TaskGroup groupA;
    TaskGroup groupB;
    TaskGroup groupC;

    {
        WorkerGate gate;

        groupA.enqueueTask<DummyTask>(std::string("A"), 0, std::ref(log));
        for (size_t i = 0; i < 2; ++i)
            groupB.enqueueTask<DummyTask>(std::string("B"), i, std::ref(log));
        for (size_t i = 0; i < 5; ++i)
            groupC.enqueueTask<DummyTask>(std::string("C"), i, std::ref(log));
    }

    groupA.waitForTasks();
    groupB.waitForTasks();
    groupC.waitForTasks();

    const std::vector<std::pair<std::string, size_t>> expected = {
        {"A", 0}, {"B", 0}, {"C", 0}, {"B", 1}, {"C", 1}, {"C", 2}, {"C", 3}, {"C", 4},
    };

    EXPECT_EQUAL(log.entries.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQUAL(log.entries[i].first, expected[i].first);
        EXPECT_EQUAL(log.entries[i].second, expected[i].second);
    }
}

CASE("group_drains_and_is_readmitted") {
    // Verify that a group whose queue is fully drained by the worker can
    // re-enter the rotation when the producer pushes more tasks afterwards.
    // The re-admitted group should join at the back of the rotation.

    DispatchLog log;
    TaskGroup groupA;
    TaskGroup groupB;

    // Phase 1: enqueue and fully drain A on its own.
    {
        WorkerGate gate;
        groupA.enqueueTask<DummyTask>(std::string("A"), 0, std::ref(log));
        groupA.enqueueTask<DummyTask>(std::string("A"), 1, std::ref(log));
    }
    // Wait for A's two tasks to actually finish executing, so A is removed
    // from the WorkQueue's rotation before phase 2.
    while (log.size() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Phase 2: with A drained, push more A tasks together with B tasks.
    // B is the first to push, so B joins the rotation first; A's re-admission
    // goes behind B. Expected: B0, A2, B1, A3.
    {
        WorkerGate gate;
        groupB.enqueueTask<DummyTask>(std::string("B"), 0, std::ref(log));
        groupA.enqueueTask<DummyTask>(std::string("A"), 2, std::ref(log));
        groupB.enqueueTask<DummyTask>(std::string("B"), 1, std::ref(log));
        groupA.enqueueTask<DummyTask>(std::string("A"), 3, std::ref(log));
    }

    groupA.waitForTasks();
    groupB.waitForTasks();

    const std::vector<std::pair<std::string, size_t>> expected = {
        {"A", 0}, {"A", 1},                      // phase 1
        {"B", 0}, {"A", 2}, {"B", 1}, {"A", 3},  // phase 2: A is re-admitted behind B
    };

    EXPECT_EQUAL(log.entries.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQUAL(log.entries[i].first, expected[i].first);
        EXPECT_EQUAL(log.entries[i].second, expected[i].second);
    }
}

//-----------------------------------------------------------------------------
// Streaming harvest queue + byte-budget backpressure

CASE("harvest_popCompleted_drains_all_completed_tasks") {
    // popCompleted() lets a consumer drain results incrementally as each task
    // completes, instead of the all-or-nothing waitForTasks() barrier.
    DispatchLog log;
    TaskGroup group;

    const size_t N = 5;
    for (size_t i = 0; i < N; ++i) {
        group.enqueueTask<DummyTask>(std::string("H"), i, std::ref(log));
    }

    std::vector<size_t> harvested;
    while (auto id = group.popCompleted()) {
        harvested.push_back(*id);
    }

    EXPECT_EQUAL(harvested.size(), N);
    std::sort(harvested.begin(), harvested.end());
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQUAL(harvested[i], i);
    }
}

CASE("harvest_single_error_yields_no_completed_and_reports_error") {
    // A failed task is not harvestable (produced no result); popCompleted()
    // still terminates and the error is surfaced for the trailer.
    TaskGroup group;
    group.enqueueTask<ThrowingTask>();

    std::vector<size_t> harvested;
    while (auto id = group.popCompleted()) {
        harvested.push_back(*id);
    }

    EXPECT_EQUAL(harvested.size(), 0u);
    EXPECT_EQUAL(group.nErrors(), 1u);
}

CASE("harvest_error_cancels_siblings_and_still_terminates") {
    // Regression: A task erroring with cancelOnFirstError cancels its still-PENDING
    //  siblings. Those cancelled tasks must still be counted towards completion
    //  (via Task::execute -> notifyCancelled), otherwise popCompleted() never
    //  reaches its terminal condition and hangs. The WorkerGate holds the single
    //  worker until every task is enqueued, so 0th task (throwing) runs first and
    //  finds all siblings still PENDING to cancel.
    TaskGroup group;
    DispatchLog log;
    const size_t nSiblings = 4;

    {
        WorkerGate gate;
        group.enqueueTask<ThrowingTask>();  // id 0: runs first, triggers cancellation
        for (size_t i = 0; i < nSiblings; ++i) {
            group.enqueueTask<DummyTask>(std::string("S"), i, std::ref(log));
        }
        // gate releases here; the worker then drains `group` in FIFO order
    }

    std::vector<size_t> harvested;
    while (auto id = group.popCompleted()) {  // must not hang
        harvested.push_back(*id);
    }

    // Nothing harvestable: the error produced no result and the siblings were
    // cancelled before they could run.
    EXPECT_EQUAL(harvested.size(), 0u);
    EXPECT_EQUAL(log.size(), 0u);  // no sibling executed
    EXPECT_EQUAL(group.nErrors(), 1u);
    EXPECT_EQUAL(group.nCancelled(), nSiblings);
}

CASE("waitForTasks_terminates_when_error_cancels_siblings") {
    // Same regression test on the buffered barrier: waitForTasks() shares the
    // nComplete_ == tasks_.size() terminal condition, so it too would hang if
    // cancelled siblings were not counted.
    TaskGroup group;
    DispatchLog log;
    const size_t nSiblings = 3;

    {
        WorkerGate gate;
        group.enqueueTask<ThrowingTask>();
        for (size_t i = 0; i < nSiblings; ++i) {
            group.enqueueTask<DummyTask>(std::string("S"), i, std::ref(log));
        }
    }

    group.waitForTasks();  // must not hang

    EXPECT_EQUAL(log.size(), 0u);
    EXPECT_EQUAL(group.nErrors(), 1u);
    EXPECT_EQUAL(group.nCancelled(), nSiblings);
}

CASE("cancel_purges_queued_tasks_from_the_queue") {
    // A group cancelled while its tasks are still queued must have those tasks
    // *purged* from the WorkQueue, not merely flagged and left for a worker to
    // pop lazily one round-robin turn at a time.
    //
    // We prove the purge by cancelling while the single worker is held inside
    // the gate: the worker cannot have popped any of the group's tasks, yet
    // they are already accounted as cancelled the instant cancel() returns.
    // That can only happen if cancel() removed them from the queue and
    // accounted them synchronously in this thread -- lazy cancellation would
    // leave nCancelled() at zero until a freed worker eventually popped each one.
    DispatchLog log;
    TaskGroup group;
    const size_t N = 5;

    {
        WorkerGate gate;  // the single worker is blocked inside the gate

        for (size_t i = 0; i < N; ++i) {
            group.enqueueTask<DummyTask>(std::string("G"), i, std::ref(log));
        }

        group.cancel();

        // Worker still blocked: it has popped none of G's tasks. The purge has
        // already removed them from the queue and accounted them as cancelled.
        EXPECT_EQUAL(group.nCancelled(), N);
        EXPECT_EQUAL(group.nErrors(), 0u);

        // gate releases here; the freed worker drains only its own group
    }

    // None of the purged tasks ever executed, and the group is fully accounted.
    EXPECT_EQUAL(log.size(), 0u);
    EXPECT_EQUAL(group.nCancelled(), N);
    group.waitForTasks();  // must not hang: cancelled tasks count as complete
}


CASE("byte_budget_accounting_and_overBudget") {
    TaskGroup group;
    EXPECT(!group.overBudget());  // unlimited by default

    group.setByteThreshold(100);
    group.addOutstanding(80);
    EXPECT(!group.overBudget());  // 80 <= 100
    group.addOutstanding(80);
    EXPECT(group.overBudget());  // 160 > 100
    group.releaseOutstanding(80);
    EXPECT(!group.overBudget());  // back to 80
}

CASE("over_budget_group_is_skipped_until_drained") {
    // The WorkQueue must not dispatch a group's tasks while it is over budget,
    // so a slow consumer cannot force unbounded server-side memory. Other groups
    // keep flowing; the throttled group resumes once its budget is released.
    DispatchLog log;
    TaskGroup groupA;
    TaskGroup groupB;

    groupA.setByteThreshold(10);
    groupA.addOutstanding(1000);  // A starts over budget
    EXPECT(groupA.overBudget());

    {
        WorkerGate gate;  // hold the single worker while we enqueue

        for (size_t i = 0; i < 3; ++i)
            groupA.enqueueTask<DummyTask>(std::string("A"), i, std::ref(log));
        for (size_t i = 0; i < 3; ++i)
            groupB.enqueueTask<DummyTask>(std::string("B"), i, std::ref(log));
        // gate releases here; worker drains what it is allowed to
    }

    // Only B is servable, so B runs to completion while A is skipped.
    while (log.size() < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        std::lock_guard<std::mutex> lock(log.m);
        EXPECT_EQUAL(log.entries.size(), 3u);
        for (const auto& e : log.entries) {
            EXPECT_EQUAL(e.first, std::string("B"));
        }
    }

    // Release A's budget: its tasks become servable and the worker resumes.
    groupA.releaseOutstanding(1000);
    EXPECT(!groupA.overBudget());

    groupA.waitForTasks();
    groupB.waitForTasks();

    EXPECT_EQUAL(log.size(), 6u);

    size_t firstA = log.entries.size();
    size_t lastB  = 0;
    for (size_t i = 0; i < log.entries.size(); ++i) {
        if (log.entries[i].first == "A" && firstA == log.entries.size())
            firstA = i;
        if (log.entries[i].first == "B")
            lastB = i;
    }
    EXPECT(lastB < firstA);  // every B dispatched before any A
}

}  // namespace test
}  // namespace gribjump


int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
