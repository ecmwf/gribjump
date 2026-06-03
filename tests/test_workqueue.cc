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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

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

    // Headline property: the small group must finish before the large group.
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

}  // namespace test
}  // namespace gribjump


int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
