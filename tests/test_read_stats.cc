/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0.
 */
#include <atomic>
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <thread>
#include <vector>

#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"
#include "gribjump/ReadStats.h"
#include "gribjump/GribJumpDataAccessor.h"
#include "gribjump/Task.h"

using namespace eckit::testing;
using namespace gribjump;

CASE("read counters are task-local and merge once, including on exceptions") {
    std::atomic<size_t> bytes{0};
    TaskReadScope::requested(100);  // not executing a task: no attribution
    {
        TaskReadScope scope(bytes);
        TaskReadScope::requested(7);
        TaskReadScope::requested(11);
        TaskReadScope::requested(0);
        TaskReadScope::requested(-1);
        EXPECT(bytes.load() == 0);
    }
    EXPECT(bytes.load() == 18);
    try {
        TaskReadScope scope(bytes);
        TaskReadScope::requested(5);
        throw std::runtime_error("read failed");
    }
    catch (const std::runtime_error&) {}
    EXPECT(bytes.load() == 23);
}

CASE("concurrent task groups have independent exact read totals") {
    std::atomic<size_t> a{0}, b{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&, i] {
            for (int task = 0; task < 50; ++task) {
                TaskReadScope scope(i % 2 ? a : b);
                for (int read = 0; read < 100; ++read)
                    TaskReadScope::requested(i % 2 ? 3 : 7);
            }
        });
    }
    for (auto& thread : threads)
        thread.join();
    EXPECT(a.load() == 4 * 50 * 100 * 3);
    EXPECT(b.load() == 4 * 50 * 100 * 7);
}

CASE("source reads count requested lengths, rereads and short reads; not seeks") {
    eckit::TmpDir dir;
    const auto path = dir / "bytes";
    { std::ofstream f(path.asString()); f << "0123456789"; }
    std::atomic<size_t> bytes{0};
    {
        TaskReadScope scope(bytes);
        ReadCountingFileHandle file(path.asString());
        file.openForRead();
        char buf[20];
        EXPECT(file.read(buf, 4) == 4);
        file.seek(0);
        EXPECT(file.read(buf, 4) == 4);  // reread counts again
        EXPECT(file.read(buf, 20) == 6); // count requested 20, not returned 6
        file.close();
    }
    EXPECT(bytes.load() == 28);
    {
        TaskReadScope scope(bytes);
        ReadCountingFileStream file(path, "r");
        char buf[10];
        EXPECT(file.read(buf, 10) == 10);
        file.close();
    }
    EXPECT(bytes.load() == 38);
}

namespace {
class ReadTask : public Task {
public:
    ReadTask(TaskGroup& group, size_t id, bool fail = false) : Task(group, id), fail_(fail) {}
    void info() const override {}
private:
    void executeImpl() override {
        TaskReadScope::requested(123);
        if (fail_)
            throw std::runtime_error("task failure");
    }
    bool fail_;
};
}

CASE("data accessor counts encoded source reads, without double counting") {
    eckit::TmpDir dir;
    const auto path = dir / "encoded";
    { std::ofstream f(path.asString()); f << "01234567890123456789"; }
    ReadCountingFileHandle file(path.asString());
    file.openForRead();
    std::atomic<size_t> bytes{0};
    {
        TaskReadScope scope(bytes);
        GribJumpDataAccessor accessor(file, {5, 10});
        accessor.read({0, 4});
        accessor.read({2, 4}); // overlapping reads both count
        accessor.read();      // whole data section
    }
    file.close();
    EXPECT(bytes.load() == 18);
}

CASE("TaskGroup counters include failures and exclude cancelled tasks") {
    TaskGroup a, b;
    ReadTask success(a, 0), failed(a, 1, true), cancelled(a, 2), other(b, 0);
    success.execute();
    EXPECT(a.bytesRead() == 123);  // already merged before completion notification
    try { failed.execute(); } catch (const std::runtime_error&) {}
    EXPECT(a.bytesRead() == 246);
    cancelled.cancel();
    cancelled.execute();
    EXPECT(a.bytesRead() == 246);
    other.execute();
    EXPECT(b.bytesRead() == 123);
}

CASE("group read totals are published in the request thread metrics, not cumulative across groups") {
    for (size_t n : {3, 1}) {
        TaskGroup group;
        for (size_t i = 0; i < n; ++i)
            group.enqueueTask<ReadTask>();
        group.waitForTasks();
        group.report().raiseErrors();
        EXPECT(group.bytesRead() == n * 123);

        std::ostringstream log;
        eckit::Log::metrics().setStream(log);
        MetricsManager::instance().report();
        eckit::Log::metrics().reset();
        const auto json = eckit::JSONParser::decodeString(log.str());
        EXPECT(static_cast<long long>(json["count_data_bytes_read"]) == static_cast<long long>(n * 123));
    }
}

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
