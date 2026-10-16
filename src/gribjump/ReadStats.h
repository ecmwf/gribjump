/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0.
 */
#pragma once

#include <atomic>
#include <cstddef>

#include "eckit/io/FileHandle.h"
#include "eckit/serialisation/FileStream.h"

namespace gribjump {

/// Logical source bytes requested by a task, independent of filesystem/page cache.
/// One scope per executing task. Reads use an ordinary thread-local accumulator;
/// destruction merges it once into the owning TaskGroup, also on exception paths.
/// The scope must end BEFORE task completion is published.
class TaskReadScope {
public:

    explicit TaskReadScope(std::atomic<size_t>& groupBytes);
    ~TaskReadScope();

    TaskReadScope(const TaskReadScope&)            = delete;
    TaskReadScope& operator=(const TaskReadScope&) = delete;

    /// Add the requested length BEFORE the source read. Includes short/failed
    /// reads and repeated/overlapping ranges, not just unique file coverage.
    static void requested(long length);

private:

    static thread_local TaskReadScope* current_;
    std::atomic<size_t>& groupBytes_;
    size_t bytes_ = 0;
};

/// Source-file boundary: includes all reads through this handle (data, bitmap,
/// headers), but not reads from a MemoryHandle after a source was materialised.
class ReadCountingFileHandle : public eckit::FileHandle {
public:

    using eckit::FileHandle::FileHandle;
    long read(void* buffer, long length) override {
        TaskReadScope::requested(length);
        return eckit::FileHandle::read(buffer, length);
    }
};

/// Jump-info index-file reads use the serialisation API rather than DataHandle.
class ReadCountingFileStream : public eckit::FileStream {
public:

    using eckit::FileStream::FileStream;
    long read(void* buffer, long length) override {
        TaskReadScope::requested(length);
        return eckit::FileStream::read(buffer, length);
    }
};

}  // namespace gribjump
