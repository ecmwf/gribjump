/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "gribjump/Metrics.h"
#include "gribjump/remote/Protocol.h"

namespace gribjump {

/// Owned independently of handlers/tasks, so monitoring never dereferences live
/// TaskGroup pointers. All updates are short; no JSON or I/O under these locks.
class RequestStatus {
public:

    RequestStatus(uint64_t id, RequestType operation, const LogContext& context);
    const uint64_t id;

    std::string phase(const std::string& value);
    void enqueued();
    void started();
    void finished(const char* outcome, bool wasRunning);
    void json(eckit::JSON&) const;

private:

    const RequestType operation_;
    const LogContext context_;
    const std::string thread_;
    const std::chrono::steady_clock::time_point started_;
    mutable std::mutex mutex_;
    std::string phase_ = "receiving";
    std::chrono::steady_clock::time_point phaseStarted_;
    std::map<std::string, size_t> tasks_{{"total", 0},     {"queued", 0}, {"running", 0},
                                         {"completed", 0}, {"failed", 0}, {"cancelled", 0}};
};

class ServerStatus {
public:

    static ServerStatus& instance();
    static std::shared_ptr<RequestStatus> current();
    std::string json() const;

    std::shared_ptr<RequestStatus> add(RequestType, const LogContext&);
    void remove(uint64_t);
    void worker(bool present);
    void workerTask(const std::shared_ptr<RequestStatus>&, size_t task = 0);

private:

    ServerStatus();
    struct Worker {
        bool busy = false;
        std::shared_ptr<RequestStatus> request;
        size_t task = 0;
    };
    mutable std::mutex mutex_;
    uint64_t nextId_ = 0;
    std::map<uint64_t, std::shared_ptr<RequestStatus>> requests_;
    std::map<std::thread::id, Worker> workers_;
    const std::chrono::system_clock::time_point startedAt_;
    const std::chrono::steady_clock::time_point started_;
    friend class StatusRequestScope;
    static thread_local std::shared_ptr<RequestStatus> current_;
};

/// Register after decoding the header, before receiving the request body.
/// STATUS itself is excluded. Restores thread-local state on every exit path.
class StatusRequestScope {
public:

    StatusRequestScope(RequestType, const LogContext&);
    ~StatusRequestScope();
    StatusRequestScope(const StatusRequestScope&)            = delete;
    StatusRequestScope& operator=(const StatusRequestScope&) = delete;

private:

    std::shared_ptr<RequestStatus> previous_;
};

/// Nestable phase marker; inactive for local (non-server) calls.
class StatusPhase {
public:

    explicit StatusPhase(const std::string& phase);
    ~StatusPhase();
    StatusPhase(const StatusPhase&)            = delete;
    StatusPhase& operator=(const StatusPhase&) = delete;

private:

    std::shared_ptr<RequestStatus> request_;
    std::string previous_;
};

}  // namespace gribjump
