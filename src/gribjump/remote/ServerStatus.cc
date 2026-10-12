/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#include "gribjump/remote/ServerStatus.h"

#include <unistd.h>
#include <sstream>

#include "eckit/runtime/Main.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {
namespace {
std::string threadId(std::thread::id id = std::this_thread::get_id()) {
    std::ostringstream out;
    out << id;
    return out.str();
}
const char* operationName(RequestType type) {
    switch (type) {
        case RequestType::EXTRACT:
            return "extract";
        case RequestType::AXES:
            return "axes";
        case RequestType::SCAN:
            return "scan";
        case RequestType::FORWARD_EXTRACT:
            return "forwarded-extract";
        case RequestType::FORWARD_SCAN:
            return "forwarded-scan";
        case RequestType::STATUS:
            return "status";
    }
    return "unknown";
}
double seconds(std::chrono::steady_clock::duration duration) {
    return std::chrono::duration<double>(duration).count();
}
double unixSeconds(std::chrono::system_clock::time_point time) {
    return std::chrono::duration<double>(time.time_since_epoch()).count();
}
}  // namespace

RequestStatus::RequestStatus(uint64_t requestId, RequestType operation, const LogContext& context) :
    id(requestId),
    operation_(operation),
    context_(context),
    thread_(threadId()),
    started_(std::chrono::steady_clock::now()),
    phaseStarted_(started_) {}

std::string RequestStatus::phase(const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto previous = phase_;
    phase_        = value;
    phaseStarted_ = std::chrono::steady_clock::now();
    return previous;
}
void RequestStatus::enqueued() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++tasks_.at("total");
    ++tasks_.at("queued");
}
void RequestStatus::started() {
    std::lock_guard<std::mutex> lock(mutex_);
    --tasks_.at("queued");
    ++tasks_.at("running");
}
void RequestStatus::finished(const char* outcome, bool wasRunning) {
    std::lock_guard<std::mutex> lock(mutex_);
    --tasks_.at(wasRunning ? "running" : "queued");
    ++tasks_.at(outcome);
}
void RequestStatus::json(eckit::JSON& json) const {
    std::string phase;
    std::chrono::steady_clock::time_point phaseStarted;
    std::map<std::string, size_t> tasks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        phase        = phase_;
        phaseStarted = phaseStarted_;
        tasks        = tasks_;
    }
    const auto now = std::chrono::steady_clock::now();
    json.startObject();
    json << "id" << id << "operation" << operationName(operation_) << "thread_id" << thread_;
    json << "phase" << phase << "elapsed_seconds" << seconds(now - started_);
    json << "phase_elapsed_seconds" << seconds(now - phaseStarted);
    json << "tasks" << tasks << "context" << context_;
    json.endObject();
}

thread_local std::shared_ptr<RequestStatus> ServerStatus::current_;
ServerStatus::ServerStatus() :
    startedAt_(std::chrono::system_clock::now()), started_(std::chrono::steady_clock::now()) {}
ServerStatus& ServerStatus::instance() {
    static ServerStatus status;
    return status;
}
std::shared_ptr<RequestStatus> ServerStatus::current() {
    return current_;
}
std::shared_ptr<RequestStatus> ServerStatus::add(RequestType type, const LogContext& context) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto request = std::make_shared<RequestStatus>(nextId_++, type, context);
    requests_.emplace(request->id, request);
    return request;
}
void ServerStatus::remove(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    requests_.erase(id);
}
void ServerStatus::worker(bool present) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (present)
        workers_.insert_or_assign(std::this_thread::get_id(), Worker{});
    else
        workers_.erase(std::this_thread::get_id());
}
void ServerStatus::workerTask(const std::shared_ptr<RequestStatus>& request, size_t task) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(std::this_thread::get_id());
    if (it != workers_.end()) {
        it->second = Worker{true, request, task};
    }
}

std::string ServerStatus::json() const {
    std::map<uint64_t, std::shared_ptr<RequestStatus>> requests;
    std::map<std::thread::id, Worker> workers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        requests = requests_;
        workers  = workers_;
    }
    size_t busy = 0;
    for (const auto& entry : workers)
        busy += entry.second.busy;
    std::ostringstream out;
    eckit::JSON json(out);
    json.startObject();
    json << "schema_version" << 1;
    json << "server";
    json.startObject();
    json << "hostname" << eckit::Main::hostname() << "pid" << static_cast<long>(::getpid());
    json << "gribjump_version" << LibGribJump::instance().version();
    json << "started_at_unix_seconds" << unixSeconds(startedAt_);
    json << "snapshot_at_unix_seconds" << unixSeconds(std::chrono::system_clock::now());
    json << "uptime_seconds" << seconds(std::chrono::steady_clock::now() - started_);
    json.endObject();
    json << "threads";
    json.startObject();
    json << "worker_count" << workers.size() << "busy" << busy << "idle" << workers.size() - busy;
    json << "active_request_handlers" << requests.size();
    json << "workers";
    json.startList();
    for (const auto& [id, worker] : workers) {
        json.startObject();
        json << "id" << threadId(id) << "state" << (worker.busy ? "busy" : "idle");
        if (worker.busy) {
            json << "task_id" << worker.task;
            if (worker.request)
                json << "request_id" << worker.request->id;
        }
        json.endObject();
    }
    json.endList();
    json.endObject();
    json << "active_requests";
    json.startList();
    for (const auto& entry : requests)
        entry.second->json(json);
    json.endList();
    json.endObject();
    return out.str();
}

StatusRequestScope::StatusRequestScope(RequestType type, const LogContext& context) :
    previous_(ServerStatus::current_) {
    ServerStatus::current_ = ServerStatus::instance().add(type, context);
}
StatusRequestScope::~StatusRequestScope() {
    ServerStatus::instance().remove(ServerStatus::current_->id);
    ServerStatus::current_ = std::move(previous_);
}
StatusPhase::StatusPhase(const std::string& phase) : request_(ServerStatus::current()) {
    if (request_)
        previous_ = request_->phase(phase);
}
StatusPhase::~StatusPhase() {
    if (request_)
        request_->phase(previous_);
}
}  // namespace gribjump
