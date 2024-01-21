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
/// @author Tiago Quintino

#include "gribjump/remote/WorkQueue.h"
#include "gribjump/remote/ScanRequest.h"

namespace gribjump {

ScanTask::ScanTask(size_t id, ExtractionRequest& request, ScanRequest* clientRequest): Task(id),
    request_(request), clientRequest_(clientRequest) 
{
    ASSERT(clientRequest_);
}

ScanTask::~ScanTask() {
}

void ScanTask::notify() {
    clientRequest_->notify(id());
}

void ScanTask::notifyError(const std::string& s) {
    clientRequest_->notifyError(id(), s);
}

void ScanTask::execute(GribJump& gj) {
    result_ = gj.scan( request_.getRequest() );
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ScanRequest::ScanRequest(eckit::Stream& stream) : Request(stream) {

    size_t numRequests;
    client_ >> numRequests;

    // Flat vector of requests
    for (size_t i = 0; i < numRequests; i++) {
        const ExtractionRequest baseRequest = ExtractionRequest(client_);
        std::vector<ExtractionRequest> splitRequests = baseRequest.split("number"); // todo: date, time.

        for (size_t j = 0; j < splitRequests.size(); j++) {
            tasks_.emplace_back(new ScanTask(j, splitRequests[j], this));
        }
        requestGroups_.push_back(splitRequests.size());
    }

    taskStatus_.resize(tasks_.size(), Task::Status::PENDING);
}

ScanRequest::~ScanRequest() {
    for (auto& task : tasks_) {
        delete task;
    }
}

void ScanRequest::enqueueTasks() {
    WorkQueue& queue = WorkQueue::instance();
    for (size_t i = 0; i < tasks_.size(); i++) {
        WorkItem w(tasks_[i]);
        queue.push(w);
        LOG_DEBUG_LIB(LibGribJump)  << "Pushed request (" << i << ") onto queue" << std::endl;
    }
}

void ScanRequest::replyToClient() {

    reportErrors();

    size_t nScannedFiles = 0;
    for (size_t i = 0; i < tasks_.size(); i++) {
        nScannedFiles += tasks_[i]->result();
    }
    client_ << nScannedFiles;
}

} // namespace gribjump