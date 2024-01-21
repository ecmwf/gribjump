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

#include "gribjump/remote/ExtractRequest.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

ExtractTask::ExtractTask(size_t id, ExtractRequest* clientRequest, ExtractionRequest& request): Task(id, clientRequest),
    request_(request) {
}

ExtractTask::~ExtractTask() {
}

void ExtractTask::execute(GribJump& gj) {
    std::vector<ExtractionResult> results = gj.extract(request_.getRequest(), request_.getRanges());
    results_.swap(results);
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ExtractRequest::ExtractRequest(eckit::Stream& stream) : Request(stream) {

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ExtractRequest: numRequests = " << numRequests << std::endl;

    // flat vector of tasks, one per split request
    for (size_t i = 0; i < numRequests; i++) {

        const ExtractionRequest baseRequest = ExtractionRequest(client_);

        std::vector<std::string> split_keys = { "date", "time", "number" };
        std::vector<ExtractionRequest> splitRequests = baseRequest.split( split_keys );

        for (size_t j = 0; j < splitRequests.size(); j++) {
            LOG_DEBUG_LIB(LibGribJump) << "ExtractRequest: split request " << splitRequests[j].getRequest() << std::endl;
            tasks_.emplace_back(new ExtractTask(j, this, splitRequests[j]));
        }
        requestGroups_.push_back(splitRequests.size());
    }

    taskStatus_.resize(tasks_.size(), Task::Status::PENDING);
}

ExtractRequest::~ExtractRequest() {
    for (auto& task : tasks_) {
        delete task;
    }
}

void ExtractRequest::enqueueTasks() {
    WorkQueue& queue = WorkQueue::instance();
    for (size_t i = 0; i < tasks_.size(); i++) {
        WorkItem w(tasks_[i]);
        queue.push(w);
        LOG_DEBUG_LIB(LibGribJump) << "Pushed request (" << i << ") onto queue" << std::endl;
    }
}

void ExtractRequest::replyToClient() {       

    reportErrors();

    size_t nReq = 0;
    for (size_t group : requestGroups_){
        size_t nfieldsInOriginalReq = 0;
        for (size_t i = 0; i < group; i++) {
            nfieldsInOriginalReq += tasks_[nReq+i]->results().size();
        }
        client_ << nfieldsInOriginalReq;
        for (size_t i = 0; i < group; i++) {
            const std::vector<ExtractionResult>& results = tasks_[nReq]->results();
            for (auto& result : results) {
                client_ << result;
            }
            nReq++;
        }
    }
    ASSERT(nReq == tasks_.size());
}

}  // namespace gribjump
