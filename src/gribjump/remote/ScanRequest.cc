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

#include "gribjump/remote/ScanRequest.h"

#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"

#include "gribjump/FDBService.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

ScanTask::ScanTask(size_t id, Request* clientRequest): Task(id, clientRequest) {
}

ScanTask::~ScanTask() {
}

//----------------------------------------------------------------------------------------------------------------------

ScanMARSTask::ScanMARSTask(size_t id, Request* clientRequest, const metkit::mars::MarsRequest& request): 
    ScanTask(id, clientRequest),
    request_(request) {
}

void ScanMARSTask::execute(GribJump& gj) {
    result_ = gj.scan( { request_ } );
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ScanFileTask::ScanFileTask(size_t id, Request* clientRequest, const eckit::PathName& path): ScanTask(id, clientRequest), file_(path) {
}

void ScanFileTask::execute(GribJump& gj) {
    result_ = gj.scan(file_);
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ScanRequest::ScanRequest(eckit::Stream& stream) : Request(stream) {

    bool byfiles;
    client_ >> byfiles;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: byfiles=" << byfiles << std::endl;

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: numRequests=" << numRequests << std::endl;

    if(byfiles) {

        std::vector<metkit::mars::MarsRequest> requests;
        for (size_t i = 0; i < numRequests; i++) {
            requests.emplace_back(metkit::mars::MarsRequest(client_));
        }

        std::vector<eckit::PathName> files = FDBService::instance().listFilesInRequest(requests);

        for (size_t i = 0; i < files.size(); i++) {
            tasks_.emplace_back(new ScanFileTask(i, this, files[i]));
        }
    }
    else {
        size_t count = 0;
        for (size_t i = 0; i < numRequests; i++) {
            metkit::mars::MarsRequest base = metkit::mars::MarsRequest(client_);

            std::vector<std::string> split_keys = { "date", "time", "number" };
            std::vector< metkit::mars::MarsRequest > splits = base.split(split_keys);

            for (size_t j = 0; j < splits.size(); j++) {
                tasks_.emplace_back(new ScanMARSTask(count, this, splits[j]));
                count++;
            }
        }
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