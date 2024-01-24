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

#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/thread/AutoLock.h"

#include "gribjump/FDBService.h"
#include "gribjump/Config.h"

#include "gribjump/remote/ExtractRequest.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

ExtractTask::ExtractTask(size_t id, ExtractRequest* clientRequest) : Task(id, clientRequest){
}

ExtractTask::~ExtractTask() {
}

//----------------------------------------------------------------------------------------------------------------------

 ExtractMARSTask::ExtractMARSTask(size_t id, ExtractRequest* clientRequest, ExtractionRequest& request) : 
    ExtractTask(id, clientRequest), request_(request) {
 }

void ExtractMARSTask::execute(GribJump& gj) {
    std::vector<ExtractionResult> results = gj.extract(request_.getRequest(), request_.getRanges());
    results_.swap(results);
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ExtractFDBLocTask::ExtractFDBLocTask(size_t id, ExtractRequest* clientRequest, std::vector<eckit::URI> fields, std::vector<Range> ranges) :
    ExtractTask(id, clientRequest), fields_(std::move(fields)), ranges_(std::move(ranges)) {
}

void ExtractFDBLocTask::execute(GribJump& gj) {
    std::vector<ExtractionResult> results = gj.extract(fields_, ranges_); 
    results_.swap(results);
    notify();
}

//----------------------------------------------------------------------------------------------------------------------

ExtractRequest::ExtractRequest(eckit::Stream& stream) : Request(stream) {

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ExtractRequest: numRequests = " << numRequests << std::endl;

    // receive requests
    std::vector<ExtractionRequest> reqs;
    reqs.reserve(numRequests);
    for (size_t i = 0; i < numRequests; i++) { 
        ExtractionRequest req(client_);
        reqs.push_back(req);
    }

    bool distributeByFile = LibGribJump::instance().config().getBool("distributeByFile", false);

    for (size_t i = 0; i < numRequests; i++) {

        size_t countTasks = 0;
        const ExtractionRequest& baseRequest = reqs[i];

        /// @todo: XXX the order here needs to be checked -- are we returning the results in the correct order?

        if(distributeByFile) {
            std::vector<eckit::URI> fields = FDBService::instance().fieldLocations(baseRequest.getRequest());

            for (size_t j = 0; j < fields.size(); j++) {
                LOG_DEBUG_LIB(LibGribJump) << "Extracting from " << fields[j] << std::endl;
                tasks_.emplace_back(new ExtractFDBLocTask(countTasks, this, { fields[j] }, baseRequest.getRanges()));
                countTasks++;
            }
            requestGroups_.push_back(fields.size());
        }
        else {
            std::vector<std::string> split_keys = { "date", "time", "number" };
            std::vector<ExtractionRequest> splitRequests = baseRequest.split( split_keys );
            
            for (size_t j = 0; j < splitRequests.size(); j++) {
                LOG_DEBUG_LIB(LibGribJump) << "ExtractRequest: split request " << splitRequests[j].getRequest() << std::endl;
                tasks_.emplace_back(new ExtractMARSTask(countTasks, this, splitRequests[j]));
                countTasks++;
            }
            requestGroups_.push_back(splitRequests.size());
        }
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
        LOG_DEBUG_LIB(LibGribJump) << "Queued task " << i << std::endl;
    }
    eckit::Log::info() << "ExtractRequest: " << tasks_.size() << " tasks enqueued" << std::endl;
}

void ExtractRequest::replyToClient() {       

    reportErrors();

    eckit::Timer timer;

    size_t nReq = 0;
    size_t nValues = 0;
    for (size_t group : requestGroups_){
        size_t nfieldsInOriginalReq = 0;
        for (size_t i = 0; i < group; i++) {
            nfieldsInOriginalReq += tasks_[nReq+i]->results().size();
        }
        client_ << nfieldsInOriginalReq;
        for (size_t i = 0; i < group; i++) {
            const std::vector<ExtractionResult>& results = tasks_[nReq]->results();
            for (auto& result : results) {
                for (size_t i = 0; i < result.nrange(); i++) {
                    nValues += result.nvalues(i);
                }
                client_ << result;
            }
            nReq++;
        }
    }

    timer.stop();

    double rate = nValues / timer.elapsed();
    double byterate = rate * sizeof(double) / 1024.0; // KiB/sec

    eckit::Log::info() << "ExtractRequest sent " << eckit::Plural(nValues,"value") << " @ " << rate << " values/s " << byterate << "KiB/s" << std::endl;

    ASSERT(nReq == tasks_.size());
}

}  // namespace gribjump
