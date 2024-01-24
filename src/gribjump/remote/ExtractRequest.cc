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

 ExtractPerFileTask::ExtractPerFileTask(size_t id, ExtractRequest* clientRequest, WorkPerFile* work) : 
    ExtractTask(id, clientRequest), work_(work) {
 }

void ExtractPerFileTask::execute(GribJump& gj) {

    bool sortOffsets = true;
    if(sortOffsets) {
        
        std::sort(work_->fields_.begin(), work_->fields_.end(), [](const WorkPerField& a, const WorkPerField& b) { return a.field_offset_ < b.field_offset_; });
        
        std::vector<eckit::Offset> offsets;
        offsets.reserve(work_->fields_.size());
        std::vector<std::vector<Range>> ranges;
        ranges.reserve(work_->fields_.size());
        for(auto& field : work_->fields_) {
            offsets.push_back(field.field_offset_);
            ranges.push_back(field.ranges_);
        }

        std::vector<ExtractionResult> r = gj.extract(work_->file_, offsets, ranges);

        for(size_t i = 0; i < r.size(); ++i) {
            work_->fields_[i].result_ = r[i];
        }
    }
    else {
        for(auto& field : work_->fields_) {
            std::vector<ExtractionResult> r = gj.extract(work_->file_, { field.field_offset_ }, { field.ranges_ });
            std::copy(r.begin(), r.end(), std::back_inserter(results_));
        }
    }

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

    nb_fields_in_client_request_.reserve(numRequests);

    bool mergeBefore = LibGribJump::instance().config().getBool("mergeBefore", false);
    if(mergeBefore) {

        eckit::AutoLock<FDBService> lock(FDBService::instance()); // worker threads wont touch FDB, only main thread, however this is not good for multiple clients
        fdb5::FDB& fdb = FDBService::instance().fdb();

        std::map<eckit::PathName, WorkPerFile*> merged;

    
        for (size_t reqId = 0; reqId < numRequests; ++reqId) {    
            auto listIter = fdb.list(fdb5::FDBToolRequest(reqs[reqId].getRequest()), true);
            
            
            size_t field_id = 0;
            fdb5::ListElement elem;
            while (listIter.next(elem)) {
                
                // fdb5::Key key = elem.combinedKey(true);
                
                const fdb5::FieldLocation& loc = elem.location();
                LOG_DEBUG_LIB(LibGribJump) << loc << std::endl;

                eckit::PathName filepath = loc.uri().path();

                auto it = merged.find(filepath);
                if(it != merged.end()) {
                    it->second->fields_.push_back({reqId, field_id, loc.offset(), reqs[reqId].getRanges(), ExtractionResult{} });
                }
                else { 
                    WorkPerFile* work = new WorkPerFile{ filepath, {{ reqId, field_id, loc.offset(), reqs[reqId].getRanges(), ExtractionResult{} }} };
                    merged.emplace(filepath, work);
                }

                field_id++;
            }

            nb_fields_in_client_request_.push_back(field_id);

            size_t countTasks = 0;
            for(auto& file : merged) {
                LOG_DEBUG_LIB(LibGribJump) << "Extracting from file " << file.first << std::endl;
                size_t taskid = reqId * numRequests + countTasks;
                tasks_.emplace_back(new ExtractPerFileTask(taskid, this, file.second));
                countTasks++;
            }
            requestGroups_.push_back(countTasks);
        }

        taskStatus_.resize(tasks_.size(), Task::Status::PENDING);
        return;
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
            nb_fields_in_client_request_ = requestGroups_;
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
            nb_fields_in_client_request_ = requestGroups_;
        }
    }

    ASSERT(nb_fields_in_client_request_.size() == requestGroups_.size());

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

    size_t nValues = 0;

    bool mergeBefore = LibGribJump::instance().config().getBool("mergeBefore", false);
    if(mergeBefore) {

        std::vector< std::vector<ExtractionResult>> replies; // for all requests, for all fields in request
        replies.resize(nb_fields_in_client_request_.size());
        for(size_t i = 0; i < nb_fields_in_client_request_.size(); ++i) {
            replies[i].resize(nb_fields_in_client_request_[i]);
        }

        for(size_t taskid = 0; taskid < tasks_.size(); ++taskid) {
            WorkPerFile* work = static_cast<ExtractPerFileTask*>(tasks_[taskid])->work();
            for(auto& field : work->fields_) {
                replies[field.client_request_id_][field.field_id_] = field.result_;
            }
        }

        for(size_t i = 0; i < replies.size(); ++i) {
            client_ << replies[i].size();
            for(size_t j = 0; j < replies[i].size(); ++j) {
                client_ << replies[i][j];
                nValues += replies[i][j].total_values();
            }
        }
    }
    else {
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
                    for (size_t i = 0; i < result.nrange(); i++) {
                        nValues += result.total_values();
                    }
                    client_ << result;
                }
                nReq++;
            }
        }
        ASSERT(nReq == tasks_.size());  
    }

    timer.stop();

    double rate = nValues / timer.elapsed();
    double byterate = rate * sizeof(double) / 1024.0; // KiB/sec

    eckit::Log::info() << "ExtractRequest sent " << eckit::Plural(nValues,"value") << " @ " << rate << " values/s " << byterate << "KiB/s" << std::endl;
}

}  // namespace gribjump
