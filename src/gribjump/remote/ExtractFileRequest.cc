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

#include "gribjump/remote/ExtractFileRequest.h"

#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/thread/AutoLock.h"

#include "metkit/mars/MarsExpension.h"


#include "gribjump/FDBService.h"
#include "gribjump/Config.h"

#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

ExtractFileTask::ExtractFileTask(size_t id, ExtractFileRequest* clientRequest) : Task(id, clientRequest){
}

ExtractFileTask::~ExtractFileTask() {
}

//----------------------------------------------------------------------------------------------------------------------

 PerFileTask::PerFileTask(size_t id, ExtractFileRequest* clientRequest, WorkPerFile2* work) : 
    ExtractFileTask(id, clientRequest), work_(work) {
 }

void PerFileTask::execute(GribJump& gj) {

    NOTIMP;
#if 0
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
#endif
    notify();
}

//----------------------------------------------------------------------------------------------------------------------


class FillMapCB : public metkit::mars::FlattenCallback {
public:
    FillMapCB(map_results_t& mapres, const std::vector<Range>& ranges) : mapres_(mapres), ranges_(ranges) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        mapres_.insert({req.asString(), {ranges_, nullptr}});
    }
    map_results_t& mapres_;
    std::vector<Range> ranges_;
};

void requestToMap(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges, map_results_t& mapres) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;

    FillMapCB cb(mapres, ranges);

    expansion.flatten(ctx, request, cb);
}
//----------------------------------------------------------------------------------------------------------------------

class ToStrCallback : public metkit::mars::FlattenCallback {
public:
    ToStrCallback(std::vector<flatkey_t>& fieldKeys) : fieldKeys_(fieldKeys) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        fieldKeys_.push_back(req.asString());
    }

    std::vector<flatkey_t>& fieldKeys_;
};

void requestToStr(const metkit::mars::MarsRequest& request, std::vector<flatkey_t>& fieldKeys) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;

    ToStrCallback cb(fieldKeys);

    expansion.flatten(ctx, request, cb);
}

//----------------------------------------------------------------------------------------------------------------------


ExtractFileRequest::ExtractFileRequest(eckit::Stream& stream) : Request(stream) {

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ExtractFileRequest: numRequests = " << numRequests << std::endl;

    // receive requests
    std::vector<ExtractionRequest> reqs;
    reqs.reserve(numRequests);
    for (size_t i = 0; i < numRequests; i++) { 
        ExtractionRequest req(client_);
        reqs.push_back(req);
    }

#if 1
        for(auto& req : reqs) {
            const metkit::mars::MarsRequest& mreq = req.getRequest();
            LOG_DEBUG_LIB(LibGribJump) << "current request " << mreq << std::endl;
            std::cout << "current request params " << mreq.params() << std::endl;
            for(auto& p: mreq.params()) {
                std::cout << "param " << p << " values " << mreq.values(p) << std::endl;
            }
        }
#endif

        // create the map of results
        for(size_t i = 0; i < reqs.size(); ++i) {
            requestToMap(reqs[i].getRequest(), reqs[i].getRanges(), results_);
        }

        // we should do some check not to merge on keys like class and stream
        metkit::mars::MarsRequest unionRequest;
        for(auto& req : reqs) {
            unionRequest.merge(req.getRequest());
        }

        eckit::AutoLock<FDBService> lock(FDBService::instance()); // worker threads wont touch FDB, only main thread, however this is not good for multiple clients

        std::map<eckit::PathName, WorkPerFile2*> per_file_work;

        fdb5::FDBToolRequest fdbreq(unionRequest);
        auto listIter = FDBService::instance().fdb().list(fdbreq, true);

        fdb5::ListElement elem;
        while (listIter.next(elem)) {
                
                fdb5::Key key = elem.combinedKey(true);

                flatkey_t flatkey = key;

                auto resit = results_.find(flatkey);
                if(resit != results_.end()) {
                    // this is a key we are interested in
                    const fdb5::FieldLocation& loc = elem.location();
                    eckit::Offset offset = loc.offset();

                    eckit::PathName filepath = elem.location().uri().path();

                    auto it = per_file_work.find(filepath);
                    if(it != per_file_work.end()) {
                        it->second->fields_.push_back();
                    }
                    else { 
                        WorkPerFile2* work = new WorkPerFile2{ filepath, {key, offset} };
                        merged_work.emplace(filepath, work);
                    }

                }

                // // LOG_DEBUG_LIB(LibGribJump) << "FOUND key=" << key << " " << loc << std::endl;

                // eckit::PathName filepath = loc.uri().path();

                // auto it = merged.find(filepath);
                // if(it != merged.end()) {
                //     it->second->fields_.push_back({reqId, field_id, loc.offset(), reqs[reqId].getRanges(), ExtractionResult{} });
                // }
                // else { 
                //     WorkPerFile2* work = new WorkPerFile2{ filepath, {{ reqId, field_id, loc.offset(), reqs[reqId].getRanges(), ExtractionResult{} }} };
                //     merged.emplace(filepath, work);
                // }

                // field_id++;
            }



        for (size_t reqId = 0; reqId < numRequests; ++reqId) {    

            const metkit::mars::MarsRequest& request = reqs[reqId].getRequest();
            LOG_DEBUG_LIB(LibGribJump) << "Extract request " << request << std::endl;
            
            if(field_id == 0) 
                throw eckit::BadValue("No fields found for request " + reqs[reqId].getRequest().asString(), Here());

            nb_fields_in_client_request_.push_back(field_id);
        }

        size_t countTasks = 0;
        for(auto& file : merged) {
            LOG_DEBUG_LIB(LibGribJump) << "Extracting from file " << file.first << std::endl;
            tasks_.emplace_back(new PerFileTask(countTasks, this, file.second));
            countTasks++;
                    }
#endif

    taskStatus_.resize(tasks_.size(), Task::Status::PENDING);
}

ExtractFileRequest::~ExtractFileRequest() {
    for (auto& task : tasks_) {
        delete task;
    }
}

void ExtractFileRequest::enqueueTasks() {
    WorkQueue& queue = WorkQueue::instance();
    for (size_t i = 0; i < tasks_.size(); i++) {
        WorkItem w(tasks_[i]);
        queue.push(w);
        LOG_DEBUG_LIB(LibGribJump) << "Queued task " << i << std::endl;
    }
    eckit::Log::info() << "ExtractFileRequest: " << tasks_.size() << " tasks enqueued" << std::endl;
}

void ExtractFileRequest::replyToClient() {       

    NOTIMP; // TODO: merge results before sending to client

    reportErrors();

#if 0
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
            WorkPerFile2* work = static_cast<PerFileTask*>(tasks_[taskid])->work();
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



    timer.stop();

    double rate = nValues / timer.elapsed();
    double byterate = rate * sizeof(double) / 1024.0; // KiB/sec

    eckit::Log::info() << "ExtractFileRequest sent " << eckit::Plural(nValues,"value") << " @ " << rate << " values/s " << byterate << "KiB/s" << std::endl;

#endif
}

}  // namespace gribjump
