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
#include "eckit/config/Resource.h"

#include "metkit/mars/MarsExpension.h"


#include "gribjump/FDBService.h"
#include "gribjump/Config.h"

#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

/// @todo these need to be checked and ensure the fdb list also returns the same order
///       this should be a function from metkit::mars::MarsRequest probably using information 
///       from metkit MARS language to know the order of the keys
const std::vector<std::string> keysorder = { "class", "expver", "stream", "date", "time", "domain", "type", "levtype", "levellist", "step", "number", "param" };

std::string requestToStr(const metkit::mars::MarsRequest& request) {
    std::stringstream ss;
    std::string separator = "";
    for(const auto& key : keysorder) {
        if(request.has(key)) {
            ss << separator << key << "=" << request[key];
        }
        separator = ",";
    }
    return ss.str();
}

//----------------------------------------------------------------------------------------------------------------------

ExtractFileTask::ExtractFileTask(size_t id, ExtractFileRequest* clientRequest) : Task(id, clientRequest), request_(clientRequest) {
}

ExtractFileTask::~ExtractFileTask() {
}

//----------------------------------------------------------------------------------------------------------------------

 PerFileTask::PerFileTask(size_t id, ExtractFileRequest* clientRequest, PerFileWork* work) : 
    ExtractFileTask(id, clientRequest), work_(work) {
 }

void PerFileTask::execute(GribJump& gj) {
    eckit::Timer timer;
    auto id = std::this_thread::get_id();
    std::stringstream ss;
    ss << id;
    std::string thread_id = ss.str();
    eckit::Timer full_timer("Thread total time. Thread: " + thread_id);

    const map_ranges_t& allranges = request_->ranges();

    std::sort(work_->jumps_.begin(), work_->jumps_.end(), [](const KeyOffset& a, const KeyOffset& b) { return a.offset_ < b.offset_; });

    std::vector<eckit::Offset> offsets;
    offsets.reserve(work_->jumps_.size());
    std::vector<ranges_t> ranges;
    ranges.reserve(work_->jumps_.size());
    for(auto& jump : work_->jumps_) {
        offsets.push_back(jump.offset_);
        ranges.push_back( allranges.at(jump.key_) );
    }
    
    timer.reset("PerFileTask : Sorted offsets Thread: " + thread_id);
    std::vector<ExtractionResult*> r = gj.extract(work_->file_, offsets, ranges);
    timer.reset("PerFileTask : Extracted results Thread: " + thread_id);

    for(size_t i = 0; i < work_->jumps_.size(); ++i) {
        partial_results_.insert( { work_->jumps_[i].key_, r[i] } );
    }

    notify();
    timer.reset("PerFileTask : Notified. Thread: " + thread_id);
}

//----------------------------------------------------------------------------------------------------------------------

class FillMapCB : public metkit::mars::FlattenCallback {
public:
    FillMapCB(map_results_t& mapres, map_ranges_t& mapranges, const std::vector<Range>& ranges) : mapres_(mapres),  mapranges_(mapranges), ranges_(ranges) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        std::string reqkey = requestToStr(req);
        ASSERT(mapres_.find(reqkey) == mapres_.end());
        mapres_.insert({reqkey, nullptr});
        mapranges_.insert({reqkey, ranges_});
    }
    map_results_t& mapres_;
    map_ranges_t& mapranges_;
    std::vector<Range> ranges_;
};

void requestToMap(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges, map_results_t& mapres, map_ranges_t& mapranges) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;

    FillMapCB cb(mapres, mapranges, ranges);

    expansion.flatten(ctx, request, cb);
}

void requestToMapNoFlatten(std::vector<ExtractionRequest> requests, map_results_t& mapres, map_ranges_t& mapranges) {
    // do it all at once, no flattening. Assume request is one field (as it will be from polytope).
    
    for (auto& req : requests) {
        std::string reqkey = requestToStr(req.getRequest());
        mapres.insert({reqkey, nullptr});
        mapranges.insert({reqkey, req.getRanges()});
    }
}

//----------------------------------------------------------------------------------------------------------------------

class ToStrCallback : public metkit::mars::FlattenCallback {
public:
    ToStrCallback(std::vector<flatkey_t>& fieldKeys) : fieldKeys_(fieldKeys) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        fieldKeys_.push_back(requestToStr(req));
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


ExtractFileRequest::ExtractFileRequest(eckit::Stream& stream) : Request(stream), flattenRequest_(eckit::Resource<bool>("$GRIBJUMP_FLATTEN_REQUESTS", true)) {

    eckit::Timer timer;

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ExtractFileRequest received " << eckit::Plural(numRequests, "client request") << std::endl;

    // receive requests
    received_requests_.reserve(numRequests);
    for (size_t i = 0; i < numRequests; i++) { 
        ExtractionRequest req(client_);
        received_requests_.push_back(req);
    }
    
    timer.reset("ExtractFileRequest : Received all requests");

    // create the map of results

    if (flattenRequest_){
        std::cout << "Flattening requests" << std::endl;
        for(size_t i = 0; i < received_requests_.size(); ++i) {
            requestToMap(received_requests_[i].getRequest(), received_requests_[i].getRanges(), results_, ranges_);
        }
    }
    else {
        std::cout << "No flattening requests -- assuming single requests" << std::endl;
        requestToMapNoFlatten(received_requests_, results_, ranges_);
    }

    LOG_DEBUG_LIB(LibGribJump) << "Results to be computed " << results_.size() << std::endl;

    timer.reset("ExtractFileRequest : Created map for results");

    // merge requests
    /// @todo: we should do some check not to merge on keys like class and stream
    metkit::mars::MarsRequest unionRequest = received_requests_.front().getRequest();  
    for(size_t i = 1; i < received_requests_.size(); ++i) {
        unionRequest.merge(received_requests_[i].getRequest());
    }
    LOG_DEBUG_LIB(LibGribJump) << "Union request " << unionRequest << std::endl;

    timer.reset("ExtractFileRequest : Created union request for resultss");

    // fdb list to get locations
    eckit::AutoLock<FDBService> lock(FDBService::instance()); // worker threads wont touch FDB, only main thread, however this is not good for multiple clients

    std::map<eckit::PathName, PerFileWork*> per_file_work;

    fdb5::FDBToolRequest fdbreq(unionRequest);
    auto listIter = FDBService::instance().fdb().list(fdbreq, true);

    fdb5::ListElement elem;
    while (listIter.next(elem)) {
                
        // the order in this key should match the order in requestToStr
        flatkey_t key = elem.combinedKey(true);
        LOG_DEBUG_LIB(LibGribJump) << "FDB LIST found " << key << std::endl;

        auto resit = results_.find(key);
        if(resit != results_.end()) {

            // LOG_DEBUG_LIB(LibGribJump) << "Work found for key=" << key << std::endl;

            // this is a key we are interested in
            const fdb5::FieldLocation& loc = elem.location();
            eckit::Offset offset = loc.offset();

            eckit::PathName filepath = elem.location().uri().path();

            auto it = per_file_work.find(filepath);
            if(it != per_file_work.end()) {
                it->second->jumps_.push_back( {key, offset} );
            }
            else { 
                PerFileWork* work = new PerFileWork{ filepath, {{key, offset}} };
                per_file_work.insert( { filepath, work} );
                LOG_DEBUG_LIB(LibGribJump) << "Work for file=" << filepath << std::endl;
            }

        }
    }

    size_t countTasks = 0;
    for(auto& file : per_file_work) {
        LOG_DEBUG_LIB(LibGribJump) << "Extracting from file " << file.first << std::endl;
        enqueueTask(new PerFileTask(countTasks, this, file.second));
        countTasks++;
    }

    timer.reset("ExtractFileRequest : Generated all tasks");

    // LOG_DEBUG_LIB(LibGribJump)  << "All tasks created. Time in ExtractFileRequest: " << timer.elapsed() << std::endl;
}

ExtractFileRequest::~ExtractFileRequest() {
    for (auto& task : tasks_) {
        delete task;
    }
    for(auto& res : results_) {
        delete res.second;
    }
}

void ExtractFileRequest::enqueueTasks() {
    // all tasks were already pushed
    eckit::Log::info() << "ExtractFileRequest: " << tasks_.size() << " tasks enqueued" << std::endl;
}

void ExtractFileRequest::enqueueTask(ExtractFileTask* task) {

    tasks_.emplace_back(task);
    taskStatus_.push_back(Task::Status::PENDING);
    WorkItem w(task);
    WorkQueue& queue = WorkQueue::instance();
    queue.push(w);
    LOG_DEBUG_LIB(LibGribJump) << "Queued task " <<  tasks_.size() << std::endl;
}


//----------------------------------------------------------------------------------------------------------------------

class CollectResultsCB : public metkit::mars::FlattenCallback {
public:
    CollectResultsCB(ExtractFileRequest& clientReq) : clientReq_(clientReq) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) {
        flatkey_t key = requestToStr(req);
        ExtractionResult* r = clientReq_.results(key);
        collected_.push_back(r);
    }

    ExtractFileRequest& clientReq_;
    std::vector<ExtractionResult*> collected_;
};

std::vector<ExtractionResult*> orderedCollectResults(const metkit::mars::MarsRequest& request, ExtractFileRequest& clientReq) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;

    CollectResultsCB cb(clientReq);
    expansion.flatten(ctx, request, cb);

    return cb.collected_;
}

//----------------------------------------------------------------------------------------------------------------------

void ExtractFileRequest::replyToClient() {       

    eckit::Timer timer;

    reportErrors();

    // merge results
    for(auto& task: tasks_) {
        map_results_t& partial = task->results();
        for(const auto& res : partial) {
            results_[res.first] = res.second;
        }
    }

    size_t nValues = 0;

    // collect successful (non nullptr) requests in order & send to client
    if (flattenRequest_){
        // collect successful (non nullptr) requests in order & send to client
        for(size_t i = 0; i < received_requests_.size(); ++i) {

            const metkit::mars::MarsRequest& mreq = received_requests_[i].getRequest();
            std::vector<ExtractionResult*> collected = orderedCollectResults(mreq, *this);
            
            size_t nValidResults = std::count_if(collected.begin(), collected.end(), [](ExtractionResult* r) { return r != nullptr; });
            client_ << nValidResults;
            
            for(auto& res : collected) {
                if(res != nullptr) {
                    ExtractionResult& result = *res;
                    for (size_t i = 0; i < result.nrange(); i++) {
                        nValues += result.total_values();
                    }
                    client_ << result;
                }
            }
        }
    }
    else{
        // SPECIAL CASE: we know that the requests are one field each, so we can skip the very expensive un-flattening
        for(size_t i = 0; i < received_requests_.size(); ++i) {
            
            const metkit::mars::MarsRequest& mreq = received_requests_[i].getRequest();
            flatkey_t key = requestToStr(mreq);
            ExtractionResult* r = results_[key]; // We are assuming there is exactly one result per request
            
            size_t nValidResults = (r != nullptr) ? 1 : 0;
            client_ << nValidResults;

            if(r != nullptr) {
                ExtractionResult& result = *r;
                for (size_t i = 0; i < result.nrange(); i++) {
                    nValues += result.total_values();
                }
                client_ << result;
            }
        }
    }

    double rate = nValues / timer.elapsed();
    double byterate = rate * sizeof(double) / 1024.0; // KiB/sec

    eckit::Log::info() << "ExtractFileRequest sent " << eckit::Plural(nValues,"value") << " @ " << rate << " values/s " << byterate << "KiB/s" << std::endl;
}

}  // namespace gribjump
