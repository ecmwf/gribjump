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

#pragma once

#include "gribjump/remote/WorkQueue.h"

namespace gribjump {
    
class ClientRequest {
public:

    ClientRequest(eckit::Stream& stream): client_(stream) {

        size_t numRequests;
        client_ >> numRequests;

        // Flat vector of requests
        for (size_t i = 0; i < numRequests; i++) {
            const ExtractionRequest baseRequest = ExtractionRequest(client_);
            std::vector<ExtractionRequest> splitRequests = baseRequest.split("number"); // todo: date, time.

            for (size_t j = 0; j < splitRequests.size(); j++) {
                requests_.emplace_back(RequestResult(splitRequests[j], this));
            }
            requestGroups_.push_back(splitRequests.size());
        }
    }

    ~ClientRequest() {
    }

    void notify() {
        std::lock_guard<std::mutex> lk(m_);
        counter_++;
        cv_.notify_one();
    }

    void doWork(){
        WorkQueue& queue = WorkQueue::instance();
        for (size_t i = 0; i < requests_.size(); i++) {
            WorkItem w(requests_[i]);
            queue.push(w);
            eckit::Log::debug<LibGribJump>() << "Pushed request (" << i << ") onto queue" << std::endl;
        }

        // Wait for all results to be ready.
        eckit::Log::debug<LibGribJump>() << "Waiting for results" << std::endl;
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]{return counter_ == requests_.size();});

    }

    void replyToClient(){
        size_t req = 0;
        for (size_t group : requestGroups_){
            size_t nfieldsInOriginalReq = 0;
            for (size_t i = 0; i < group; i++) {
                nfieldsInOriginalReq += requests_[req+i].results().size();
            }
            client_ << nfieldsInOriginalReq;
            for (size_t i = 0; i < group; i++) {
                RequestResult& rr = requests_[req];
                const std::vector<ExtractionResult>& results = rr.results();
                for (auto& result : results) {
                    client_ << result;
                }
                req++;
            }
        }
        ASSERT(req == requests_.size());
    }

private:
    eckit::Stream& client_;
    GribJump gj_;
    std::condition_variable cv_;
    std::mutex m_;
    std::vector<RequestResult> requests_;
    int counter_ = 0;
    std::vector<size_t> requestGroups_;
};

} // namespace gribjump
