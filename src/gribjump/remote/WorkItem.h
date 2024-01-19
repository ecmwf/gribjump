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

#include <thread>
#include "eckit/container/Queue.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"

namespace gribjump {
                // WorkItem w(exrequest, result, &cv, &m, &counter);

class ClientRequest;

class RequestResult {
public: 
    RequestResult(ExtractionRequest& request, ClientRequest* clientRequest);

    ExtractionRequest request() {return request_;}
    const std::vector<ExtractionResult>& results() {return results_;}
    
    void storeResults(std::vector<ExtractionResult>& results) {
        results_.swap(results);
    }

    void notify();

private:
    ExtractionRequest request_;
    std::vector<ExtractionResult> results_;
    ClientRequest* clientRequest_;
};

class WorkItem {
public:
    WorkItem();
    WorkItem(RequestResult& rr);

    void run(GribJump& gj);

private:

    RequestResult* rr_;
};

}  // namespace gribjump
