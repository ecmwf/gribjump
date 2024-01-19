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

#include "gribjump/remote/WorkItem.h"
#include "gribjump/remote/ClientRequest.h"

namespace gribjump {
                // WorkItem w(exrequest, result, &cv, &m, &counter);

WorkItem::WorkItem(): rr_(nullptr) {}
WorkItem::WorkItem(RequestResult& rr): rr_(&rr) {}

void WorkItem::run(GribJump& gj) {
    ASSERT(rr_);
    std::vector<ExtractionResult> results = gj.extract(rr_->request().getRequest(), rr_->request().getRanges());
    rr_->storeResults(results);
    rr_->notify();
}

RequestResult::RequestResult(ExtractionRequest& request, ClientRequest* clientRequest): 
    request_(request), clientRequest_(clientRequest) {
        ASSERT(clientRequest_);
    }

void RequestResult::notify() {
    clientRequest_->notify();
}

}  // namespace gribjump
