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

#include "gribjump/remote/Request.h"
#include "gribjump/Engine.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

Request::Request(eckit::Stream& stream, LogContext ctx) : client_(stream), metrics_(ctx) {
}

Request::~Request() {}

void Request::reportErrors() {
    engine_.reportErrors(client_);
}

//----------------------------------------------------------------------------------------------------------------------

ScanRequest::ScanRequest(eckit::Stream& stream) : Request(stream) {

    client_ >> byfiles_;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: byfiles=" << byfiles_ << std::endl;

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: numRequests=" << numRequests << std::endl;

    for (size_t i = 0; i < numRequests; i++) {
        requests_.emplace_back(metkit::mars::MarsRequest(client_));
    }
}

ScanRequest::~ScanRequest() {
}

void ScanRequest::execute() {
    nfiles_ = engine_.scan(requests_, byfiles_);
}

void ScanRequest::replyToClient() {
    reportErrors();
    client_ << nfiles_;
}

//----------------------------------------------------------------------------------------------------------------------

ExtractRequest::ExtractRequest(eckit::Stream& stream) : Request(stream) {

    // Receive the requests
    // Temp, repackage the requests from old format into format the engine expects
    eckit::Timer timer;

    size_t nRequests;
    client_ >> nRequests;

    std::vector<ExtractionRequest> requests;
    for (size_t i = 0; i < nRequests; i++) {
        ExtractionRequest req(client_);
        requests.push_back(req);
    }

    for (auto& req : requests) {
        marsRequests_.push_back(req.getRequest());
        ranges_.push_back(req.getRanges());
    }

    flatten_ = false; // xxx hard coded for now

    metrics_.nRequests = nRequests;
    metrics_.timeReceive = timer.elapsed();
}

ExtractRequest::~ExtractRequest() {
}

void ExtractRequest::execute() {
    eckit::Timer timer;

    results_ = engine_.extract(marsRequests_, ranges_, flatten_);
    engine_.updateMetrics(metrics_);

    if (LibGribJump::instance().debug()) {
        for (auto& pair : results_) {
            LOG_DEBUG_LIB(LibGribJump) << pair.first << ": ";
            for (auto& item : pair.second) {
                item->debug_print();
            }
        }
    }
    metrics_.timeExecute = timer.elapsed();
}

void ExtractRequest::replyToClient() {
    eckit::Timer timer;

    reportErrors();

    // Send the results, again repackage.

    size_t nRequests = marsRequests_.size();
    LOG_DEBUG_LIB(LibGribJump) << "Sending " << nRequests << " results to client" << std::endl;

    for (size_t i = 0; i < nRequests; i++) {
        LOG_DEBUG_LIB(LibGribJump) << "Sending result " << i << " to client" << std::endl;

        auto it = results_.find(marsRequests_[i]);
        ASSERT(it != results_.end());
        std::vector<std::unique_ptr<ExtractionItem>>& items = it->second;
        // ExtractionItems items = it->second;
        size_t nfields = items.size();
        client_ << nfields;
        for (size_t i = 0; i < nfields; i++) {
            ExtractionResult res(items[i]->values(), items[i]->mask());
            client_ << res;
        }
    }

    LOG_DEBUG_LIB(LibGribJump) << "Sent " << nRequests << " results to client" << std::endl;

    metrics_.timeReply = timer.elapsed();
}

//----------------------------------------------------------------------------------------------------------------------

AxesRequest::AxesRequest(eckit::Stream& stream) : Request(stream) {
    client_ >> request_;
}

AxesRequest::~AxesRequest() {
}

void AxesRequest::execute() {
    // @todo, use the engine.
    // or, polytope should use pyfdb not gribjump for this.
    GribJump gj;
    axes_ = gj.axes(request_);
}

void AxesRequest::replyToClient() {

    // @todo, reporting of axes errors, i.e. implement an AxesTask.
    reportErrors();

    // print the axes we are sending
    for (auto& pair : axes_) {
        eckit::Log::info() << pair.first << ": ";
        for (auto& val : pair.second) {
            eckit::Log::info() << val << ", ";
        }
        eckit::Log::info() << std::endl;
    }

    size_t naxes = axes_.size();
    client_ << naxes;
    for (auto& pair : axes_) {
        client_ << pair.first;
        size_t n = pair.second.size();
        client_ << n;
        for (auto& val : pair.second) {
            client_ << val;
        }
    }

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
