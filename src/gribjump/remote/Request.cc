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

namespace {
    static std::atomic<uint64_t> requestid_{0};
    static uint64_t requestid() {
        return requestid_++;
    }
} // namespace

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// @todo: Lots of common behaviour between these classes, consider refactoring. Especially the interaction with metrics.

Request::Request(eckit::Stream& stream) : client_(stream) {
    id_ = requestid();
    MetricsManager::instance().set("request_id", id_);
}

Request::~Request() {}

void Request::reportErrors() {
    engine_.reportErrors(client_);
}

//----------------------------------------------------------------------------------------------------------------------

ScanRequest::ScanRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "scan");

    client_ >> byfiles_;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: byfiles=" << byfiles_ << std::endl;

    size_t numRequests;
    client_ >> numRequests;

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: numRequests=" << numRequests << std::endl;

    for (size_t i = 0; i < numRequests; i++) {
        requests_.emplace_back(metkit::mars::MarsRequest(client_));
    }

    MetricsManager::instance().set("count_requests", numRequests);
}

ScanRequest::~ScanRequest() {
}

void ScanRequest::execute() {
    nfiles_ = engine_.scan(requests_, byfiles_);
}

void ScanRequest::replyToClient() {

    client_ << nfiles_;

}

//----------------------------------------------------------------------------------------------------------------------


ExtractRequest::ExtractRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "extract");

    // Receive the requests
    // Temp, repackage the requests from old format into format the engine expects

    size_t nRequests;
    client_ >> nRequests;

    for (size_t i = 0; i < nRequests; i++) {
        ExtractionRequest req(client_);
        requests_.push_back(req);
    }

    flatten_ = false; // xxx hard coded for now

    MetricsManager::instance().set("count_requests", nRequests);
}

ExtractRequest::~ExtractRequest() {
}

void ExtractRequest::execute() {

    results_ = engine_.extract(requests_, flatten_);

    if (LibGribJump::instance().debug()) {
        for (auto& pair : results_) {
            LOG_DEBUG_LIB(LibGribJump) << pair.first << ": ";
            for (auto& item : pair.second) {
                item->debug_print();
            }
        }
    }
}

void ExtractRequest::replyToClient() {

    // Send the results, again repackage.

    size_t nRequests = requests_.size();
    LOG_DEBUG_LIB(LibGribJump) << "Sending " << nRequests << " results to client" << std::endl;

    for (size_t i = 0; i < nRequests; i++) {
        LOG_DEBUG_LIB(LibGribJump) << "Sending result " << i << " to client" << std::endl;

        auto it = results_.find(requests_[i].request());
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
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedExtractRequest::ForwardedExtractRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "forwarded-extract");

    size_t nFiles;
    client_ >> nFiles;

    LOG_DEBUG_LIB(LibGribJump) << "ForwardedExtractRequest: nFiles=" << nFiles << std::endl;
    size_t count = 0;
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        size_t nItems;
        client_ >> fname;
        client_ >> nItems;
        filemap_[fname] = std::vector<ExtractionItem*>(); // non-owning pointers
        filemap_[fname].reserve(nItems);

        for (size_t j = 0; j < nItems; j++) {
            ExtractionRequest req(client_);
            eckit::URI uri("file", eckit::URI(client_));
            auto extractionItem = std::make_unique<ExtractionItem>(req.ranges());
            extractionItem->gridHash(req.gridHash()); // @todo, tidy this up.
            extractionItem->URI(uri);
            filemap_[fname].push_back(extractionItem.get());
            items_.push_back(std::move(extractionItem));
        }
        count += nItems;
    }
    MetricsManager::instance().set("count_requests", count);

    ASSERT(count > 0); // We should not be talking to this server if we have no requests.
}

ForwardedExtractRequest::~ForwardedExtractRequest() {
}

void ForwardedExtractRequest::execute() {
    engine_.scheduleTasks(filemap_);
}

void ForwardedExtractRequest::replyToClient() {

    for (auto& [fname, extractionItems] : filemap_) {
        client_ << fname; // sanity check
        size_t nItems = extractionItems.size();
        client_ << nItems;
        for (auto& item : extractionItems) {
            ExtractionResult res(item->values(), item->mask());
            client_ << res;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

AxesRequest::AxesRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "axes");
    client_ >> request_;
    ASSERT(request_.size() > 0);
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
