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
#include <cstddef>
#include "gribjump/Engine.h"

namespace {
static std::atomic<uint64_t> requestid_{0};
static uint64_t requestid() {
    return requestid_++;
}
}  // namespace

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// @todo: Lots of common behaviour between these classes, consider refactoring. Especially the interaction with metrics.

Request::Request(eckit::Stream& stream) : client_(stream) {
    id_ = requestid();
    MetricsManager::instance().set("request_id", id_);
}

void Request::reportErrors() {
    report_.reportErrors(client_);
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

void ScanRequest::execute() {
    auto [nfields, report] = engine_.scan(requests_, byfiles_);
    nFields_               = nfields;
    report_                = std::move(report);
}

void ScanRequest::replyToClient() {
    client_ << nFields_;
}

void ScanRequest::info() const {
    eckit::Log::status() << "New ScanRequest: nRequests=" << requests_.size() << std::endl;
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

    MetricsManager::instance().set("count_requests", nRequests);
}

void ExtractRequest::execute() {

    auto [results, report] = engine_.extract(requests_);
    results_               = std::move(results);
    report_                = std::move(report);

    if (LibGribJump::instance().debug()) {
        for (auto& pair : results_) {
            LOG_DEBUG_LIB(LibGribJump) << pair.first << ": ";
            pair.second->debug_print();
            LOG_DEBUG_LIB(LibGribJump) << std::endl;
        }
    }
}

void ExtractRequest::replyToClient() {

    // Send the results, again repackage.

    size_t nRequests = requests_.size();
    LOG_DEBUG_LIB(LibGribJump) << "Sending " << nRequests << " results to client" << std::endl;

    for (size_t i = 0; i < nRequests; i++) {
        LOG_DEBUG_LIB(LibGribJump) << "Sending result " << i << " to client" << std::endl;

        auto it = results_.find(requests_[i].requestString());
        ASSERT(it != results_.end());
        size_t nfields = 1;  // @todo: remove this (bump protocol version)
        client_ << nfields;
        client_ << *(it->second->result());
    }

    LOG_DEBUG_LIB(LibGribJump) << "Sent " << nRequests << " results to client" << std::endl;
}

void ExtractRequest::info() const {
    eckit::Log::status() << "New ExtractRequest: nRequests=" << requests_.size() << std::endl;
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
        filemap_[fname] = std::vector<ExtractionItem*>();  // non-owning pointers
        filemap_[fname].reserve(nItems);

        for (size_t j = 0; j < nItems; j++) {
            auto extractionItem = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(client_));
            extractionItem->URI(eckit::URI("file", client_));
            filemap_[fname].push_back(extractionItem.get());  // non-owning pointers
            items_.push_back(std::move(extractionItem));
        }
        count += nItems;
    }
    MetricsManager::instance().set("count_requests", count);

    ASSERT(count > 0);  // We should not be talking to this server if we have no requests.
}

void ForwardedExtractRequest::execute() {
    report_ = engine_.scheduleExtractionTasks(filemap_);
}

void ForwardedExtractRequest::replyToClient() {

    for (auto& [fname, extractionItems] : filemap_) {
        client_ << fname;  // sanity check
        size_t nItems = extractionItems.size();
        client_ << nItems;
        for (auto& item : extractionItems) {
            client_ << *(item->result());
        }
    }
}

void ForwardedExtractRequest::info() const {
    eckit::Log::status() << "New ForwardedExtractRequest: nItems=" << items_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedScanRequest::ForwardedScanRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "forwarded-scan");

    size_t nFiles;
    client_ >> nFiles;
    LOG_DEBUG_LIB(LibGribJump) << "ForwardedScanRequest: nFiles=" << nFiles << std::endl;

    size_t count = 0;
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        eckit::OffsetList offsets;
        client_ >> fname;
        client_ >> offsets;
        scanmap_[fname] = offsets;
        count += offsets.size();
    }

    MetricsManager::instance().set("count_received_offsets", count);
}

void ForwardedScanRequest::execute() {
    auto [nfields, report] = engine_.scheduleScanTasks(scanmap_);
    nfields_               = nfields;
    report_                = std::move(report);
}

void ForwardedScanRequest::replyToClient() {
    client_ << nfields_;
}

void ForwardedScanRequest::info() const {
    eckit::Log::status() << "New ForwardedScanRequest: nfiles=" << scanmap_.size() << std::endl;
}
//----------------------------------------------------------------------------------------------------------------------

AxesRequest::AxesRequest(eckit::Stream& stream) : Request(stream) {
    MetricsManager::instance().set("action", "axes");
    client_ >> request_;
    client_ >> level_;
    ASSERT(request_.size() > 0);
}

void AxesRequest::execute() {
    axes_ = engine_.axes(request_, level_);
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

void AxesRequest::info() const {
    eckit::Log::status() << "New AxesRequest: " << request_ << ", level=" << level_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
