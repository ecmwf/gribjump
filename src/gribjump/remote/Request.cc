/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley
/// @author Tiago Quintino

#include "gribjump/remote/Request.h"
#include <cstddef>
#include "gribjump/Engine.h"
#include "gribjump/remote/ProtocolCodec.h"

namespace {
static std::atomic<uint64_t> requestid_{0};
static uint64_t requestid() {
    return requestid_++;
}
}  // namespace

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// @todo: Lots of common behaviour between these classes, consider refactoring. Especially the interaction with metrics.

Request::Request(eckit::Stream& stream, EngineIface& engine) : client_(stream), engine_(engine) {
    id_ = requestid();
    MetricsManager::instance().set("gribjump_request_id", id_);
}

void Request::reportErrors() {
    report_.reportErrors(client_);
}

//----------------------------------------------------------------------------------------------------------------------

ScanRequest::ScanRequest(eckit::Stream& stream, EngineIface& engine) : Request(stream, engine) {
    MetricsManager::instance().set("action", "scan");

    requests_ = ProtocolCodec::decodeScanRequest(client_, byfiles_);

    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: byfiles=" << byfiles_ << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "ScanRequest: numRequests=" << requests_.size() << std::endl;

    MetricsManager::instance().set("count_scan_requests", requests_.size());
}

void ScanRequest::execute() {
    auto [nfields, report] = engine_.scan(requests_, byfiles_);
    nFields_               = nfields;
    report_                = std::move(report);
}

void ScanRequest::replyToClient() {
    ProtocolCodec::encodeScanReply(client_, nFields_);
}

void ScanRequest::info() const {
    eckit::Log::status() << "New ScanRequest: nRequests=" << requests_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------


ExtractRequest::ExtractRequest(eckit::Stream& stream, EngineIface& engine) : Request(stream, engine) {
    MetricsManager::instance().set("action", "extract");

    requests_ = ProtocolCodec::decodeExtractRequest(client_);

    MetricsManager::instance().set("count_extraction_requests", requests_.size());
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

    size_t nRequests = requests_.size();
    LOG_DEBUG_LIB(LibGribJump) << "Sending " << nRequests << " results to client" << std::endl;

    // Assemble the results in request order; encodeExtractReply frames each.
    std::vector<std::unique_ptr<ExtractionResult>> ordered;
    std::vector<const ExtractionResult*> results;
    ordered.reserve(nRequests);
    results.reserve(nRequests);
    for (size_t i = 0; i < nRequests; i++) {
        auto it = results_.find(requests_[i].requestString());
        ASSERT(it != results_.end());
        ordered.push_back(it->second->result());
        results.push_back(ordered.back().get());
    }

    ProtocolCodec::encodeExtractReply(client_, results);

    LOG_DEBUG_LIB(LibGribJump) << "Sent " << nRequests << " results to client" << std::endl;
}

void ExtractRequest::info() const {
    eckit::Log::status() << "New ExtractRequest: nRequests=" << requests_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedExtractRequest::ForwardedExtractRequest(eckit::Stream& stream, EngineIface& engine) : Request(stream, engine) {
    MetricsManager::instance().set("action", "forwarded-extract");

    auto data = ProtocolCodec::decodeForwardExtractRequest(client_);
    items_    = std::move(data.items);
    filemap_  = std::move(data.filemap);

    size_t count = 0;
    for (const auto& [fname, extractionItems] : filemap_) {
        count += extractionItems.size();
    }
    LOG_DEBUG_LIB(LibGribJump) << "ForwardedExtractRequest: nFiles=" << filemap_.size() << std::endl;
    MetricsManager::instance().set("count_extraction_requests", count);

    ASSERT(count > 0);  // We should not be talking to this server if we have no requests.
}

void ForwardedExtractRequest::execute() {
    report_ = engine_.scheduleExtractionTasks(filemap_);
}

void ForwardedExtractRequest::replyToClient() {
    ProtocolCodec::encodeForwardExtractReply(client_, filemap_);
}

void ForwardedExtractRequest::info() const {
    eckit::Log::status() << "New ForwardedExtractRequest: nItems=" << items_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedScanRequest::ForwardedScanRequest(eckit::Stream& stream, EngineIface& engine) : Request(stream, engine) {
    MetricsManager::instance().set("action", "forwarded-scan");

    scanmap_ = ProtocolCodec::decodeForwardScanRequest(client_);
    LOG_DEBUG_LIB(LibGribJump) << "ForwardedScanRequest: nFiles=" << scanmap_.size() << std::endl;

    size_t count = 0;
    for (const auto& [fname, offsets] : scanmap_) {
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
    ProtocolCodec::encodeScanReply(client_, nfields_);
}

void ForwardedScanRequest::info() const {
    eckit::Log::status() << "New ForwardedScanRequest: nfiles=" << scanmap_.size() << std::endl;
}
//----------------------------------------------------------------------------------------------------------------------

AxesRequest::AxesRequest(eckit::Stream& stream, EngineIface& engine) : Request(stream, engine) {
    MetricsManager::instance().set("action", "axes");
    ProtocolCodec::decodeAxesRequest(client_, request_, level_);
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

    ProtocolCodec::encodeAxesReply(client_, axes_);
}

void AxesRequest::info() const {
    eckit::Log::status() << "New AxesRequest: " << request_ << ", level=" << level_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
