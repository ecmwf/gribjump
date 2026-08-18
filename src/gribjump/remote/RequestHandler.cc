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

#include "gribjump/remote/RequestHandler.h"
#include <cstddef>
#include "eckit/log/Timer.h"
#include "gribjump/Engine.h"
#include "gribjump/remote/Protocol.h"

namespace {
static std::atomic<uint64_t> requestid_{0};
static uint64_t requestid() {
    return requestid_++;
}
}  // namespace

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// @todo: Lots of common behaviour between these classes, consider refactoring. Especially the interaction with metrics.

RequestHandler::RequestHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    client_(stream), engine_(engine), protocolVersion_(version) {
    id_ = requestid();
    MetricsManager::instance().set("gribjump_request_id", id_);
}

void RequestHandler::process() {
    eckit::Timer timer("GribJumpUser::processRequest");

    receive();
    MetricsManager::instance().set("elapsed_receive", timer.elapsed());
    timer.reset("Request received");

    info();

    execute();
    MetricsManager::instance().set("elapsed_execute", timer.elapsed());
    timer.reset("Request executed");

    reportErrors();
    replyToClient();
    MetricsManager::instance().set("elapsed_reply", timer.elapsed());
    timer.reset("Request replied");
}

void RequestHandler::reportErrors() {
    report_.reportErrors(client_);
}

//----------------------------------------------------------------------------------------------------------------------

ScanHandler::ScanHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    RequestHandler(stream, engine, version) {}

void ScanHandler::receive() {
    MetricsManager::instance().set("action", "scan");

    requests_ = Protocol::decodeScanRequest(client_, byfiles_);

    LOG_DEBUG_LIB(LibGribJump) << "ScanHandler: byfiles=" << byfiles_ << std::endl;
    LOG_DEBUG_LIB(LibGribJump) << "ScanHandler: numRequests=" << requests_.size() << std::endl;

    MetricsManager::instance().set("count_scan_requests", requests_.size());
}

void ScanHandler::execute() {
    auto [nfields, report] = engine_.scan(requests_, byfiles_);
    nFields_               = nfields;
    report_                = std::move(report);
}

void ScanHandler::replyToClient() {
    Protocol::encodeScanReply(client_, nFields_);
}

void ScanHandler::info() const {
    eckit::Log::status() << "New ScanHandler: nRequests=" << requests_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------
// EXTRACT reply strategies: one implementation per protocol version, selected
// at construction so ExtractHandler itself carries no version branching.

class ExtractReplyStrategy {
public:

    virtual ~ExtractReplyStrategy() = default;

    /// Run the extraction. v4 streams results to the client here; v3 buffers
    /// them for replyToClient(). Fills @p report with the task report.
    virtual void execute(eckit::Stream& client, EngineIface& engine, std::vector<ExtractionRequest>& requests,
                         TaskReport& report) = 0;

    /// Whether the base class should emit the leading error block. v3 does; v4
    /// folds errors into its END-chunk footer instead.
    virtual bool emitsLeadingErrorBlock() const = 0;

    /// Finalise the reply on the wire.
    virtual void reply(eckit::Stream& client, std::vector<ExtractionRequest>& requests, TaskReport& report) = 0;
};

namespace {

/// v3: buffer the full reply, then send one in-order block (leading errors +
/// results).
class BufferedExtractReply : public ExtractReplyStrategy {
public:

    void execute(eckit::Stream& /*client*/, EngineIface& engine, std::vector<ExtractionRequest>& requests,
                 TaskReport& report) override {
        auto [results, rep] = engine.extract(requests);
        results_            = std::move(results);
        report              = std::move(rep);

        if (LibGribJump::instance().debug()) {
            for (auto& pair : results_) {
                LOG_DEBUG_LIB(LibGribJump) << pair.first << ": ";
                pair.second->debug_print();
                LOG_DEBUG_LIB(LibGribJump) << std::endl;
            }
        }
    }

    bool emitsLeadingErrorBlock() const override { return true; }

    void reply(eckit::Stream& client, std::vector<ExtractionRequest>& requests, TaskReport& report) override {
        size_t nRequests = requests.size();
        LOG_DEBUG_LIB(LibGribJump) << "Sending " << nRequests << " results to client" << std::endl;

        // Assemble the results in request order; encodeExtractReply frames each.
        std::vector<std::unique_ptr<ExtractionResult>> ordered;
        std::vector<const ExtractionResult*> results;
        ordered.reserve(nRequests);
        results.reserve(nRequests);
        for (size_t i = 0; i < nRequests; i++) {
            auto it = results_.find(requests[i].requestString());
            ASSERT(it != results_.end());

            // *Move* result into ordered vector.
            ordered.push_back(it->second->result());
            results.push_back(ordered.back().get());
        }

        Protocol::encodeExtractReply(client, results);

        LOG_DEBUG_LIB(LibGribJump) << "Sent " << nRequests << " results to client" << std::endl;
    }

private:

    ResultsMap results_;
};

/// v4: stream results to the client as tasks complete, then terminate with the
/// END chunk followed by the error footer.
class StreamingExtractReply : public ExtractReplyStrategy {
public:

    void execute(eckit::Stream& client, EngineIface& engine, std::vector<ExtractionRequest>& requests,
                 TaskReport& report) override {
        // Any exception is captured so reply() can still emit the END chunk +
        // error footer -- chunks already on the wire cannot be unsent.
        StreamResultSink sink(client);
        try {
            report = engine.extractStreaming(requests, sink);
        }
        catch (std::exception& e) {
            streamError_ = e.what();
        }
    }

    bool emitsLeadingErrorBlock() const override { return false; }

    void reply(eckit::Stream& client, std::vector<ExtractionRequest>& requests, TaskReport& report) override {
        std::vector<std::string> errors = report.errors();
        if (!streamError_.empty()) {
            errors.push_back(streamError_);
        }
        Protocol::encodeExtractReplyEnd(client, errors);
    }

private:

    std::string streamError_;  //< set if the streaming pass threw mid-reply
};

std::unique_ptr<ExtractReplyStrategy> makeExtractReplyStrategy(ProtocolVersion version) {
    if (version.streaming()) {
        return std::make_unique<StreamingExtractReply>();
    }
    return std::make_unique<BufferedExtractReply>();
}

}  // namespace

ExtractHandler::ExtractHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    RequestHandler(stream, engine, version), replyStrategy_(makeExtractReplyStrategy(version)) {}

ExtractHandler::~ExtractHandler() = default;

void ExtractHandler::receive() {
    MetricsManager::instance().set("action", "extract");

    requests_ = Protocol::decodeExtractRequest(client_);

    MetricsManager::instance().set("count_extraction_requests", requests_.size());
}

void ExtractHandler::execute() {
    replyStrategy_->execute(client_, engine_, requests_, report_);
}

void ExtractHandler::reportErrors() {
    if (replyStrategy_->emitsLeadingErrorBlock()) {
        RequestHandler::reportErrors();
    }
}

void ExtractHandler::replyToClient() {
    replyStrategy_->reply(client_, requests_, report_);
}

void ExtractHandler::info() const {
    eckit::Log::status() << "New ExtractHandler: nRequests=" << requests_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedExtractHandler::ForwardedExtractHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    RequestHandler(stream, engine, version) {}

void ForwardedExtractHandler::receive() {
    MetricsManager::instance().set("action", "forwarded-extract");

    auto data = Protocol::decodeForwardExtractRequest(client_);
    items_    = std::move(data.items);
    filemap_  = std::move(data.filemap);

    size_t count = 0;
    for (const auto& [fname, extractionItems] : filemap_) {
        count += extractionItems.size();
    }
    LOG_DEBUG_LIB(LibGribJump) << "ForwardedExtractHandler: nFiles=" << filemap_.size() << std::endl;
    MetricsManager::instance().set("count_extraction_requests", count);

    ASSERT(count > 0);  // We should not be talking to this server if we have no requests.
}

void ForwardedExtractHandler::execute() {
    report_ = engine_.scheduleExtractionTasks(filemap_);
}

void ForwardedExtractHandler::replyToClient() {
    Protocol::encodeForwardExtractReply(client_, filemap_);
}

void ForwardedExtractHandler::info() const {
    eckit::Log::status() << "New ForwardedExtractHandler: nItems=" << items_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

ForwardedScanHandler::ForwardedScanHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    RequestHandler(stream, engine, version) {}

void ForwardedScanHandler::receive() {
    MetricsManager::instance().set("action", "forwarded-scan");

    scanmap_ = Protocol::decodeForwardScanRequest(client_);
    LOG_DEBUG_LIB(LibGribJump) << "ForwardedScanHandler: nFiles=" << scanmap_.size() << std::endl;

    size_t count = 0;
    for (const auto& [fname, offsets] : scanmap_) {
        count += offsets.size();
    }

    MetricsManager::instance().set("count_received_offsets", count);
}

void ForwardedScanHandler::execute() {
    auto [nfields, report] = engine_.scheduleScanTasks(scanmap_);
    nfields_               = nfields;
    report_                = std::move(report);
}

void ForwardedScanHandler::replyToClient() {
    Protocol::encodeScanReply(client_, nfields_);
}

void ForwardedScanHandler::info() const {
    eckit::Log::status() << "New ForwardedScanHandler: nfiles=" << scanmap_.size() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

AxesHandler::AxesHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version) :
    RequestHandler(stream, engine, version) {}

void AxesHandler::receive() {
    MetricsManager::instance().set("action", "axes");
    Protocol::decodeAxesRequest(client_, request_, level_);
    ASSERT(request_.size() > 0);
}

void AxesHandler::execute() {
    axes_ = engine_.axes(request_, level_);
}

void AxesHandler::replyToClient() {

    // print the axes we are sending
    for (auto& pair : axes_) {
        eckit::Log::info() << pair.first << ": ";
        for (auto& val : pair.second) {
            eckit::Log::info() << val << ", ";
        }
        eckit::Log::info() << std::endl;
    }

    Protocol::encodeAxesReply(client_, axes_);
}

void AxesHandler::info() const {
    eckit::Log::status() << "New AxesHandler: " << request_ << ", level=" << level_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
