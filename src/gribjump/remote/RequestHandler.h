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

#pragma once

#include <condition_variable>
#include <mutex>
#include "eckit/serialisation/Stream.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/GribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/remote/Protocol.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

/// Server-side handler for requests
class RequestHandler {
public:

    RequestHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

    virtual ~RequestHandler() = default;

    void process();

protected:  // members

    eckit::Stream& client_;
    EngineIface& engine_;
    TaskReport report_;
    uint64_t id_;

    /// The negotiated protocol version for this connection. Used by EXTRACT to
    /// select between buffered vs. streaming.
    ProtocolVersion protocolVersion_;

    /// Emit errors.
    virtual void reportErrors();

private:

    /// Decode the request from the client stream.
    virtual void receive() = 0;

    /// Execute the request against the engine.
    virtual void execute() = 0;

    /// Write the reply to the client.
    virtual void replyToClient() = 0;

    /// Log a one-line status summary of the request, for monitoring.
    virtual void info() const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanHandler : public RequestHandler {
public:

    ScanHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;
    void info() const override;

    std::vector<metkit::mars::MarsRequest> requests_;
    bool byfiles_;

    size_t nFields_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Abstraction for buffered vs streamed replies.
class ExtractReplyStrategy;

class ExtractHandler : public RequestHandler {
public:

    ExtractHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

    ~ExtractHandler() override;

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;

    void reportErrors() override;

    void info() const override;

    std::vector<ExtractionRequest> requests_;

    std::unique_ptr<ExtractReplyStrategy> replyStrategy_;
};

//----------------------------------------------------------------------------------------------------------------------

class ForwardedExtractHandler : public RequestHandler {
public:

    ForwardedExtractHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;
    void info() const override;

    std::vector<std::unique_ptr<ExtractionItem>> items_;
    filemap_t filemap_;

    ResultsMap results_;
};


//----------------------------------------------------------------------------------------------------------------------

class ForwardedScanHandler : public RequestHandler {
public:

    ForwardedScanHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;
    void info() const override;

    std::vector<std::unique_ptr<ExtractionItem>> items_;
    scanmap_t scanmap_;
    ResultsMap results_;

    size_t nfields_;
};

//----------------------------------------------------------------------------------------------------------------------

class AxesHandler : public RequestHandler {
public:

    AxesHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;
    void info() const override;

    std::string request_;  /// @todo why is this a string?
    int level_;
    std::map<std::string, std::unordered_set<std::string>> axes_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
