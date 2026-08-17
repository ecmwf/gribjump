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

/// Server-side handler for a client request.
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
    /// select v3 buffered vs. v4 streaming reply framing.
    ProtocolVersion protocolVersion_;

    /// Emit the leading error block. A phase of process(); exposed here so a
    /// subclass override can still chain to the default behaviour.
    virtual void reportErrors();

private:  // lifecycle phases (run by process())

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

/// Strategy encapsulating one protocol version's EXTRACT reply framing (v3
/// buffered vs. v4 streaming). ExtractHandler picks one at construction and
/// delegates to it.
class ExtractReplyStrategy;

class ExtractHandler : public RequestHandler {
public:

    ExtractHandler(eckit::Stream& stream, EngineIface& engine, ProtocolVersion version);

    ~ExtractHandler() override;

private:

    void receive() override;
    void execute() override;
    void replyToClient() override;

    /// v4 streaming replies carry errors in the END-chunk trailer, so the
    /// leading error block written by the base is suppressed.
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
