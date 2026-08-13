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

class Request {
public:

    Request(eckit::Stream& stream, EngineIface& engine);

    virtual ~Request() = default;

    // Have engine execute the request
    virtual void execute() = 0;

    /// Reply to the client with the results of the request
    virtual void replyToClient() = 0;

    virtual void reportErrors();

    /// The negotiated protocol version for this connection. Set by the dispatch
    /// before execute(); used by EXTRACT to select v3 buffered vs. v4 streaming
    /// reply framing.
    void protocolVersion(uint16_t version) { protocolVersion_ = version; }

    /// Print information about the request to status(), for monitoring
    virtual void info() const = 0;

protected:  // members

    eckit::Stream& client_;
    EngineIface& engine_;
    TaskReport report_;
    uint64_t id_;
    uint16_t protocolVersion_ = remoteProtocolVersion;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanRequest : public Request {
public:

    ScanRequest(eckit::Stream& stream, EngineIface& engine);

    ~ScanRequest() = default;

    void execute() override;

    void replyToClient() override;

    void info() const override;

private:

    std::vector<metkit::mars::MarsRequest> requests_;
    bool byfiles_;

    size_t nFields_;
};

//----------------------------------------------------------------------------------------------------------------------

class ExtractRequest : public Request {
public:

    ExtractRequest(eckit::Stream& stream, EngineIface& engine);

    ~ExtractRequest() = default;

    void execute() override;

    void replyToClient() override;

    /// v4 streaming replies carry errors in the END-chunk trailer, so the
    /// leading error block written by the base is suppressed.
    void reportErrors() override;

    void info() const override;

private:

    bool streaming() const { return protocolVersion_ >= streamingProtocolVersion; }

    std::vector<ExtractionRequest> requests_;

    ResultsMap results_;

    std::string streamError_;  //< set if the streaming pass threw mid-reply
};

//----------------------------------------------------------------------------------------------------------------------

class ForwardedExtractRequest : public Request {
public:

    ForwardedExtractRequest(eckit::Stream& stream, EngineIface& engine);

    ~ForwardedExtractRequest() = default;

    void execute() override;

    void replyToClient() override;

    void info() const override;

private:

    std::vector<std::unique_ptr<ExtractionItem>> items_;
    filemap_t filemap_;

    ResultsMap results_;
};


//----------------------------------------------------------------------------------------------------------------------

class ForwardedScanRequest : public Request {
public:

    ForwardedScanRequest(eckit::Stream& stream, EngineIface& engine);

    ~ForwardedScanRequest() = default;

    void execute() override;

    void replyToClient() override;

    void info() const override;

private:

    std::vector<std::unique_ptr<ExtractionItem>> items_;
    scanmap_t scanmap_;
    ResultsMap results_;

    size_t nfields_;
};

//----------------------------------------------------------------------------------------------------------------------

class AxesRequest : public Request {
public:

    AxesRequest(eckit::Stream& stream, EngineIface& engine);

    ~AxesRequest() = default;

    void execute() override;

    void replyToClient() override;

    void info() const override;

private:

    std::string request_;  /// @todo why is this a string?
    int level_;
    std::map<std::string, std::unordered_set<std::string>> axes_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
