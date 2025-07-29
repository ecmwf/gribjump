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

#pragma once

#include <condition_variable>
#include <mutex>
#include "eckit/serialisation/Stream.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/GribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class Request {
public:

    Request(eckit::Stream& stream);

    virtual ~Request() = default;

    // Have engine execute the request
    virtual void execute() = 0;

    /// Reply to the client with the results of the request
    virtual void replyToClient() = 0;

    void reportErrors();

    /// Print information about the request to status(), for monitoring
    virtual void info() const = 0;

protected:  // members

    eckit::Stream& client_;
    Engine engine_;
    TaskReport report_;
    uint64_t id_;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanRequest : public Request {
public:

    ScanRequest(eckit::Stream& stream);

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

    ExtractRequest(eckit::Stream& stream);

    ~ExtractRequest() = default;

    void execute() override;

    void replyToClient() override;

    void info() const override;

private:

    std::vector<ExtractionRequest> requests_;

    ResultsMap results_;
};

//----------------------------------------------------------------------------------------------------------------------

class ForwardedExtractRequest : public Request {
public:

    ForwardedExtractRequest(eckit::Stream& stream);

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

    ForwardedScanRequest(eckit::Stream& stream);

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

    AxesRequest(eckit::Stream& stream);

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
