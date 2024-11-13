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

#include <mutex>
#include <condition_variable>
#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Metrics.h"
#include "gribjump/remote/WorkQueue.h"
#include "gribjump/Engine.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class Request {
public: // methods

    Request(eckit::Stream& stream);

    virtual ~Request();
    
    // Have engine execute the request
    virtual void execute() = 0;

    /// Reply to the client with the results of the request
    virtual void replyToClient() = 0;

    void reportErrors();

protected: // members

    eckit::Stream& client_;
    Engine engine_; //< Engine and schedule tasks based on request

    uint64_t id_;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanRequest : public Request {
public:

    ScanRequest(eckit::Stream& stream);

    ~ScanRequest();

    void execute() override;

    void replyToClient() override;

private:

    std::vector<metkit::mars::MarsRequest> requests_;
    bool byfiles_;

    size_t nfiles_;

};

//----------------------------------------------------------------------------------------------------------------------

class ExtractRequest : public Request {
public:

    ExtractRequest(eckit::Stream& stream);

    ~ExtractRequest();

    void execute() override;

    void replyToClient() override;

private:
    std::vector<ExtractionRequest> requests_;

    ResultsMap results_;

};

//----------------------------------------------------------------------------------------------------------------------

class ForwardedExtractRequest : public Request {
public:

    ForwardedExtractRequest(eckit::Stream& stream);

    ~ForwardedExtractRequest();

    void execute() override;

    void replyToClient() override;

private:

    std::vector<std::unique_ptr<ExtractionItem>> items_;
    filemap_t filemap_;

    ResultsMap results_;
};

//----------------------------------------------------------------------------------------------------------------------

class AxesRequest : public Request {
public:

    AxesRequest(eckit::Stream& stream);

    ~AxesRequest();

    void execute() override;

    void replyToClient() override;

private:

    std::string request_; /// @todo why is this a string?
    int level_;
    std::map<std::string, std::unordered_set<std::string>> axes_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump
