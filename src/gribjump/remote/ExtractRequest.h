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

#include "gribjump/ExtractionData.h"
#include "gribjump/remote/Request.h"

namespace gribjump {

class ExtractRequest;

//----------------------------------------------------------------------------------------------------------------------

class ExtractTask : public Task {
public: 
    
    ExtractTask(ExtractionRequest& request, ExtractRequest* ExtractRequest);

    virtual ~ExtractTask();

    const std::vector<ExtractionResult>& results() { return results_; }
    
    void execute(GribJump& gj) override;

    void notify() override;

private:
    ExtractionRequest request_;
    std::vector<ExtractionResult> results_;
    ExtractRequest* clientRequest_;
};

//----------------------------------------------------------------------------------------------------------------------

class ExtractRequest : public Request {
public:

    ExtractRequest(eckit::Stream& stream);

    ~ExtractRequest();

    void enqueueTasks() override; 

    void replyToClient() override;

private:
    std::vector<ExtractTask*> tasks_;
    std::vector<size_t> requestGroups_;
};

} // namespace gribjump
