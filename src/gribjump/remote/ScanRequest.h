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

#include "gribjump/remote/Request.h"

namespace gribjump {

class ScanRequest;
    
class ScanTask : public Task {
public: 
    
    ScanTask(size_t id, ExtractionRequest& request, ScanRequest* clientRequest);

    virtual ~ScanTask();
    
    size_t result() { return result_; }

    void execute(GribJump& gj) override;

    void notify() override;

    void notifyError(const std::string&) override;

private:
    size_t result_ = 0;
    ExtractionRequest request_;
    ScanRequest* clientRequest_;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanRequest : public Request {
public:

    ScanRequest(eckit::Stream& stream);

    ~ScanRequest();

    void enqueueTasks() override;

    void replyToClient() override;

private:
    std::vector<ScanTask*> tasks_;
    std::vector<size_t> requestGroups_;
};

} // namespace gribjump
