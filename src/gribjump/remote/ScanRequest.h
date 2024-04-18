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

#include <cstddef>

#include "eckit/filesystem/PathName.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/remote/Request.h"

namespace gribjump {
    
//----------------------------------------------------------------------------------------------------------------------

class ScanTask : public Task {
public: 
    
    ScanTask(size_t id, Request* clientRequest);

    virtual ~ScanTask();
    
    size_t result() { return result_; }

protected:
    size_t result_ = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanMARSTask : public ScanTask {
public: 
    
    ScanMARSTask(size_t id, Request* clientRequest, const metkit::mars::MarsRequest& request);
    
    void execute(GribJump& gj) override;

private:
    metkit::mars::MarsRequest request_;
};

//----------------------------------------------------------------------------------------------------------------------

class ScanFileTask : public ScanTask {
public: 
    
    ScanFileTask(size_t id, Request* clientRequest, const eckit::PathName& path);
    
    void execute(GribJump& gj) override;

private:
    eckit::PathName file_;
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

};

} // namespace gribjump
