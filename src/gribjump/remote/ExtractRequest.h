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

#include "fdb5/api/FDB.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/remote/Request.h"

namespace gribjump {

class ExtractRequest;

//----------------------------------------------------------------------------------------------------------------------

class ExtractTask : public Task {
public: 
    
    ExtractTask(size_t id, ExtractRequest* clientRequest);

    virtual ~ExtractTask();

    const std::vector<ExtractionResult>& results() { return results_; }
    
protected:
    std::vector<ExtractionResult> results_;
};

//----------------------------------------------------------------------------------------------------------------------

struct WorkPerField {
    size_t client_request_id_;    // id in client request
    size_t field_id_;             // field id in that client request
    eckit::Offset field_offset_;
    std::vector<Range> ranges_;
    ExtractionResult result_;
};

struct WorkPerFile {
    eckit::PathName file_; // full path to file
    std::vector<WorkPerField> fields_;
};

class ExtractPerFileTask : public ExtractTask {
public: 
    
    ExtractPerFileTask(size_t id, ExtractRequest* clientRequest, WorkPerFile* work);
    
    void execute(GribJump& gj) override;

    WorkPerFile* work() { return work_; }

private:
    WorkPerFile* work_;
};

//----------------------------------------------------------------------------------------------------------------------

class ExtractMARSTask : public ExtractTask {
public: 
    
    ExtractMARSTask(size_t id, ExtractRequest* clientRequest, ExtractionRequest& request);
    
    void execute(GribJump& gj) override;

private:
    ExtractionRequest request_;
};

//----------------------------------------------------------------------------------------------------------------------

class ExtractFDBLocTask : public ExtractTask {
public: 
    
    ExtractFDBLocTask(size_t id, ExtractRequest* clientRequest, std::vector<eckit::URI> fields, std::vector<Range> ranges);
    
    void execute(GribJump& gj) override;

private:
    std::vector<eckit::URI> fields_;
    std::vector<Range> ranges_;
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
    std::vector<size_t> nb_fields_in_client_request_;
};

} // namespace gribjump
