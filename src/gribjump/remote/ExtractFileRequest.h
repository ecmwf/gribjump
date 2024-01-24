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

class ExtractFileRequest;

//----------------------------------------------------------------------------------------------------------------------

typedef std::string flatkey_t;

struct WorkPerField2 {
    flatkey_t key_;
    eckit::Offset field_offset_;
};

struct WorkPerFile2 {
    eckit::PathName file_; // full path to file
    std::vector<WorkPerField2> fields_;
};

struct RangesResults {
    std::vector<Range> ranges_;
    ExtractionResult* result_;
};

typedef std::map<flatkey_t, RangesResults> map_results_t;

//----------------------------------------------------------------------------------------------------------------------

class ExtractFileTask : public Task {
public: 
    
    ExtractFileTask(size_t id, ExtractFileRequest* clientRequest);

    virtual ~ExtractFileTask();

    map_results_t& results() { return partial_results_; }
    
protected:
    map_results_t partial_results_;
};

//----------------------------------------------------------------------------------------------------------------------

class PerFileTask : public ExtractFileTask {
public: 
    
    PerFileTask(size_t id, ExtractFileRequest* clientRequest, WorkPerFile2* work);
    virtual ~PerFileTask() { delete work_; }
    
    void execute(GribJump& gj) override;

    WorkPerFile2* work() { return work_; }

private:
    WorkPerFile2* work_; // owned by this task
}; 

//----------------------------------------------------------------------------------------------------------------------

class ExtractFileRequest : public Request {
public:

    ExtractFileRequest(eckit::Stream& stream);

    ~ExtractFileRequest();

    void enqueueTasks() override; 

    void replyToClient() override;

private:
    std::vector<ExtractFileTask*> tasks_;
    map_results_t results_;
};

} // namespace gribjump
