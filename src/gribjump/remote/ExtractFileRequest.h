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

struct KeyOffset {
    flatkey_t key_;
    eckit::Offset offset_;
};

struct PerFileWork {
    eckit::PathName file_; // full path to file
    std::vector<KeyOffset> jumps_;
};

typedef std::vector<Range> ranges_t;
typedef std::map<flatkey_t, ExtractionResult*> map_results_t;
typedef std::map<flatkey_t, ranges_t> map_ranges_t;

//----------------------------------------------------------------------------------------------------------------------

class ExtractFileTask : public Task {
public: 
    
    ExtractFileTask(size_t id, ExtractFileRequest* clientRequest);

    virtual ~ExtractFileTask();

    map_results_t& results() { return partial_results_; }
    
protected:
    const ExtractFileRequest *const request_;
    map_results_t partial_results_;
};

//----------------------------------------------------------------------------------------------------------------------

class PerFileTask : public ExtractFileTask {
public: 
    
    PerFileTask(size_t id, ExtractFileRequest* clientRequest, PerFileWork* work);
    virtual ~PerFileTask() { delete work_; }
    
    void execute(GribJump& gj) override;

    PerFileWork* work() { return work_; }

private:
    PerFileWork* work_; // owned by this task
}; 

//----------------------------------------------------------------------------------------------------------------------

class ExtractFileRequest : public Request {
public:

    ExtractFileRequest(eckit::Stream& stream);

    ~ExtractFileRequest();

    const map_ranges_t& ranges() const { return ranges_; }

    void enqueueTasks() override;

    void enqueueTask(ExtractFileTask*);

    void replyToClient() override;

    ExtractionResult* results(const flatkey_t& k) {
        return results_[k];
    }    

private:
    std::vector<ExtractionRequest> received_requests_;
    std::vector<ExtractFileTask*> tasks_;
    map_results_t results_;
    map_ranges_t ranges_;

    bool flattenRequest_;
};

} // namespace gribjump
