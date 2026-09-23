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

#pragma once

#include "eckit/serialisation/Stream.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Lister.h"
#include "gribjump/Metrics.h"
#include "gribjump/Task.h"
#include "gribjump/Types.h"
#include "gribjump/remote/ResultSink.h"
#include "metkit/mars/MarsRequest.h"

#include <functional>
#include <unordered_map>

namespace gribjump {

template <typename T>

struct TaskOutcome {
    T result;
    TaskReport report;
};

//----------------------------------------------------------------------------------------------------------------------
// Abstract interface exposing only the operations the remote Request classes
// need from the engine. This is used to inject a mock engine in remote tests.

class EngineIface {
public:

    virtual ~EngineIface() = default;

    virtual TaskOutcome<ResultsMap> extract(ExtractionRequests& requests) = 0;

    /// Streaming extraction: schedule the work and hand results to the sink in batches as tasks complete.
    virtual TaskReport extractStreaming(ExtractionRequests& requests, ResultSink& sink) = 0;

    /// Streaming extraction from a prebuilt filemap.
    /// Results are keyed by the shared filemap enumeration index (see
    /// ForwardExtractIndex.h) rather than a client request index.
    /// @todo: is filemap_t not already ordered byt *string* value? Why the need for another index?
    ///        unless its the order inside the vector of ExtractionItems, but aren't they always in the same batch?
    virtual TaskReport extractStreaming(filemap_t& filemap, ResultSink& sink) = 0;

    // byfiles: scan entire file, not just fields matching request
    virtual TaskOutcome<size_t> scan(const MarsRequests& requests, bool byfiles = false) = 0;

    virtual TaskOutcome<size_t> scheduleScanTasks(const scanmap_t& scanmap) = 0;

    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level = 3) = 0;

    virtual TaskReport scheduleExtractionTasks(filemap_t& filemap, bool forward = false) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class Engine : public EngineIface {
public:

    Engine();
    ~Engine();

    TaskOutcome<ResultsMap> extract(ExtractionRequests& requests) override;
    TaskOutcome<ResultsMap> extract(PathExtractionRequests& requests);

    TaskReport extractStreaming(ExtractionRequests& requests, ResultSink& sink) override;
    TaskReport extractStreaming(filemap_t& filemap, ResultSink& sink) override;

    // byfiles: scan entire file, not just fields matching request
    TaskOutcome<size_t> scan(const MarsRequests& requests, bool byfiles = false) override;
    TaskOutcome<size_t> scan(std::vector<eckit::PathName> files);
    TaskOutcome<size_t> scheduleScanTasks(const scanmap_t& scanmap) override;

    std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level = 3) override;

    TaskReport scheduleExtractionTasks(filemap_t& filemap, bool forward = false) override;

private:

    filemap_t buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem);
    filemap_t buildFileMapfromPaths(ExItemMap& keyToExtractionItem);
    void enqueueFileExtractionTasks(TaskGroup& taskGroup, filemap_t& filemap);
    TaskReport streamHarvest(TaskGroup& taskGroup, ResultSink& sink,
                             const std::function<size_t(ExtractionItem*)>& indexFor);
    void streamBufferedResults(ResultsMap& results, ResultSink& sink);
    ResultsMap collectResults(ExItemMap& keyToExtractionItem);
    metkit::mars::MarsRequest buildRequestMap(ExtractionRequests& requests, ExItemMap& keyToExtractionItem);
    void buildRequestURIsMap(PathExtractionRequests& requests, ExItemMap& keyToExtractionItem);

private:
};


}  // namespace gribjump
