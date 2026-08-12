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
#include "metkit/mars/MarsRequest.h"

namespace gribjump {

template <typename T>

struct TaskOutcome {
    T result;
    TaskReport report;
};

//----------------------------------------------------------------------------------------------------------------------
// Abstract interface exposing only the operations the remote Request classes
// need from the engine. This is the seam used to inject a mock engine in tests,
// keeping the server-side protocol logic testable without FDB.

class EngineIface {
public:

    virtual ~EngineIface() = default;

    virtual TaskOutcome<ResultsMap> extract(ExtractionRequests& requests) = 0;

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

    // byfiles: scan entire file, not just fields matching request
    TaskOutcome<size_t> scan(const MarsRequests& requests, bool byfiles = false) override;
    TaskOutcome<size_t> scan(std::vector<eckit::PathName> files);
    TaskOutcome<size_t> scheduleScanTasks(const scanmap_t& scanmap) override;

    std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level = 3) override;

    TaskReport scheduleExtractionTasks(filemap_t& filemap, bool forward = false) override;

private:

    filemap_t buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem);
    filemap_t buildFileMapfromPaths(ExItemMap& keyToExtractionItem);
    ResultsMap collectResults(ExItemMap& keyToExtractionItem);
    metkit::mars::MarsRequest buildRequestMap(ExtractionRequests& requests, ExItemMap& keyToExtractionItem);
    void buildRequestURIsMap(PathExtractionRequests& requests, ExItemMap& keyToExtractionItem);

private:
};


}  // namespace gribjump
