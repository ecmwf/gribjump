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

class Engine {
public:

    Engine();
    ~Engine();

    TaskOutcome<ResultsMap> extract(ExtractionRequests& requests);

    // byfiles: scan entire file, not just fields matching request
    TaskOutcome<size_t> scan(const MarsRequests& requests, bool byfiles = false);
    TaskOutcome<size_t> scan(std::vector<eckit::PathName> files);
    TaskOutcome<size_t> scheduleScanTasks(const scanmap_t& scanmap);

    std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level = 3);

    TaskReport scheduleExtractionTasks(filemap_t& filemap);

private:

    filemap_t buildFileMap(const metkit::mars::MarsRequest& unionrequest, ExItemMap& keyToExtractionItem);
    ResultsMap collectResults(ExItemMap& keyToExtractionItem);
    metkit::mars::MarsRequest buildRequestMap(ExtractionRequests& requests, ExItemMap& keyToExtractionItem);

private:
};


}  // namespace gribjump
