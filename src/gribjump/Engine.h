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

#include "metkit/mars/MarsRequest.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/remote/ExtractFileRequest.h" // todo move to not remote

namespace gribjump {

typedef std::vector<metkit::mars::MarsRequest> MarsRequests;
typedef std::pair<size_t, size_t> Range;
typedef std::vector<std::vector<Range>> RangesList;

// typedef std::vector<ExtractionResult> Results;
typedef std::map<metkit::mars::MarsRequest, std::vector<ExtractionItem*>> Results;


class Engine {
public:
    
    Engine();
    ~Engine();

    Results extract(const MarsRequests& requests, const RangesList& ranges, bool flattenRequests = false);

private:
    // std::vector<Task*> tasks_; // Want one vector per user request, not one per engine
};


} // namespace gribjump
