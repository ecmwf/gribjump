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
#include "metkit/mars/MarsRequest.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Task.h"

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
    
    // byfiles: scan entire file, not just fields matching request
    size_t scan(const MarsRequests& requests, bool byfiles = false);

    std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request);

    void reportErrors(eckit::Stream& client_);

private:

    TaskGroup taskGroup_;

};


} // namespace gribjump
