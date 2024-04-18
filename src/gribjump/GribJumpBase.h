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

#include <unordered_set>

#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/URI.h"


#include "gribjump/ExtractionData.h"
#include "gribjump/Config.h"
#include "gribjump/Stats.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {

typedef std::pair<size_t, size_t> Range;
class GribJumpBase : public eckit::NonCopyable {
public:
    
    GribJumpBase(const Config& config);
    
    virtual ~GribJumpBase();

    size_t virtual scan(const eckit::PathName& path) = 0;
    virtual size_t scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) = 0;

    virtual std::vector<std::vector<ExtractionResult*>> extract(std::vector<ExtractionRequest>) = 0;
    virtual std::vector<ExtractionResult*> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) = 0;
    
    virtual std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) = 0;
    
    virtual void stats();

protected: // members

    Stats stats_;

};

} // namespace gribjump
