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

#include "eckit/filesystem/URI.h"
#include "eckit/memory/NonCopyable.h"

#include "gribjump/Config.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/Stats.h"
#include "gribjump/Types.h"

namespace fdb5 {
class Key;
class FieldLocation;
}  // namespace fdb5

namespace gribjump {

///@todo: Why is this *here*? and not in Engine
using ResultsMap = std::map<std::string, std::unique_ptr<ExtractionItem>>;

class GribJumpBase : public eckit::NonCopyable {
public:

    GribJumpBase(const Config& config);
    GribJumpBase();

    virtual ~GribJumpBase();

    size_t virtual scan(const std::vector<eckit::PathName>& paths) = 0;

    virtual size_t scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) = 0;

    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<ExtractionRequest>&) = 0;

    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(const eckit::PathName& path,
                                                                   const std::vector<eckit::Offset>& offsets,
                                                                   const std::vector<std::vector<Range>>& ranges) = 0;

    virtual std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) = 0;

    virtual void stats();

protected:  // members

    Stats stats_;
};

}  // namespace gribjump
