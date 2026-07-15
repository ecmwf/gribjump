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
/// @author Tiago Quintino

#pragma once

#include <unordered_set>

#include "eckit/filesystem/URI.h"

#include "gribjump/Capabilities.h"
#include "gribjump/Config.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/Stats.h"
#include "gribjump/Types.h"
#include "gribjump/api/ListRequest.h"
#include "gribjump/api/ListResult.h"

namespace fdb5 {
class Key;
class FieldLocation;
}  // namespace fdb5

namespace gribjump {

///@todo: Why is this *here*? and not in Engine
using ResultsMap = std::map<std::string, std::unique_ptr<ExtractionItem>>;

// Convenience base implementing every capability at once, so a single concrete backend
// (e.g. LocalGribJump, RemoteGribJump) can serve several roles and share resources.
// The GribJump facade composes the roles individually, so backends may still be mixed.
class GribJumpBase : public Scanner, public Extractor, public AxesProvider, public Lister {
public:

    GribJumpBase();

    GribJumpBase(const GribJumpBase&)            = delete;
    GribJumpBase& operator=(const GribJumpBase&) = delete;
    GribJumpBase(GribJumpBase&&)                 = delete;
    GribJumpBase& operator=(GribJumpBase&&)      = delete;

    virtual ~GribJumpBase();

    virtual void stats();

protected:  // members

    Stats stats_;
};

}  // namespace gribjump
