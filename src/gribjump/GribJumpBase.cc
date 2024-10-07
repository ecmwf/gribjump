/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino

#include "gribjump/GribJumpBase.h"
#include "gribjump/LibGribJump.h"
#include "GribJumpBase.h"

namespace gribjump {

GribJumpBase::GribJumpBase(const Config &config) {
}

GribJumpBase::GribJumpBase() {
}

GribJumpBase::~GribJumpBase() {
}

void GribJumpBase::stats()
{
    stats_.report(eckit::Log::debug<LibGribJump>(), "Extraction stats: ");
}

} // namespace gribjump
