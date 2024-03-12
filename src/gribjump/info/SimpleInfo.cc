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

#include "gribjump/info/SimpleInfo.h"
#include "gribjump/info/JumpInfoFactory.h"

namespace gribjump {

SimpleInfo::SimpleInfo(eckit::DataHandle& h, const metkit::grib::GribHandle& gh, const eckit::Offset startOffset) : NewJumpInfo(gh, startOffset) {}

SimpleInfo::SimpleInfo(eckit::Stream& s) : NewJumpInfo(s) {}

void SimpleInfo::encode(eckit::Stream& s) const {
    NewJumpInfo::encode(s);
}

void SimpleInfo::print(std::ostream& s) const {
    s << "SimpleInfo,";
    NewJumpInfo::print(s);
}

// -----------------------------------------------------------------------------

eckit::ClassSpec SimpleInfo::classSpec_ = {&NewJumpInfo::classSpec(), "SimpleInfo",};
eckit::Reanimator<SimpleInfo> SimpleInfo::reanimator_;

static InfoBuilder<SimpleInfo> simpleInfoBuilder("grid_simple");

// -----------------------------------------------------------------------------

} // namespace gribjump
