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
#include "gribjump/info/InfoFactory.h"

namespace gribjump {

SimpleInfo::SimpleInfo(eckit::DataHandle& h, const metkit::grib::GribHandle& gh, const eckit::Offset startOffset) : JumpInfo(gh, startOffset) {}

SimpleInfo::SimpleInfo(const eckit::message::Message& msg) : JumpInfo(msg) {}

SimpleInfo::SimpleInfo(eckit::Stream& s) : JumpInfo(s) {}

void SimpleInfo::encode(eckit::Stream& s) const {
    JumpInfo::encode(s);
}

void SimpleInfo::print(std::ostream& s) const {
    s << "SimpleInfo,";
    JumpInfo::print(s);
}

// -----------------------------------------------------------------------------

eckit::ClassSpec SimpleInfo::classSpec_ = {&JumpInfo::classSpec(), "SimpleInfo",};
eckit::Reanimator<SimpleInfo> SimpleInfo::reanimator_;

static InfoBuilder<SimpleInfo> simpleInfoBuilder("grid_simple");

// -----------------------------------------------------------------------------

} // namespace gribjump
