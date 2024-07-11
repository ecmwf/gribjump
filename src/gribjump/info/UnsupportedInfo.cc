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

#include "gribjump/info/UnsupportedInfo.h"
#include "gribjump/info/InfoFactory.h"

namespace gribjump {

UnsupportedInfo::UnsupportedInfo(eckit::DataHandle& h, const metkit::grib::GribHandle& gh, const eckit::Offset startOffset) : JumpInfo(gh, startOffset) {}

UnsupportedInfo::UnsupportedInfo(const eckit::message::Message& msg) : JumpInfo(msg) {}

UnsupportedInfo::UnsupportedInfo(eckit::Stream& s) : JumpInfo(s) {}

void UnsupportedInfo::encode(eckit::Stream& s) const {
    JumpInfo::encode(s);
}

void UnsupportedInfo::print(std::ostream& s) const {
    s << "UnsupportedInfo,";
    JumpInfo::print(s);
}

// -----------------------------------------------------------------------------

eckit::ClassSpec UnsupportedInfo::classSpec_ = {&JumpInfo::classSpec(), "UnsupportedInfo",};
eckit::Reanimator<UnsupportedInfo> UnsupportedInfo::reanimator_;

static InfoBuilder<UnsupportedInfo> UnsupportedInfoBuilder("unsupported");

// -----------------------------------------------------------------------------

} // namespace gribjump
