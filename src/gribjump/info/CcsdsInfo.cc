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

#include "eckit/io/DataHandle.h"

#include "metkit/codes/GribAccessor.h"

#include "gribjump/compression/compressors/Ccsds.h"
#include "gribjump/info/CcsdsInfo.h"
#include "gribjump/info/InfoFactory.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

using namespace metkit::grib;
namespace grib {
static GribAccessor<unsigned long> ccsdsFlags("ccsdsFlags", true);
static GribAccessor<unsigned long> ccsdsBlockSize("ccsdsBlockSize", true);
static GribAccessor<unsigned long> ccsdsRsi("ccsdsRsi", true);
} // namespace grib

//----------------------------------------------------------------------------------------------------------------------

CcsdsInfo::CcsdsInfo(eckit::DataHandle& handle, const metkit::grib::GribHandle& h, const eckit::Offset startOffset) : JumpInfo(h, startOffset) {
    
    ccsdsFlags_ = grib::ccsdsFlags(h);
    ccsdsBlockSize_ = grib::ccsdsBlockSize(h);
    ccsdsRsi_ = grib::ccsdsRsi(h);

    // Read data section to get offsets.
    handle.seek(startOffset + offsetBeforeData_);

    eckit::Length len = offsetAfterData_ - offsetBeforeData_;
    eckit::Buffer buffer(len);
    handle.read(buffer, len);

    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);
    ccsds.n_elems(numberOfValues_);
    ccsds.decode(buffer);
    
    ccsdsOffsets_ = ccsds.offsets().value();
}

CcsdsInfo::CcsdsInfo(const eckit::message::Message& msg) : JumpInfo(msg) {
    ccsdsFlags_ = msg.getLong("ccsdsFlags");
    ccsdsBlockSize_ = msg.getLong("ccsdsBlockSize");
    ccsdsRsi_ = msg.getLong("ccsdsRsi");

    eckit::Length len = offsetAfterData_ - offsetBeforeData_;
    eckit::Buffer buffer(len);
    std::unique_ptr<eckit::DataHandle> dh(msg.readHandle());
    dh->openForRead();
    dh->seek(offsetBeforeData_);
    dh->read(buffer, len);

    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);
    ccsds.n_elems(numberOfValues_);
    ccsds.decode(buffer);

    ccsdsOffsets_ = ccsds.offsets().value();
}

CcsdsInfo::CcsdsInfo(eckit::Stream& s) : JumpInfo(s) {
    s >> ccsdsFlags_;
    s >> ccsdsBlockSize_;
    s >> ccsdsRsi_;
    s >> ccsdsOffsets_;    
}

void CcsdsInfo::encode(eckit::Stream& s) const {
    JumpInfo::encode(s);
    s << ccsdsFlags_;
    s << ccsdsBlockSize_;
    s << ccsdsRsi_;
    s << ccsdsOffsets_;
}

void CcsdsInfo::print(std::ostream& s) const {
    s << "CcsdsInfo,";
    JumpInfo::print(s);
    s << ",ccsdsFlags=" << ccsdsFlags_ << ",";
    s << "ccsdsBlockSize=" << ccsdsBlockSize_ << ",";
    s << "ccsdsRsi=" << ccsdsRsi_ << ",";
    s << "ccsdsOffsets.size=" << ccsdsOffsets_.size();
}

bool CcsdsInfo::equals(const JumpInfo& other) const {
    
    if (!JumpInfo::equals(other)) return false;

    const auto& o = static_cast<const CcsdsInfo&>(other);
    return ccsdsFlags_ == o.ccsdsFlags_ &&
           ccsdsBlockSize_ == o.ccsdsBlockSize_ &&
           ccsdsRsi_ == o.ccsdsRsi_ &&
           ccsdsOffsets_ == o.ccsdsOffsets_;
}

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec CcsdsInfo::classSpec_ = {&JumpInfo::classSpec(), "CcsdsInfo",};
eckit::Reanimator<CcsdsInfo> CcsdsInfo::reanimator_;

static InfoBuilder<CcsdsInfo> ccsdsInfoBuilder("grid_ccsds");

//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump
