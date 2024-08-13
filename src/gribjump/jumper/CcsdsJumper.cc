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

#include "gribjump/compression/compressors/Ccsds.h"
#include "gribjump/GribJumpDataAccessor.h"
#include "gribjump/info/CcsdsInfo.h"
#include "gribjump/jumper/CcsdsJumper.h" 
#include "gribjump/jumper/JumperFactory.h"


namespace gribjump {

CcsdsJumper::CcsdsJumper(): Jumper() {}

CcsdsJumper::~CcsdsJumper() {}

void CcsdsJumper::readValues(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info_in, const std::vector<Interval>& intervals, ExtractionItem& item) {

    const CcsdsInfo* pccsds = dynamic_cast<const CcsdsInfo*>(&info_in);

    if (!pccsds) throw BadJumpInfoException("CcsdsJumper::readValues: info is not of type CcsdsInfo", Here());

    const CcsdsInfo& info = *pccsds;
    ASSERT(info.ccsdsOffsets().size() > 0);

    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(info.ccsdsFlags())
        .bits_per_sample(info.bitsPerValue())
        .block_size(info.ccsdsBlockSize())
        .rsi(info.ccsdsRsi())
        .reference_value(info.referenceValue())
        .binary_scale_factor(info.binaryScaleFactor())
        .decimal_scale_factor(info.decimalScaleFactor())
        .offsets(info.ccsdsOffsets());


    auto data_range = mc::Range{offset + info.offsetBeforeData(), info.offsetAfterData() - info.offsetBeforeData()};
    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(dh, data_range);

    // TODO(maee): Optimize this
    auto ranges = toRanges(intervals);

    ccsds.decode(data_accessor, ranges, item.values());

    return;
}

static JumperBuilder<CcsdsJumper> ccsdsJumperBuilder("grid_ccsds");

} // namespace gribjump