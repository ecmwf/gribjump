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

#include "gribjump/compression/compressors/Simple.h"
#include "gribjump/GribJumpDataAccessor.h"
#include "gribjump/info/SimpleInfo.h"
#include "gribjump/jumper/SimpleJumper.h"
#include "gribjump/jumper/JumperFactory.h"

namespace gribjump {

SimpleJumper::SimpleJumper(): Jumper() {}

SimpleJumper::~SimpleJumper() {}

void SimpleJumper::readValues(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info_in, const std::vector<Interval>& intervals, ExtractionItem& item) {

    const SimpleInfo* psimple = dynamic_cast<const SimpleInfo*>(&info_in);

    if (!psimple) throw BadJumpInfoException("SimpleJumper::readValues: info is not of type SimpleInfo", Here());

    const SimpleInfo& info = *psimple;


    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(dh, mc::Block{offset + info.offsetBeforeData(), info.offsetAfterData() - info.offsetBeforeData()}); // TODO XXX
    mc::SimpleDecompressor<double> simple{};
    simple
        .bits_per_value(info.bitsPerValue())
        .reference_value(info.referenceValue())
        .binary_scale_factor(info.binaryScaleFactor())
        .decimal_scale_factor(info.decimalScaleFactor());

    // TODO(maee): Optimize this
    auto ranges = toRanges(intervals);

    simple.decode(data_accessor, ranges, item.values());

    return;
}

static JumperBuilder<SimpleJumper> simpleJumperBuilder("grid_simple");

} // namespace gribjump