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

std::vector<Values> SimpleJumper::readValues(eckit::DataHandle& dh, const NewJumpInfo& info_in, const std::vector<Range>& intervals){

    const SimpleInfo* psimple = dynamic_cast<const SimpleInfo*>(&info_in);

    if (!psimple) throw BadJumpInfoException("SimpleJumper::readValues: info is not of type SimpleInfo", Here());

    const SimpleInfo& info = *psimple;


    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor2>(dh, mc::Range{info.msgStartOffset() + info.offsetBeforeData(), info.offsetAfterData() - info.offsetBeforeData()}); // TODO XXX
    std::cout << info.bitsPerValue() << std::endl;
    mc::SimpleDecompressor<double> simple{};
    simple
        .bits_per_value(info.bitsPerValue())
        .reference_value(info.referenceValue())
        .binary_scale_factor(info.binaryScaleFactor())
        .decimal_scale_factor(info.decimalScaleFactor());

    // TODO(maee): Optimize this
    auto ranges = toRanges(intervals);

    return simple.decode(data_accessor, ranges);
}

static JumperBuilder<SimpleJumper> simpleJumperBuilder("grid_simple");

} // namespace gribjump