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

#pragma once

#include "gribjump/compression/NumericCompressor.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/ExtractionData.h"


namespace gribjump {

typedef std::pair<size_t, size_t> Interval;
typedef std::vector<double> Values;
typedef std::vector<bool> Bitmap;

// todo: factory
// todo: avoid rebuilding jumpers: one per thread per type
class Jumper {
public:
    Jumper();
    virtual ~Jumper() = 0;

    ExtractionResult* extract(eckit::DataHandle& dh, const NewJumpInfo& info, const std::vector<Interval>& intervals);

private:

    virtual std::vector<Values> readValues(eckit::DataHandle& dh, const NewJumpInfo& info, const std::vector<Interval>& intervals) {NOTIMP;}
    Bitmap readBitmap(eckit::DataHandle& dh, const NewJumpInfo& info) const;

    ExtractionResult* extractConstant(const NewJumpInfo& info, const std::vector<Interval>& intervals);
    ExtractionResult* extractNoMask(eckit::DataHandle& dh, const NewJumpInfo& info, const std::vector<Interval>& intervals);
    ExtractionResult* extractMasked(eckit::DataHandle& dh, const NewJumpInfo& info, const std::vector<Interval>& intervals);

    std::pair<std::vector<Range>, std::vector<Bitmap>> calculateMaskedIntervals(const std::vector<Interval>& intervals, const Bitmap& bitmap) const;
};

// -----------------------------------------------------------------------------------------

class BadJumpInfoException : public eckit::Exception {
public:

    BadJumpInfoException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("BadJumpInfoException: ") + msg, here) {}

};

// -----------------------------------------------------------------------------------------
// To remove...:

// Convert ranges to intervals
// TODO(maee): Simplification: Switch to intervals or ranges
std::vector<mc::Range> toRanges(const std::vector<Interval>& intervals);

} // namespace gribjump
