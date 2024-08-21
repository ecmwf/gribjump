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
#include "gribjump/ExtractionItem.h"

namespace gribjump {

typedef std::pair<size_t, size_t> Interval;
typedef std::vector<double> Values;
typedef std::vector<bool> Bitmap;

// todo: avoid rebuilding jumpers: one per thread per type
class Jumper {
public:
    Jumper();
    virtual ~Jumper() = 0;

    void extract(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info, ExtractionItem& extractionItem);
    virtual std::vector<mc::Range> byteRanges(const eckit::Offset offset, const JumpInfo& info_in, const std::vector<Interval>& intervals) {NOTIMP;}

private:

    virtual void readValues(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info, const std::vector<Interval>& intervals, ExtractionItem& item) {NOTIMP;}
    Bitmap readBitmap(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info) const;

    void extractConstant(const JumpInfo& info, ExtractionItem& item);
    void extractNoMask(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info, ExtractionItem&);
    void extractMasked(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info, ExtractionItem&);

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
