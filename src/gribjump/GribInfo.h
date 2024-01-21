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
/// @author Tiago Quintino

#pragma once

#include <queue>

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/types/FixedString.h"

#include "metkit/codes/GribHandle.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/Bitmap.h"
#include "gribjump/Values.h"
#include "gribjump/Interval.h"

namespace gribjump {

class JumpHandle;

void accumulateIndexes(uint64_t &n, size_t &count, std::vector<size_t> &n_index, std::queue<size_t> &edges, bool&, size_t&);
std::vector<std::bitset<64>> to_bitset(const Bitmap& bitmap);

//----------------------------------------------------------------------------------------------------------------------

class JumpInfo {
public:

    static JumpInfo fromFile(const eckit::PathName& path, uint16_t msg_id = 0);

    JumpInfo();

    explicit JumpInfo(const metkit::grib::GribHandle& h);
    explicit JumpInfo(eckit::Stream& s);

    bool ready() const { return numberOfValues_ > 0; }
    void update(const metkit::grib::GribHandle& h);

    ExtractionResult extractRanges(const JumpHandle&, const std::vector<std::pair<size_t, size_t>>& ranges) const;

    void print(std::ostream&) const;
    void encode(eckit::Stream&) const;

    size_t streamSize() const;

    unsigned long getNumberOfDataPoints() const { return numberOfDataPoints_; }
    unsigned long length() const { return totalLength_; }
    void setStartOffset(eckit::Offset offset) { msgStartOffset_ = offset; }

    void updateCcsdsOffsets(const JumpHandle& f);
    std::vector<size_t> getCcsdsOffsets() const { return ccsdsOffsets_; }
    std::string getPackingType() const { return packingType_; }

private:

    static constexpr uint8_t currentVersion_ = 3;
    uint8_t version_;
    double        referenceValue_;
    long          binaryScaleFactor_;
    long          decimalScaleFactor_;
    unsigned long editionNumber_;
    unsigned long bitsPerValue_;
    unsigned long offsetBeforeData_;
    unsigned long offsetAfterData_;
    unsigned long bitmapPresent_;
    unsigned long offsetBeforeBitmap_;
    unsigned long numberOfValues_;
    unsigned long numberOfDataPoints_;
    unsigned long totalLength_;
    unsigned long msgStartOffset_;
    long          sphericalHarmonics_;
    eckit::FixedString<32> md5GridSection_;
    eckit::FixedString<64> packingType_;

    double binaryMultiplier_; // = 2^binaryScaleFactor_
    double decimalMultiplier_; // = 10^-decimalScaleFactor_

    unsigned long ccsdsFlags_;
    unsigned long ccsdsBlockSize_;
    unsigned long ccsdsRsi_;
    std::vector<size_t> ccsdsOffsets_;


    Bitmap get_bitmap(const JumpHandle& f) const;
    std::pair<std::vector<Interval>, std::vector<Bitmap>> calculate_intervals(const std::vector<Interval>&, const Bitmap&) const;

    std::vector<Values> get_ccsds_values(const JumpHandle& f, const std::vector<Interval> &intervals) const;
    std::vector<Values> get_simple_values(const JumpHandle& f, const std::vector<Interval> &intervals) const;

    friend std::ostream& operator<<(std::ostream& s, const JumpInfo& f) {
        f.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream&s, const JumpInfo& f) {
        f.encode(s);
        return s;
    }

}; 

//----------------------------------------------------------------------------------------------------------------------

class JumpInfoHandle {
public: // methods

    JumpInfoHandle() : info_(nullptr) {}

    explicit JumpInfoHandle(JumpInfo* info) : info_(info) {
        ASSERT(info_);
    }
    explicit JumpInfoHandle(eckit::Stream& s) : info_(new JumpInfo(s)) {}
    
    JumpInfo* get() { return info_; }

    JumpInfo* operator->() { return info_; }

private: // members

    JumpInfo* info_; //< not owned

private: // methods
    friend eckit::Stream& operator<<(eckit::Stream&s, const JumpInfoHandle& h) {
        h.info_->encode(s);
        return s;
    }
};

} // namespace gribjump

