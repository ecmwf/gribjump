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


#include <sstream>

#include "eckit/io/DataHandle.h"
#include "eckit/exception/Exceptions.h"

#include "metkit/codes/GribAccessor.h"

#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/CcsdsInfo.h"
#include "gribjump/info/SimpleInfo.h"
#include "gribjump/GribJumpException.h"

extern "C" {
    double grib_power(long s, long n); // Todo: Do we really need this?
}

namespace gribjump {

// --------------------------------------------------------------------------------------------

using metkit::grib::GribAccessor;
namespace grib {
static GribAccessor<long>          editionNumber("editionNumber");
static GribAccessor<long>          bitmapPresent("bitmapPresent");
static GribAccessor<long>          binaryScaleFactor("binaryScaleFactor");
static GribAccessor<long>          decimalScaleFactor("decimalScaleFactor");
static GribAccessor<unsigned long> bitsPerValue("bitsPerValue");
static GribAccessor<double>        referenceValue("referenceValue");
static GribAccessor<unsigned long> offsetBeforeData("offsetBeforeData");
static GribAccessor<unsigned long> offsetAfterData("offsetAfterData");
static GribAccessor<unsigned long> offsetBeforeBitmap("offsetBeforeBitmap");
static GribAccessor<unsigned long> numberOfValues("numberOfValues");
static GribAccessor<unsigned long> numberOfDataPoints("numberOfDataPoints");
static GribAccessor<long>          sphericalHarmonics("sphericalHarmonics", true);
static GribAccessor<unsigned long> totalLength("totalLength");
static GribAccessor<unsigned long> offsetBSection6("offsetBSection6");
static GribAccessor<std::string>   md5GridSection("md5GridSection");
static metkit::grib::GribAccessor<std::string>   packingType("packingType");

} // namespace grib
// --------------------------------------------------------------------------------------------

NewJumpInfo::NewJumpInfo(const metkit::grib::GribHandle& h, const eckit::Offset startOffset): 
    version_(currentVersion_), msgStartOffset_(startOffset) {

    editionNumber_      = grib::editionNumber(h);
    packingType_        = grib::packingType(h);
    if (editionNumber_ != 1 && editionNumber_ != 2) {
        std::stringstream ss;
        ss << "Unsupported GRIB edition number: " << editionNumber_;
        throw JumpException(ss.str(), Here());
    }
    
    binaryScaleFactor_  = grib::binaryScaleFactor(h);
    decimalScaleFactor_ = grib::decimalScaleFactor(h);
    bitsPerValue_       = grib::bitsPerValue(h);
    referenceValue_     = grib::referenceValue(h);
    offsetBeforeData_   = grib::offsetBeforeData(h);
    offsetAfterData_    = grib::offsetAfterData(h);
    numberOfDataPoints_ = grib::numberOfDataPoints(h);
    numberOfValues_     = grib::numberOfValues(h);
    sphericalHarmonics_ = grib::sphericalHarmonics(h);
    totalLength_        = grib::totalLength(h);
    md5GridSection_     = grib::md5GridSection(h);

    long bitmapPresent_ = grib::bitmapPresent(h);
    
    if (bitmapPresent_) {
        constexpr size_t offsetToBitmap = 6;
        offsetBeforeBitmap_ = editionNumber_ == 1? grib::offsetBeforeBitmap(h) : grib::offsetBSection6(h) + offsetToBitmap;
    }
    else {
        offsetBeforeBitmap_ = 0;
    }

    binaryMultiplier_ = grib_power(binaryScaleFactor_, 2);
    decimalMultiplier_ = grib_power(-decimalScaleFactor_, 10);
}


NewJumpInfo::NewJumpInfo(eckit::Stream& s) : Streamable(s) {
    s >> version_;
    s >> referenceValue_;
    s >> binaryScaleFactor_;
    s >> decimalScaleFactor_;
    s >> editionNumber_;
    s >> bitsPerValue_;
    s >> offsetBeforeData_;
    s >> offsetAfterData_;
    s >> offsetBeforeBitmap_;
    s >> numberOfValues_;
    s >> numberOfDataPoints_;
    s >> totalLength_;
    s >> msgStartOffset_;
    s >> sphericalHarmonics_;
    s >> md5GridSection_;
    s >> binaryMultiplier_;
    s >> decimalMultiplier_;
}

void NewJumpInfo::encode(eckit::Stream& s) const {
    Streamable::encode(s);
    s << version_;
    s << referenceValue_;
    s << binaryScaleFactor_;
    s << decimalScaleFactor_;
    s << editionNumber_;
    s << bitsPerValue_;
    s << offsetBeforeData_;
    s << offsetAfterData_;
    s << offsetBeforeBitmap_;
    s << numberOfValues_;
    s << numberOfDataPoints_;
    s << totalLength_;
    s << msgStartOffset_;
    s << sphericalHarmonics_;
    s << md5GridSection_;
    s << binaryMultiplier_;
    s << decimalMultiplier_;
}

std::string NewJumpInfo::toString() const {
    std::stringstream ss;
    print(ss);
    return ss.str();
}

void NewJumpInfo::print(std::ostream& s) const {
    s << "version=" << static_cast<int>(version_) << ","
      << "referenceValue=" << referenceValue_ << ","
      << "binaryScaleFactor=" << binaryScaleFactor_ << ","
      << "decimalScaleFactor=" << decimalScaleFactor_ << ","
      << "editionNumber=" << editionNumber_ << ","
      << "bitsPerValue=" << bitsPerValue_ << ","
      << "offsetBeforeData=" << offsetBeforeData_ << ","
      << "offsetAfterData=" << offsetAfterData_ << ","
      << "offsetBeforeBitmap=" << offsetBeforeBitmap_ << ","
      << "numberOfValues=" << numberOfValues_ << ","
      << "numberOfDataPoints=" << numberOfDataPoints_ << ","
      << "totalLength=" << totalLength_ << ","
      << "msgStartOffset=" << msgStartOffset_ << ","
      << "sphericalHarmonics=" << sphericalHarmonics_ << ","
      << "md5GridSection=" << md5GridSection_ << ","
      << "binaryMultiplier=" << binaryMultiplier_ << ","
      << "decimalMultiplier=" << decimalMultiplier_;
}

bool NewJumpInfo::equals(const NewJumpInfo& rhs) const {
    return version_ == rhs.version() &&
           referenceValue() == rhs.referenceValue() &&
           binaryScaleFactor() == rhs.binaryScaleFactor() &&
           decimalScaleFactor() == rhs.decimalScaleFactor() &&
           editionNumber() == rhs.editionNumber() &&
           bitsPerValue() == rhs.bitsPerValue() &&
           offsetBeforeData() == rhs.offsetBeforeData() &&
           offsetAfterData() == rhs.offsetAfterData() &&
           offsetBeforeBitmap() == rhs.offsetBeforeBitmap() &&
           numberOfValues() == rhs.numberOfValues() &&
           numberOfDataPoints() == rhs.numberOfDataPoints() &&
           totalLength() == rhs.totalLength() &&
           msgStartOffset() == rhs.msgStartOffset() &&
           sphericalHarmonics() == rhs.sphericalHarmonics() &&
           md5GridSection() == rhs.md5GridSection() &&
           binaryMultiplier() == rhs.binaryMultiplier() &&
           decimalMultiplier() == rhs.decimalMultiplier();
}

// --------------------------------------------------------------------------------------------

} // namespace gribjump
