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

// --------------------------------------------------------------------------------------------

Info::Info(const metkit::grib::GribHandle& h){

    editionNumber_      = editionNumber(h);

    if (editionNumber_ != 1 && editionNumber_ != 2) {
        std::stringstream ss;
        ss << "Unsupported GRIB edition number: " << editionNumber_;
        throw JumpException(ss.str(), Here());
    }
    
    binaryScaleFactor_  = binaryScaleFactor(h);
    decimalScaleFactor_ = decimalScaleFactor(h);
    bitsPerValue_       = bitsPerValue(h);
    referenceValue_     = referenceValue(h);
    offsetBeforeData_   = offsetBeforeData(h);
    offsetAfterData_    = offsetAfterData(h);
    numberOfDataPoints_ = numberOfDataPoints(h);
    numberOfValues_     = numberOfValues(h);
    sphericalHarmonics_ = sphericalHarmonics(h);
    totalLength_        = totalLength(h);
    md5GridSection_     = md5GridSection(h);

    long bitmapPresent_ = bitmapPresent(h);
    
    if (bitmapPresent_) {
        constexpr size_t offsetToBitmap = 6;
        offsetBeforeBitmap_ = editionNumber_ == 1? offsetBeforeBitmap(h) : offsetBSection6(h) + offsetToBitmap;
    }
    else {
        offsetBeforeBitmap_ = 0;
    }

    binaryMultiplier_ = grib_power(binaryScaleFactor_, 2);
    decimalMultiplier_ = grib_power(-decimalScaleFactor_, 10);
}


Info::Info(eckit::Stream& s) : Streamable(s) {
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

void Info::encode(eckit::Stream& s) const {
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

std::string Info::toString() const {
    std::stringstream ss;
    print(ss);
    return ss.str();
}

void Info::print(std::ostream& s) const {
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

bool Info::equals(const Info& rhs) const {
    return version_ == rhs.version_ &&
           referenceValue_ == rhs.referenceValue_ &&
           binaryScaleFactor_ == rhs.binaryScaleFactor_ &&
           decimalScaleFactor_ == rhs.decimalScaleFactor_ &&
           editionNumber_ == rhs.editionNumber_ &&
           bitsPerValue_ == rhs.bitsPerValue_ &&
           offsetBeforeData_ == rhs.offsetBeforeData_ &&
           offsetAfterData_ == rhs.offsetAfterData_ &&
           offsetBeforeBitmap_ == rhs.offsetBeforeBitmap_ &&
           numberOfValues_ == rhs.numberOfValues_ &&
           numberOfDataPoints_ == rhs.numberOfDataPoints_ &&
           totalLength_ == rhs.totalLength_ &&
           msgStartOffset_ == rhs.msgStartOffset_ &&
           sphericalHarmonics_ == rhs.sphericalHarmonics_ &&
           md5GridSection_ == rhs.md5GridSection_ &&
           binaryMultiplier_ == rhs.binaryMultiplier_ &&
           decimalMultiplier_ == rhs.decimalMultiplier_;
}

// --------------------------------------------------------------------------------------------

} // namespace gribjump
