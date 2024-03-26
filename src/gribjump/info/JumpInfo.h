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

#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/types/FixedString.h"
#include "eckit/serialisation/Streamable.h"

#include "metkit/codes/GribHandle.h"

namespace gribjump
{
class JumpInfo : public eckit::Streamable { // TODO rename to JumpInfo when finished.

public:

    JumpInfo(const metkit::grib::GribHandle& h, const eckit::Offset startOffset);
    JumpInfo(eckit::Stream&); 

    virtual void encode(eckit::Stream&) const override;
    std::string toString() const;

    virtual void print(std::ostream&) const;

    virtual std::string className() const override = 0;
    virtual const eckit::ReanimatorBase& reanimator() const override = 0;

    // getters
    uint8_t version() const { return version_; }
    double referenceValue() const { return referenceValue_; }
    long binaryScaleFactor() const { return binaryScaleFactor_; }
    long decimalScaleFactor() const { return decimalScaleFactor_; }
    unsigned long editionNumber() const { return editionNumber_; }
    unsigned long bitsPerValue() const { return bitsPerValue_; }
    eckit::Offset offsetBeforeData() const { return offsetBeforeData_; }
    eckit::Offset offsetAfterData() const { return offsetAfterData_; }
    eckit::Offset offsetBeforeBitmap() const { return offsetBeforeBitmap_; }
    unsigned long numberOfValues() const { return numberOfValues_; }
    unsigned long numberOfDataPoints() const { return numberOfDataPoints_; }
    eckit::Length totalLength() const { return totalLength_; }
    eckit::Offset msgStartOffset() const { return msgStartOffset_; }
    long sphericalHarmonics() const { return sphericalHarmonics_; }
    std::string md5GridSection() const { return md5GridSection_; }
    std::string packingType() const { return packingType_; }
    double binaryMultiplier() const { return binaryMultiplier_; }
    double decimalMultiplier() const { return decimalMultiplier_; }

protected:

    virtual bool equals(const JumpInfo& other) const;

private:

    friend std::ostream& operator<<(std::ostream& s, const JumpInfo& f) {
        f.print(s);
        return s;
    }

    friend bool operator==(const JumpInfo& lhs, const JumpInfo& rhs) {
        return lhs.equals(rhs);
    }

protected:

    static constexpr uint8_t currentVersion_ = 4;
    uint8_t       version_;
    double        referenceValue_;
    long          binaryScaleFactor_;
    long          decimalScaleFactor_;
    unsigned long editionNumber_;
    unsigned long bitsPerValue_;
    eckit::Offset offsetBeforeData_;
    eckit::Offset offsetAfterData_;
    eckit::Offset offsetBeforeBitmap_;
    unsigned long numberOfValues_;
    unsigned long numberOfDataPoints_;
    eckit::Length totalLength_;
    eckit::Offset msgStartOffset_;
    long          sphericalHarmonics_;
    std::string   md5GridSection_;
    std::string   packingType_ = "none";

    double binaryMultiplier_; // = 2^binaryScaleFactor_     // todo: check if used
    double decimalMultiplier_; // = 10^-decimalScaleFactor_     // todo: check if used

};

} // namespace gribjump
