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

#include "gribjump/info/JumpInfo.h"

namespace gribjump {

class CcsdsInfo : public Info {

public:
    CcsdsInfo(eckit::DataHandle& h, const metkit::grib::GribHandle& gh);
    CcsdsInfo(eckit::Stream& s);

    virtual void encode(eckit::Stream&) const override;

    virtual void print(std::ostream&) const override;

    // From Streamable
    virtual std::string className() const override { return "CcsdsInfo"; }
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:

    virtual bool equals(const Info& other) const override;

private:

    unsigned long ccsdsFlags_;
    unsigned long ccsdsBlockSize_;
    unsigned long ccsdsRsi_;
    std::vector<size_t> ccsdsOffsets_;

private:

    // From Streamable
    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<CcsdsInfo> reanimator_;

};
    
} // namespace gribjump
