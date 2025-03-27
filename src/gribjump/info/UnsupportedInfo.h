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

class UnsupportedInfo : public JumpInfo {

public:

    UnsupportedInfo(eckit::DataHandle& handle, const metkit::grib::GribHandle& h, const eckit::Offset startOffset);
    UnsupportedInfo(const eckit::message::Message& msg);
    UnsupportedInfo(eckit::Stream& s);

    void print(std::ostream&) const override;

    void encode(eckit::Stream&) const override;

    virtual std::string className() const override { return "UnsupportedInfo"; }
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<UnsupportedInfo> reanimator_;
};

}  // namespace gribjump
