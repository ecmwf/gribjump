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

#include <stddef.h>
#include <iosfwd>
#include <string>
#include <vector>

#include "eckit/io/Offset.h"
#include "eckit/serialisation/Reanimator.h"
#include "gribjump/info/JumpInfo.h"

namespace eckit {
class DataHandle;
}
namespace eckit {
class Stream;
}
namespace eckit::message {
class Message;
}
namespace metkit::codes {
class CodesHandle;
}

namespace gribjump {

class CcsdsInfo : public JumpInfo {

public:

    CcsdsInfo(eckit::DataHandle& handle, const metkit::codes::CodesHandle& h, const eckit::Offset startOffset);
    CcsdsInfo(const eckit::message::Message& msg);
    CcsdsInfo(eckit::Stream& s);

    virtual void encode(eckit::Stream&) const override;

    virtual void print(std::ostream&) const override;

    // From Streamable
    virtual std::string className() const override { return "CcsdsInfo"; }
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

    // getters
    unsigned long ccsdsFlags() const { return ccsdsFlags_; }
    unsigned long ccsdsBlockSize() const { return ccsdsBlockSize_; }
    unsigned long ccsdsRsi() const { return ccsdsRsi_; }
    const std::vector<size_t>& ccsdsOffsets() const { return ccsdsOffsets_; }

protected:

    virtual bool equals(const JumpInfo& other) const override;

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

}  // namespace gribjump
