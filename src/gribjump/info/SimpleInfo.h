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

#include <iosfwd>
#include <string>

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

class SimpleInfo : public JumpInfo {

public:

    SimpleInfo(eckit::DataHandle& handle, const metkit::codes::CodesHandle& h, const eckit::Offset startOffset);
    SimpleInfo(const eckit::message::Message& msg);
    SimpleInfo(eckit::Stream& s);

    void print(std::ostream&) const override;

    void encode(eckit::Stream&) const override;

    virtual std::string className() const override { return "SimpleInfo"; }
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<SimpleInfo> reanimator_;
};

}  // namespace gribjump
