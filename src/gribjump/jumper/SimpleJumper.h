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

#include <vector>
#include "eckit/io/Offset.h"
#include "gribjump/api/Types.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/jumper/Jumper.h"

namespace eckit {
class DataHandle;
}

namespace gribjump {

class SimpleJumper : public Jumper {
public:

    SimpleJumper();
    ~SimpleJumper();

private:

    virtual void readValues(eckit::DataHandle& dh, const eckit::Offset offset, const JumpInfo& info,
                            const std::vector<Interval>& intervals, ExValues& values) override;
};

}  // namespace gribjump
