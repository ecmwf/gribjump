/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
/// @date   Dec 2023

#pragma once

#include <string>
#include <vector>

#include "eckit/system/Library.h"

namespace gribjump {


//----------------------------------------------------------------------------------------------------------------------

class LibGribJump : public eckit::system::Library {
public:
    LibGribJump();

    static LibGribJump& instance();

protected:
    virtual std::string version() const;

    virtual std::string gitsha1(unsigned int count) const;

private:
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
