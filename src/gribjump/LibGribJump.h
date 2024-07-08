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
/// @date   Dec 2023

#pragma once

#include <string>
#include <vector>

#include "eckit/system/Library.h"
#include "eckit/system/Plugin.h"

#include "gribjump/Config.h"


namespace fdb5 {
    class FDB;
}

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class LibGribJump : public eckit::system::Plugin {
// class LibGribJump : public eckit::system::Library {
public:
    LibGribJump();

    static LibGribJump& instance();

    const Config& config() const;

    void setup(fdb5::FDB& fdb) const;

protected:
    virtual std::string version() const override;

    virtual std::string gitsha1(unsigned int count) const override;

private:

    Config config_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
