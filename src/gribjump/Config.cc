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

#include "gribjump/Config.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"

namespace gribjump
{   
    Config::Config(){
        // default config
        set("type", "local");
    }
    Config::Config(const eckit::PathName path):eckit::LocalConfiguration(eckit::YAMLConfiguration(path)) {}

} // namespace gribjump
