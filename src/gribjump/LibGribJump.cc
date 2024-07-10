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

#include "gribjump/LibGribJump.h"

#include "eckit/eckit_version.h"
#include "eckit/config/LibEcKit.h"
#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include "gribjump/gribjump_version.h"
#include "gribjump/gribjump_config.h"

#ifdef GRIBJUMP_HAVE_FDB
#include "gribjump/FDBPlugin.h"
#endif

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibGribJump);

// LibGribJump::LibGribJump() : Plugin("gribjump", "gribjump-plugin") {
LibGribJump::LibGribJump() : Plugin("gribjump") {
    if(getenv("GRIBJUMP_CONFIG_FILE") != nullptr){
        config_ = Config(getenv("GRIBJUMP_CONFIG_FILE"));
    } 
    else {
        eckit::Log::debug() << "GRIBJUMP_CONFIG_FILE not set, using default config" << std::endl;
    }
}

LibGribJump& LibGribJump::instance() {
    static LibGribJump lib;
    return lib;
}

const Config& LibGribJump::config() const {
    return config_;
}

std::string LibGribJump::version() const {
    return gribjump_version_str();
}

std::string LibGribJump::gitsha1(unsigned int count) const {
    std::string sha1(gribjump_git_sha1());
    if (sha1.empty()) {
        return "not available";
    }

    return sha1.substr(0, std::min(count, 40u));
}


#ifdef GRIBJUMP_HAVE_FDB

void LibGribJump::setup(void* fdb) {
    fdb5::FDB* fdb_ptr = static_cast<fdb5::FDB*>(fdb);
    FDBPlugin::instance(*fdb_ptr);
}

#else
void LibGribJump::setup(void* fdb) {
    std::stringstream ss;
    ss << "GribJump has been compiled without FDB support." << std::endl;
    throw eckit::UserError(ss.str(), Here());
}
#endif

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
