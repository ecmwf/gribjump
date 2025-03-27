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

#include "eckit/config/LibEcKit.h"
#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/eckit_version.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include "gribjump/gribjump_config.h"
#include "gribjump/gribjump_version.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

REGISTER_LIBRARY(LibGribJump);

LibGribJump::LibGribJump() : Plugin("gribjump-plugin", "gribjump") {}

Config LibGribJump::loadConfig() {

    if (getenv("GRIBJUMP_CONFIG_FILE") != nullptr) {
        LOG_DEBUG_LIB(LibGribJump) << "Config file set to: " << getenv("GRIBJUMP_CONFIG_FILE") << std::endl;
        return Config(getenv("GRIBJUMP_CONFIG_FILE"));
    }

    eckit::PathName defaultPath = eckit::PathName("~gribjump/etc/gribjump/config.yaml");
    if (defaultPath.exists()) {
        LOG_DEBUG_LIB(LibGribJump) << "Found config file: " << defaultPath << std::endl;
        return Config(defaultPath);
    }

    LOG_DEBUG_LIB(LibGribJump) << "No config file found, using default config" << std::endl;
    return Config();
}

LibGribJump& LibGribJump::instance() {
    static LibGribJump lib;
    return lib;
}

const Config& LibGribJump::config() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!configLoaded_) {
        config_       = loadConfig();
        configLoaded_ = true;
    }
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

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
