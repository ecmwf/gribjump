/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "gribjump/LogRouter.h"
#include "gribjump/LibGribJump.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include <algorithm>

namespace gribjump {

LogRouter& LogRouter::instance() {
    static LogRouter instance;
    return instance;
}

LogRouter::LogRouter() {
    setDefaultChannel("debug");
}

LogRouter::ChannelGetter LogRouter::standardChannel(const std::string& name) {
    static const std::map<std::string, ChannelGetter> channelMap = {
        {"debug", []() -> eckit::Channel& { return eckit::Log::debug<LibGribJump>(); }},
        {"info", []() -> eckit::Channel& { return eckit::Log::info(); }},
        {"error", []() -> eckit::Channel& { return eckit::Log::error(); }},
        {"default", [this]() -> eckit::Channel& { return defaultChannel_(); }},
    };

    auto it = channelMap.find(name);
    if (it != channelMap.end()) {
        return it->second;
    }
    else {
        throw eckit::UserError("Unknown channel name: " + name, Here());
    }
}

void LogRouter::configure(const eckit::Configuration& config) {
    if (config.has("logging")) {
        eckit::LocalConfiguration loggingConfig = config.getSubConfiguration("logging");
        std::vector<std::string> keys           = loggingConfig.keys();
        for (const auto& key : keys) {
            std::string lowerValue = loggingConfig.getString(key);
            std::transform(lowerValue.begin(), lowerValue.end(), lowerValue.begin(), ::tolower);
            set(key, lowerValue);
        }
    }
}

void LogRouter::set(const std::string& alias, const std::string& channel) {
    channels_[alias] = standardChannel(channel);
}

void LogRouter::setDefaultChannel(const std::string& channel) {
    ASSERT(channel != "default");
    defaultChannel_ = standardChannel(channel);
}

eckit::Channel& LogRouter::get(const std::string& name) {
    auto it = channels_.find(name);
    if (it != channels_.end()) {
        return it->second();
    }
    return defaultChannel_();
}

}  // namespace gribjump
