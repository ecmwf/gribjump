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

#pragma once

#include <functional>
#include <map>
#include <string>
#include "eckit/config/Configuration.h"
#include "eckit/log/Channel.h"

namespace gribjump {

class LogRouter {
public:

    using ChannelGetter = std::function<eckit::Channel&()>;

    static LogRouter& instance();

    void configure(const eckit::Configuration& config);

    void set(const std::string& alias, const std::string& channel);
    void setDefaultChannel(const std::string& channel);

    eckit::Channel& get(const std::string& name);

private:

    LogRouter();

    ChannelGetter standardChannel(const std::string& name);

    ChannelGetter defaultChannel_;
    std::map<std::string, ChannelGetter> channels_;
};

}  // namespace gribjump
