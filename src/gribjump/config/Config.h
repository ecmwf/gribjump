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

#include <unordered_map>
#include "eckit/config/LocalConfiguration.h"
#include "eckit/net/Endpoint.h"

namespace gribjump {

class Config : public eckit::LocalConfiguration {
public:  // types

    using ServerMap = std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint>;

public:

    Config();
    Config(const eckit::PathName);

    const ServerMap& serverMap() const { return serverMap_; }

    ///@note : Will be empty if default config is used
    const std::string& path() const { return path_; }

private:

    ServerMap loadServerMap() const;

private:

    ServerMap serverMap_;
    std::string path_;
};

}  // namespace gribjump