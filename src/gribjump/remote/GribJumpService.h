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

#pragma once

#include "eckit/net/NetService.h"
#include "gribjump/remote/GribJumpUser.h"

namespace gribjump {

//-------------------------------------------------------------------------------------------------

class GribJumpService : public eckit::net::NetService {

public:  // methods
    GribJumpService(int port) : NetService(port) {
        eckit::Log::info() << "GribJumpService::GribJumpService on port " << port << std::endl;
    };
    ~GribJumpService() {}

    // No copy allowed
    GribJumpService(const GribJumpService&) = delete;
    GribJumpService& operator=(const GribJumpService&) = delete;

private:  // methods
    eckit::net::NetUser* newUser(eckit::net::TCPSocket& protocol) const override {
        eckit::Log::info() << "GribJumpService::newUser" << std::endl;
        return new GribJumpUser(protocol);
    }
    std::string name() const override { return "gribjumpserver"; }
    // bool preferToRunAsProcess() const override { return true;} // TODO: Is this correct?
};

//-------------------------------------------------------------------------------------------------

} 