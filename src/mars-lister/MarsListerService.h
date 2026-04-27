/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/net/NetService.h"
#include "MarsListerUser.h"

namespace marslister {

class MarsListerService : public eckit::net::NetService {

public:

    MarsListerService(int port) : NetService(port) {}
    ~MarsListerService() {}

    MarsListerService(const MarsListerService&)            = delete;
    MarsListerService& operator=(const MarsListerService&) = delete;

private:

    eckit::net::NetUser* newUser(eckit::net::TCPSocket& protocol) const override { return new MarsListerUser(protocol); }
    std::string name() const override { return "marslister"; }
};

}  // namespace marslister
