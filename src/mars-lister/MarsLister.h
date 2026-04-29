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

#include "eckit/log/Log.h"
#include "eckit/net/NetService.h"
#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/thread/ThreadControler.h"

namespace marslister {

constexpr static uint16_t protocolVersion = 1;

enum class RequestType : uint16_t {
    LIST = 0
};

//----------------------------------------------------------------------------------------------------------------------

class MarsListerUser : public eckit::net::NetUser {
public:

    MarsListerUser(eckit::net::TCPSocket& protocol);
    ~MarsListerUser();

private:

    void serve(eckit::Stream& s, std::istream& in, std::ostream& out) override;
    void handle(eckit::Stream& s);
    void handleList(eckit::Stream& s);
};

//----------------------------------------------------------------------------------------------------------------------

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

//----------------------------------------------------------------------------------------------------------------------

class MarsListerServer {
public:

    MarsListerServer(int port) : svc_(new MarsListerService(port)), tcsvc_(svc_) {
        eckit::Log::info() << "Starting MarsListerServer on port " << port << std::endl;
        tcsvc_.start();
    }

    MarsListerServer(const MarsListerServer&)            = delete;
    MarsListerServer& operator=(const MarsListerServer&) = delete;
    MarsListerServer(MarsListerServer&&)                 = delete;
    MarsListerServer& operator=(MarsListerServer&&)      = delete;

    ~MarsListerServer() {}

private:

    eckit::net::NetService* svc_;
    eckit::ThreadControler tcsvc_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace marslister
