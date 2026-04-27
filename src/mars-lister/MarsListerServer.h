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
#include "eckit/thread/ThreadControler.h"

#include "MarsListerService.h"

namespace marslister {

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

}  // namespace marslister
