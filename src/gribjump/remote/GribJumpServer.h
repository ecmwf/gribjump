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

#include "eckit/io/cluster/ClusterNode.h"
#include "eckit/net/NetService.h"
#include "eckit/thread/ThreadControler.h"
#include "gribjump/remote/GribJumpService.h"

namespace gribjump {

//-------------------------------------------------------------------------------------------------
// Maybe we don't need this class at all.  We can just use GribJumpService directly.

class GribJumpServer : private eckit::NonCopyable {
public:
    GribJumpServer(int port): svc_(new GribJumpService(port)), tcsvc_(svc_) {
        std::cout << "Starting GribJumpServer on port " << port << std::endl;
        tcsvc_.start();
    }
    ~GribJumpServer() {}

private:  // methods
    int port() const { return svc_->port(); }

private:  // members
    eckit::net::NetService* svc_;
    eckit::ThreadControler tcsvc_;
};

//-------------------------------------------------------------------------------------------------

}  // namespace dhskit
