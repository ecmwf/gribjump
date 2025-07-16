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

#include "eckit/net/NetService.h"
#include "eckit/thread/ThreadControler.h"
#include "gribjump/LogRouter.h"
#include "gribjump/remote/GribJumpService.h"
#include "gribjump/remote/WorkQueue.h"

namespace gribjump {

//-------------------------------------------------------------------------------------------------
// Maybe we don't need this class at all.  We can just use GribJumpService directly.

class GribJumpServer : private eckit::NonCopyable {
public:

    GribJumpServer(int port) : svc_(new GribJumpService(port)), tcsvc_(svc_) {
        eckit::Log::info() << "Starting GribJumpServer on port " << port << std::endl;

        // By default, route timing and progress logs to eckit::info
        // This can be overriden in the config file.
        LogRouter::instance().setDefaultChannel("info");

        WorkQueue::instance();  // start the work queue
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

}  // namespace gribjump
