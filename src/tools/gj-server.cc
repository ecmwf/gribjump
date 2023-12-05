/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#pragma once

#include "gribjump/remote/GribJumpServer.h"
#include "eckit/runtime/Application.h"

#include <unistd.h>
#include <fstream>

#include "eckit/net/Port.h"

namespace gribjump {


class GribJumpServerApp : public eckit::Application, public GribJumpServer {
public:
    GribJumpServerApp(int argc, char** argv) : Application(argc, argv), GribJumpServer(eckit::net::Port("gribjumpserver", 9001)){}
    ~GribJumpServerApp() {}

private:
    GribJumpServerApp(const GribJumpServerApp&);
    // GribJumpServerApp& operator=(const GribJumpServerApp&); // This is left undefined on marsfs?
    virtual void run() override {
        eckit::Log::info() << "pre unique" << std::endl;
        unique();
        eckit::Log::info() << "post unique" << std::endl;
        for (;;) {
            eckit::Log::status() << "Idle" << std::endl;
            ::sleep(10);
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump


int main(int argc, char** argv) {
    eckit::Log::info() << "Starting gribjump server" << std::endl;
    gribjump::GribJumpServerApp app(argc, argv);
    app.start();
    return 0;
}