/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "gribjump/remote/GribJumpServer.h"
#include "eckit/runtime/Application.h"

#include <unistd.h>
#include <fstream>

#include "eckit/net/Port.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"

namespace gribjump {


class debugApp : public eckit::Application {
public:
    debugApp(int argc, char** argv) : Application(argc, argv) {}
    ~debugApp() {}

private:
    debugApp(const debugApp&);

    void run() override {
        eckit::Log::info() << "debug app running" << std::endl;

        // connect to server
        eckit::net::TCPClient client;
        eckit::net::InstantTCPStream stream(client.connect("localhost", 9001));
        eckit::Log::info() << "connected" << std::endl;

        stream << "EXTRACT";
        eckit::Log::info() << "sent: EXTRACT" << std::endl;

    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump


int main(int argc, char** argv) {
    gribjump::debugApp app(argc, argv);
    app.start();
    return 0;
}
