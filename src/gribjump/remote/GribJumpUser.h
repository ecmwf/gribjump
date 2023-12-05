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

#include <memory>

#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class GribJumpUser : public eckit::net::NetUser {
public:
    GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){
    }

    ~GribJumpUser() {}

private:  // methods
    // From net::NetUser

    virtual void serve(eckit::Stream& s, std::istream& in, std::ostream& out){
        // NB: Use EITHER s OR in/out, not both. We use s.
        // TODO: Monitor?

        eckit::Log::info() << "Serving new connection" << std::endl;

        try {
            std::string cmd;
            s >> cmd;

            if (cmd == "EXTRACT"){
                eckit::Log::info() << "Received cmd: EXTRACT" << std::endl;

                // TODO: Parse the request

                // TODO: Extract the data

                // TODO: Send the data back to the client
            }

            else if (cmd == "AXES"){
                eckit::Log::info() << "Received cmd: AXES" << std::endl;
            }

            else {
                throw eckit::SeriousBug("Unknown command " + cmd, Here());
            }
        }
        
        catch (std::exception& e) {
            eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
            eckit::Log::error() << "** Exception is handled" << std::endl;
            try {
                s << e;
            }
            catch (std::exception& a) {
                eckit::Log::error() << "** " << a.what() << " Caught in " << Here() << std::endl;
                eckit::Log::error() << "** Exception is ignored" << std::endl;
            }
        }

        eckit::Log::info() << "Closing connection" << std::endl;
    }

private:  // members
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
