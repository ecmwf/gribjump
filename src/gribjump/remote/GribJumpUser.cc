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


#include "eckit/log/Timer.h"
#include "eckit/system/ResourceUsage.h"

#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/remote/ClientRequest.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out){
    // NB: Use EITHER s OR in/out, not both. We use s.
    // TODO: Monitor?
    eckit::Timer timer("GribJumpUser::serve");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        std::string cmd;
        s >> cmd;
        eckit::Log::info() << "Received cmd: " << cmd << std::endl;

        if (cmd == "EXTRACT"){
            extract(s);
        }

        else if (cmd == "AXES"){
            timer.report("Received AXES request");
            GribJump gj;
            std::string request;
            s >> request;
            std::map<std::string, std::unordered_set<std::string>> axes = gj.axes(request);

            timer.report("AXES finished. Sending results");
            size_t naxes = axes.size();
            s << naxes;
            for (auto& pair : axes) {
                s << pair.first;
                size_t n = pair.second.size();
                s << n;
                for (auto& val : pair.second) {
                    s << val;
                }
            }
            timer.report("Axes sent");

            // print the axes we sent
            for (auto& pair : axes) {
                eckit::Log::info() << pair.first << ": ";
                for (auto& val : pair.second) {
                    eckit::Log::info() << val << ", ";
                }
                eckit::Log::info() << std::endl;
            }
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

    timer.report("Closing connection");
}

void GribJumpUser::extract(eckit::Stream& s){
    eckit::Timer timer("GribJumpUser::extract");

    ClientRequest clientRequest(s);
    clientRequest.doWork();
    
    timer.report("Extract finished. Sending results");
    clientRequest.replyToClient();
    
    timer.report("Results sent");

    LOG_DEBUG_LIB(LibGribJump) << eckit::system::ResourceUsage() << std::endl;
}

}  // namespace gribjump
