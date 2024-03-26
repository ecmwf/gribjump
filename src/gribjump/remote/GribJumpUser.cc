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
/// @author Tiago Quintino

#include "eckit/log/Timer.h"
#include "eckit/system/ResourceUsage.h"

#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/remote/Request.h"

#include "gribjump/Engine.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out){

    eckit::Timer timer_full("Connection closed");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        eckit::Timer timer;
        handle_client(s, timer);
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
        
    LOG_DEBUG_LIB(LibGribJump) << eckit::system::ResourceUsage() << std::endl;

}

void GribJumpUser::handle_client(eckit::Stream& s, eckit::Timer& timer) {
    std::string request;
    s >> request;
    if (request == "EXTRACT") {
        extract(s, timer);
    }
    else if (request == "AXES") {
        axes(s, timer);
    }
    else if (request == "SCAN") {
        scan(s, timer);
    }
    else {
        throw eckit::SeriousBug("Unknown request type: " + request);
    }
}

void GribJumpUser::scan(eckit::Stream& s, eckit::Timer& timer) {

    /// @todo, check if this is still working.

    timer.reset();

    ScanRequest request(s);

    timer.reset("SCAN requests received");

    request.execute();
    
    timer.reset("SCAN tasks completed");

    request.replyToClient();
    
    // s << size_t(0);

    timer.reset("SCAN scan results sent");
}

void GribJumpUser::axes(eckit::Stream& s, eckit::Timer& timer) {

    /// @todo, check if this is still working.

    timer.reset();

    AxesRequest request(s);

    timer.reset("Axes request received");

    request.execute();
    
    timer.reset("Axes tasks completed");

    request.replyToClient();
    
    // s << size_t(0);

    timer.reset("Axes results sent");
}

void GribJumpUser::extract(eckit::Stream& s, eckit::Timer& timer){ 

    /// @todo, check if this is still working.

    timer.reset();

    ExtractRequest request(s);

    timer.reset("EXTRACT requests received");

    request.execute();
    
    timer.reset("EXTRACT tasks completed");

    request.replyToClient();
    
    // s << size_t(0);

    timer.reset("EXTRACT  results sent");
}


}  // namespace gribjump
