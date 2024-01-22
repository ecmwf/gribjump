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
#include "gribjump/remote/ExtractRequest.h"
#include "gribjump/remote/ScanRequest.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out){

    eckit::Timer timer("GribJumpUser::serve");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
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

    timer.report("Closing connection");
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
    
    timer.report("SCAN request received ...");

    ScanRequest request(s);

    request.enqueueTasks();

    timer.report("SCAN tasks enqueued. Waiting for completion ...");

    request.waitForTasks();

    timer.report("SCAN tasks finished. Sending results ...");

    request.replyToClient();
    
    timer.report("SCAN finished. Sending number of files per request ...");

    s << 0;

    timer.report("SCAN results sent");
}

void GribJumpUser::axes(eckit::Stream& s, eckit::Timer& timer) {
    
    timer.report("AXES request received ...");

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

void GribJumpUser::extract(eckit::Stream& s, eckit::Timer& timer){

    timer.report("EXTRACT request received ...");

    ExtractRequest request(s);
    request.enqueueTasks();

    timer.report("EXTRACT tasks enqueued. Waiting for completion ...");

    request.waitForTasks();

    timer.report("EXTRACT tasks finished. Sending results ...");

    request.replyToClient();
    
    timer.report("EXTRACT results sent");
}

}  // namespace gribjump
