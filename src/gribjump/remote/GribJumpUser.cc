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
#include "gribjump/remote/ExtractFileRequest.h"
#include "gribjump/remote/ScanRequest.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out){

    eckit::Timer timer_full("GribJumpUser::serve");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        eckit::Timer timer("GribJumpUser::serve");
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

    timer_full.report("Connection closed");
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
    
    timer.reset();

    ScanRequest request(s);

    request.enqueueTasks();

    timer.reset("SCAN tasks enqueued.");

    request.waitForTasks();

    timer.reset("SCAN tasks completed");

    request.replyToClient();
    
    s << size_t(0);

    timer.reset("SCAN scan results sent");
}

void GribJumpUser::axes(eckit::Stream& s, eckit::Timer& timer) {
    
    timer.reset();

    GribJump gj;
    std::string request;
    s >> request;
    std::map<std::string, std::unordered_set<std::string>> axes = gj.axes(request);

    timer.reset("AXES found");

    // TODO: reporting of axes errors.
    size_t nerror = 0;
    s << nerror;

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

    timer.report("AXES sent to client");

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

    timer.reset();

    ExtractFileRequest request(s);
    request.enqueueTasks();

    timer.reset("EXTRACT tasks enqueued");

    request.waitForTasks();

    timer.reset("EXTRACT tasks completed");

    request.replyToClient();
    
    timer.reset("EXTRACT results sent");
}

}  // namespace gribjump
