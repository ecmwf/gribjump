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
#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/remote/Request.h"

#include "gribjump/Engine.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol) {}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out) {

    eckit::Timer timer_full("Connection closed");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        eckit::Timer timer("Connection served");
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

    uint16_t version;
    uint16_t i_requestType;

    s >> version;
    if (version != remoteProtocolVersion) {
        throw eckit::SeriousBug("Gribjump remote-protocol mismatch: expected version " + std::to_string(protocolVersion_) + " but got " + std::to_string(version));
    }

    LogContext ctx(s);
    ctx_ = ctx;

    s >> i_requestType;
    RequestType requestType = static_cast<RequestType>(i_requestType);
    switch (requestType) {
        case RequestType::EXTRACT:
            extract(s, timer);
            break;
        case RequestType::AXES:
            axes(s, timer);
            break;
        case RequestType::SCAN:
            scan(s, timer);
            break;
        case RequestType::FORWARD_EXTRACT:
            forwardedExtract(s, timer);
            break;
        default:
            throw eckit::SeriousBug("Unknown request type: " + std::to_string(i_requestType));
    }
}
void GribJumpUser::forwardedExtract(eckit::Stream& s, eckit::Timer& timer) {
    
    timer.reset();

    ForwardedExtractRequest request(s, ctx_);
    timer.reset("ForwardedExtract requests received");

    request.execute();
    timer.reset("ForwardedExtract tasks completed");

    request.replyToClient();
    timer.reset("ForwardedExtract results sent");

}

void GribJumpUser::scan(eckit::Stream& s, eckit::Timer& timer) {

    timer.reset();

    ScanRequest request(s, ctx_);
    timer.reset("SCAN requests received");

    request.execute();
    timer.reset("SCAN tasks completed");

    request.replyToClient();
    timer.reset("SCAN results sent");
}

void GribJumpUser::axes(eckit::Stream& s, eckit::Timer& timer) {

    timer.reset();

    AxesRequest request(s, ctx_);
    timer.reset("Axes request received");

    request.execute();
    timer.reset("Axes tasks completed");

    request.replyToClient();
    timer.reset("Axes results sent");
}

void GribJumpUser::extract(eckit::Stream& s, eckit::Timer& timer) { 

    timer.reset();
    
    ExtractRequest request(s, ctx_);

    timer.reset("EXTRACT requests received");

    request.execute();
    timer.reset("EXTRACT tasks completed");

    request.replyToClient();
    
    request.reportMetrics();

    timer.reset("EXTRACT  results sent");
}


}  // namespace gribjump
