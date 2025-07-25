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

#include "gribjump/LibGribJump.h"
#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/remote/Request.h"

#include "gribjump/Engine.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol) : NetUser(protocol) {}

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
        MetricsManager::instance().set("error", e.what());
        try {
            s << e;
        }
        catch (...) {
            eckit::Log::error() << "** Exception is ignored" << std::endl;
        }
    }
    catch (...) {
        eckit::Log::error() << "** Unknown exception caught in " << Here() << std::endl;
        eckit::Log::error() << "** Exception is ignored" << std::endl;
        MetricsManager::instance().set("error", "Uncaught exception");
    }

    LOG_DEBUG_LIB(LibGribJump) << eckit::system::ResourceUsage() << std::endl;

    MetricsManager::instance().report();
}

void GribJumpUser::handle_client(eckit::Stream& s, eckit::Timer& timer) {
    uint16_t version;
    uint16_t i_requestType;

    s >> version;
    if (version != remoteProtocolVersion) {
        throw eckit::SeriousBug(
            "Gribjump remote-protocol mismatch: Serverside version: " + std::to_string(remoteProtocolVersion) +
            ", Clientside version: " + std::to_string(version));
    }

    LogContext ctx(s);
    ContextManager::instance().set(ctx);

    s >> i_requestType;
    RequestType requestType = static_cast<RequestType>(i_requestType);

    switch (requestType) {
        case RequestType::EXTRACT:
            processRequest<ExtractRequest>(s);
            break;
        case RequestType::EXTRACT_FROM_PATHS:
            processRequest<ExtractFromPathsRequest>(s);
            break;
        case RequestType::AXES:
            processRequest<AxesRequest>(s);
            break;
        case RequestType::SCAN:
            processRequest<ScanRequest>(s);
            break;
        case RequestType::FORWARD_EXTRACT:
            processRequest<ForwardedExtractRequest>(s);
            break;
        case RequestType::FORWARD_SCAN:
            processRequest<ForwardedScanRequest>(s);
            break;
        default:
            throw eckit::SeriousBug("Unknown request type: " + std::to_string(i_requestType));
    }
}

template <typename RequestType>
void GribJumpUser::processRequest(eckit::Stream& s) {
    eckit::Timer timer("GribJumpUser::processRequest");

    RequestType request(s);
    MetricsManager::instance().set("elapsed_receive", timer.elapsed());
    timer.reset("Request received");
    request.info();

    request.execute();
    MetricsManager::instance().set("elapsed_execute", timer.elapsed());
    timer.reset("Request executed");

    request.reportErrors();
    request.replyToClient();
    MetricsManager::instance().set("elapsed_reply", timer.elapsed());
    timer.reset("Request replied");
}

}  // namespace gribjump
