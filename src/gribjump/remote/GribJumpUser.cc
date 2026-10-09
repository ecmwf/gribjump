/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley
/// @author Tiago Quintino

#include <memory>
#include <optional>

#include "eckit/log/Timer.h"
#include "eckit/system/ResourceUsage.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/Protocol.h"
#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/remote/RequestHandler.h"

#include "gribjump/Engine.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol) : NetUser(protocol) {}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out) {

    eckit::Timer timer_full("Connection closed");

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        eckit::Timer timer("Connection served");
        dispatchRequest(s);
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

namespace {

std::unique_ptr<RequestHandler> makeRequestHandler(RequestType type, eckit::Stream& s, EngineIface& engine,
                                                   ProtocolVersion version) {
    switch (type) {
        case RequestType::EXTRACT:
            return std::make_unique<ExtractHandler>(s, engine, version);
        case RequestType::AXES:
            return std::make_unique<AxesHandler>(s, engine, version);
        case RequestType::SCAN:
            return std::make_unique<ScanHandler>(s, engine, version);
        case RequestType::FORWARD_EXTRACT:
            return std::make_unique<ForwardedExtractHandler>(s, engine, version);
        case RequestType::FORWARD_SCAN:
            return std::make_unique<ForwardedScanHandler>(s, engine, version);
        default:
            throw eckit::SeriousBug("Unknown request type: " + std::to_string(static_cast<uint16_t>(type)));
    }
}

}  // namespace

void dispatchRequest(eckit::Stream& s, EngineIface* injectedEngine) {
    const Protocol::RequestHeader header = Protocol::readRequestHeader(s);

    // By default we create an engine, though tests are allowed to
    // inject one (e.g. a MockEngine) for unit testing.
    std::optional<Engine> ownedEngine;
    if (!injectedEngine) {
        ownedEngine.emplace();
    }
    EngineIface& engine = injectedEngine ? *injectedEngine : *ownedEngine;

    makeRequestHandler(header.type, s, engine, header.version)->process();
}


}  // namespace gribjump
