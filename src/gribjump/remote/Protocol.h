/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// Single source of truth for the remote gribjump wire protocol: the protocol
/// version, the request-type enum, and the request-header framing shared by the
/// client (RemoteGribJump) and the server (GribJumpUser). Centralising the
/// header (de)serialisation here structurally prevents the two sides from
/// drifting apart.
///
/// Any change to the byte layout below is a protocol change: bump
/// remoteProtocolVersion and regenerate the golden hashes in
/// tests/remote/test_protocol_codec.cc.

#pragma once

#include <cstdint>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/Stream.h"

#include "gribjump/Metrics.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

enum class RequestType : uint16_t {
    EXTRACT = 0,
    AXES,
    SCAN,
    FORWARD_EXTRACT,
    FORWARD_SCAN
};

constexpr uint16_t remoteProtocolVersion = 3;

//----------------------------------------------------------------------------------------------------------------------
// Request header framing: [protocol version][log context][request type].

/// Write the request header, as the client does at the start of every request.
inline void writeRequestHeader(eckit::Stream& stream, RequestType type, const LogContext& context) {
    stream << remoteProtocolVersion;
    stream << context;
    stream << static_cast<uint16_t>(type);
}

/// Read and validate the request header from the client. Throws on protocol
/// version mismatch. Installs the received log context into the ContextManager
/// and returns the request type.
inline RequestType readRequestHeader(eckit::Stream& stream) {
    uint16_t version;
    stream >> version;
    if (version != remoteProtocolVersion) {
        throw eckit::SeriousBug(
            "Gribjump remote-protocol mismatch: Serverside version: " + std::to_string(remoteProtocolVersion) +
            ", Clientside version: " + std::to_string(version));
    }

    LogContext ctx(stream);
    ContextManager::instance().set(ctx);

    uint16_t i_requestType;
    stream >> i_requestType;
    return static_cast<RequestType>(i_requestType);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
