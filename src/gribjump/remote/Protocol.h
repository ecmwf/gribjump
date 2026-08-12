/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// Wire-protocol constants for the remote gribjump client/server: the protocol
/// version and the request-type enum. Kept deliberately lightweight (no codec
/// includes) so any translation unit can name a RequestType or check the
/// version cheaply. The actual (de)serialisation lives in ProtocolCodec.h.
///
/// Any change to the byte layout of a message is a protocol change: bump
/// remoteProtocolVersion and regenerate the golden hashes in
/// tests/remote/test_protocol_codec.cc.

#pragma once

#include <cstdint>

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

}  // namespace gribjump
