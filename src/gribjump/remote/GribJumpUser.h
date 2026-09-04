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

#pragma once

#include <memory>

#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"
namespace gribjump {

class EngineIface;

//----------------------------------------------------------------------------------------------------------------------

/// Server-side dispatch of a single client request. Reads the request header
/// (protocol version, log context, request type) from the stream and executes
/// the matching Request, replying on the same stream. Throws on protocol
/// version mismatch or unknown request type.
///
/// Factored out of GribJumpUser so the server protocol logic can unit tested.
void dispatchRequest(eckit::Stream& s, EngineIface* engine = nullptr);

//----------------------------------------------------------------------------------------------------------------------

class GribJumpUser : public eckit::net::NetUser {
public:

    GribJumpUser(eckit::net::TCPSocket& protocol);

    ~GribJumpUser();

private:  // methods

    virtual void serve(eckit::Stream& s, std::istream& in, std::ostream& out);

private:  // members
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
