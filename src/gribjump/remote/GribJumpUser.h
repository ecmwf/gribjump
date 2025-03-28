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

#pragma once

#include <memory>

#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"
namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class GribJumpUser : public eckit::net::NetUser {
public:

    GribJumpUser(eckit::net::TCPSocket& protocol);

    ~GribJumpUser();

private:  // methods

    virtual void serve(eckit::Stream& s, std::istream& in, std::ostream& out);

    void handle_client(eckit::Stream& s, eckit::Timer& timer);

    template <typename RequestType>
    void processRequest(eckit::Stream& s);

private:  // members
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
