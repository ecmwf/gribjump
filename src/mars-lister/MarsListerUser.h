/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"

namespace marslister {

constexpr static uint16_t protocolVersion = 1;

enum class RequestType : uint16_t {
    LIST = 0
};

class MarsListerUser : public eckit::net::NetUser {
public:

    MarsListerUser(eckit::net::TCPSocket& protocol);

    ~MarsListerUser();

private:

    void serve(eckit::Stream& s, std::istream& in, std::ostream& out) override;

    void handle(eckit::Stream& s);

    void handleList(eckit::Stream& s);
};

}  // namespace marslister
