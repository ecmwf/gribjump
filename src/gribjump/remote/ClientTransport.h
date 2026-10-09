/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <memory>

namespace eckit {
class Stream;
}

namespace gribjump {

/// Owns eckit::Stream used by client in comms to server.
class ClientConnection {
public:

    virtual ~ClientConnection() = default;

    virtual eckit::Stream& stream() = 0;
};

/// Factory for client connections. Production uses TCP, tests can inject a fake stream.
class ClientTransport {
public:

    virtual ~ClientTransport() = default;

    virtual std::unique_ptr<ClientConnection> connect() = 0;
};

}  // namespace gribjump
