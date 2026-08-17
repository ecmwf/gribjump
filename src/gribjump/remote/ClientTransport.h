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

/// One request/reply exchange's transport. A ClientConnection owns whatever
/// backs the stream (socket + stream wrapper) and keeps it alive for the
/// duration of the exchange; destroying it tears the connection down.
class ClientConnection {
public:

    virtual ~ClientConnection() = default;

    /// The stream the client encodes the request into and decodes the reply from.
    virtual eckit::Stream& stream() = 0;
};

/// Factory for client connections. Production uses a TCP transport; tests inject a fake
/// to exercise the client without a live TCP server.
class ClientTransport {
public:

    virtual ~ClientTransport() = default;

    /// Open a fresh connection for a single request/reply exchange.
    virtual std::unique_ptr<ClientConnection> connect() = 0;
};

}  // namespace gribjump
