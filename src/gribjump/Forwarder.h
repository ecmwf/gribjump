/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
#include "eckit/net/Endpoint.h"
#include "gribjump/Engine.h"
#include "gribjump/Task.h"
#include "gribjump/Types.h"

#pragma once

// Class used by Engine to forward requests onto remote servers
// Example: when the remoteFDB is in use, data is distributed across multiple stores and a catalogue
// The Engine will obtain the URIs from the catalogue and use this class to forward the requests to the appropriate
// gribjump server
namespace gribjump {

class Forwarder {
public:

    Forwarder();
    ~Forwarder();

    TaskOutcome<size_t> scan(const std::vector<eckit::URI>& uris);
    TaskReport extract(filemap_t& filemap);

private:

    std::unordered_map<eckit::net::Endpoint, filemap_t> serverFileMap(filemap_t& filemap);

    const eckit::net::Endpoint& serverForURI(const eckit::URI& uri) const;
};

}  // namespace gribjump