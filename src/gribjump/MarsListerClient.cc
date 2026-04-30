/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "gribjump/MarsListerClient.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/value/Value.h"

namespace gribjump {

MarsListerClient::MarsListerClient(const std::string& host, int port) : host_(host), port_(port) {
    eckit::Log::info() << "MarsListerClient targeting " << host_ << ":" << port_ << std::endl;
}

MarsListerClient::~MarsListerClient() {}

std::vector<eckit::URI> MarsListerClient::list(const std::vector<metkit::mars::MarsRequest> requests) {

    std::vector<eckit::URI> allURIs;

    for (const auto& request : requests) {

        eckit::net::TCPClient client;
        eckit::net::InstantTCPStream stream(client.connect(host_, port_));

        // Send header
        stream << protocolVersion_;
        stream << static_cast<uint16_t>(RequestType::LIST);

        // Send single request
        stream << request;

        // Receive errors
        size_t nErrors;
        stream >> nErrors;
        if (nErrors > 0) {
            std::stringstream ss;
            ss << "MarsListerClient received " << nErrors << " server-side error(s):" << std::endl;
            for (size_t i = 0; i < nErrors; i++) {
                std::string error;
                stream >> error;
                ss << error << std::endl;
            }
            throw eckit::RemoteException(ss.str(), Here());
        }

        // Receive JSON response
        std::string json;
        stream >> json;

        eckit::Log::info() << "MarsListerClient: received JSON: " << json << std::endl;

        // Parse JSON array of {path, offsets[], lengths[]}
        eckit::Value parsed = eckit::JSONParser::decodeString(json);

        for (size_t i = 0; i < parsed.size(); i++) {
            const eckit::Value& entry = parsed[i];
            std::string path = entry["path"];
            eckit::Value offsets = entry["offsets"];
            // eckit::Value lengths = entry["lengths"]; // TODO: use lengths when needed

            for (size_t j = 0; j < offsets.size(); j++) {
                long long offset = offsets[j];
                eckit::URI uri("file", eckit::PathName(path));
                uri.fragment(std::to_string(offset));
                allURIs.push_back(uri);
            }
        }

        eckit::Log::info() << "MarsListerClient: parsed " << parsed.size()
                           << " URI(s) for request" << std::endl;
    }

    return allURIs;
}

std::map<std::string, std::unordered_set<std::string>> MarsListerClient::axes(const std::string& request, int level) {
    NOTIMP;
}

}  // namespace gribjump
