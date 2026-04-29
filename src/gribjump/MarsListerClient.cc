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
#include "eckit/log/Log.h"

namespace gribjump {

MarsListerClient::MarsListerClient(const std::string& host, int port) : host_(host), port_(port) {
    eckit::Log::info() << "MarsListerClient targeting " << host_ << ":" << port_ << std::endl;
}

MarsListerClient::~MarsListerClient() {}

std::vector<eckit::URI> MarsListerClient::list(const std::vector<metkit::mars::MarsRequest> requests) {

    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));

    // Send header
    stream << protocolVersion_;
    stream << static_cast<uint16_t>(RequestType::LIST);

    // Send requests
    size_t numRequests = requests.size();
    stream << numRequests;
    for (const auto& req : requests) {
        stream << req;
    }

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

    // Receive echoed requests
    size_t numReceived;
    stream >> numReceived;
    ASSERT(numReceived == numRequests);

    for (size_t i = 0; i < numReceived; i++) {
        metkit::mars::MarsRequest received(stream);

        // Verify the echoed request matches the original
        std::ostringstream origStr, recvStr;
        origStr << requests[i];
        recvStr << received;
        if (origStr.str() != recvStr.str()) {
            throw eckit::SeriousBug(
                "MarsListerClient: echoed request does not match original.\n"
                "  Sent:     " + origStr.str() + "\n"
                "  Received: " + recvStr.str());
        }
    }

    eckit::Log::info() << "MarsListerClient: verified echo of " << numRequests << " request(s)" << std::endl;

    // No real URIs to return yet — the server only echoes requests for now
    return {};
}

std::map<std::string, std::unordered_set<std::string>> MarsListerClient::axes(const std::string& request, int level) {
    NOTIMP;
}

}  // namespace gribjump
