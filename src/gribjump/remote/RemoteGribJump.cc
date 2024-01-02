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

#include "gribjump/remote/RemoteGribJump.h"
#include "gribjump/GribJumpFactory.h"

namespace gribjump
{

    RemoteGribJump::RemoteGribJump(): host_(getenv("GRIBJUMP_REMOTE_HOST")), port_(atoi(getenv("GRIBJUMP_REMOTE_PORT")))
    {
    }
    RemoteGribJump::~RemoteGribJump() {}

    std::vector<std::vector<ExtractionResult>> RemoteGribJump::extract(std::vector<ExtractionRequest> polyRequest) {
        std::vector<std::vector<ExtractionResult>> result;

        // connect to server
        eckit::net::TCPClient client;
        eckit::net::InstantTCPStream stream(client.connect(host_, port_));
        eckit::Log::info() << "connected" << std::endl;

        stream << "EXTRACT";
        eckit::Log::info() << "sent: EXTRACT" << std::endl;

        size_t nRequests = polyRequest.size();
        stream << nRequests;
        for (auto& req : polyRequest) {
            stream << req;
        }
        eckit::Log::info() << "Sent " << nRequests << " requests" << std::endl;

        // receive response
        for (size_t i = 0; i < nRequests; i++) {
            std::vector<ExtractionResult> response;
            size_t nfields;
            stream >> nfields;
            for (size_t i = 0; i < nfields; i++) {
                ExtractionResult output(stream);
                response.push_back(output);
            }
            result.push_back(response);
        }

        eckit::Log::info() << "All data received" << std::endl;

        return result;
    }
    std::vector<ExtractionResult> RemoteGribJump::extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) {
        NOTIMP;
    }

    std::map<std::string, std::unordered_set<std::string>> RemoteGribJump::axes(const std::string& request) {
        NOTIMP;
    }

static GribJumpBuilder<RemoteGribJump> builder("remotegribjump");
    
} // namespace gribjump