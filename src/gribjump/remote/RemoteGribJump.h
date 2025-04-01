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

#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJumpBase.h"

namespace gribjump {

enum class RequestType : uint16_t {
    EXTRACT = 0,
    AXES,
    SCAN,
    FORWARD_EXTRACT,
    FORWARD_SCAN
};

constexpr static uint16_t remoteProtocolVersion = 3;

class RemoteGribJump : public GribJumpBase {

public:  // methods

    RemoteGribJump(const Config& config);
    RemoteGribJump(eckit::net::Endpoint endpoint);
    ~RemoteGribJump();

    size_t scan(const std::vector<eckit::PathName>& path) override { NOTIMP; }
    size_t scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) override;
    size_t forwardScan(const std::map<eckit::PathName, eckit::OffsetList>& map);

    std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<ExtractionRequest>& polyRequest) override;

    std::vector<std::unique_ptr<ExtractionResult>> extract(const eckit::PathName& path,
                                                           const std::vector<eckit::Offset>& offsets,
                                                           const std::vector<std::vector<Range>>& ranges) override {
        NOTIMP;
    }

    void forwardExtract(filemap_t& filemap);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) override;

private:  // methods

    bool receiveErrors(eckit::Stream& stream, bool raise = true);
    void sendHeader(eckit::net::InstantTCPStream& stream, RequestType type);

private:  // members

    std::string host_;
    int port_;
};


}  // namespace gribjump