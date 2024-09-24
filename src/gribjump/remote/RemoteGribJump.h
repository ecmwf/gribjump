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
#include "gribjump/GribJumpBase.h"

namespace gribjump {

class RemoteGribJump : public GribJumpBase {

public: // methods

    RemoteGribJump(const Config& config);
    RemoteGribJump(eckit::net::Endpoint endpoint);
    ~RemoteGribJump();

    size_t scan(const eckit::PathName& path) override;
    size_t scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) override;

    std::vector<std::vector<ExtractionResult*>> extract(std::vector<ExtractionRequest> polyRequest, LogContext ctx) override;
    std::vector<std::unique_ptr<ExtractionItem>> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) override;
    void extract(filemap_t& filemap);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;

protected: // methods

    bool receiveErrors(eckit::Stream& stream, bool raise=true);

private: // members
    std::string host_;
    int port_;
};


}// namespace gribjump