/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJumpBase.h"
#include "gribjump/remote/Protocol.h"

namespace gribjump {

namespace test {
class RemoteProtocolTestAccess;  // test seam, see tests/remote/test_protocol_loopback.cc
}

class RemoteGribJump : public GribJumpBase {

public:  // methods

    RemoteGribJump();
    RemoteGribJump(eckit::net::Endpoint endpoint);
    ~RemoteGribJump();

    size_t scan(const std::vector<eckit::PathName>& path) override { NOTIMP; }
    size_t scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) override;
    size_t forwardScan(const std::map<eckit::PathName, eckit::OffsetList>& map);

    std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<ExtractionRequest>& polyRequest) override;

    std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<PathExtractionRequest>& requests) override;

    std::vector<std::unique_ptr<ExtractionResult>> extract(const eckit::PathName& path,
                                                           const std::vector<eckit::Offset>& offsets,
                                                           const std::vector<std::vector<Range>>& ranges) override {
        NOTIMP;
    }

    void forwardExtract(filemap_t& filemap);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) override;

private:  // methods

    bool receiveErrors(eckit::Stream& stream, bool raise = true);
    void sendHeader(eckit::Stream& stream, RequestType type);

    // Pure stream (de)serialisation of each operation, independent of the
    // transport. Split out from the TCP methods so the wire protocol can be
    // driven over an in-memory stream in tests (client<->server loopback).
    void encodeExtractRequest(eckit::Stream& stream, std::vector<ExtractionRequest>& requests);
    std::vector<std::unique_ptr<ExtractionResult>> decodeExtractReply(eckit::Stream& stream, size_t nRequests);

    void encodeScanRequest(eckit::Stream& stream, const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles);
    size_t decodeScanReply(eckit::Stream& stream);

    void encodeAxesRequest(eckit::Stream& stream, const std::string& request, int level);
    std::map<std::string, std::unordered_set<std::string>> decodeAxesReply(eckit::Stream& stream);

private:  // members

    std::string host_;
    int port_;

    friend class test::RemoteProtocolTestAccess;  // grants tests access to the transport-independent codec helpers
};


}  // namespace gribjump