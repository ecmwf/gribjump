/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// Defines the remote gribjump wire protocol.
/// Every message on the wire has one encoder and one decoder, defined here,
/// used by both the client and the server. The request-type enum and the
/// protocol version constant live here too, so this header is the single home
/// for everything that describes the wire format.
///
/// The methods operate on an abstract eckit::Stream, so the exact same code
/// runs over a TCP socket in production and over an in-memory stream in tests.
///
/// Any change to a byte layout below is a protocol change: bump
/// remoteProtocolVersion and regenerate the golden hashes in
/// tests/remote/test_protocol_codec.cc.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "eckit/serialisation/Stream.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Metrics.h"
#include "gribjump/Types.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// Wire-protocol constants

enum class RequestType : uint16_t {
    EXTRACT = 0,
    AXES,
    SCAN,
    FORWARD_EXTRACT,
    FORWARD_SCAN
};

constexpr uint16_t remoteProtocolVersion = 3;

//----------------------------------------------------------------------------------------------------------------------

class Protocol {
public:

    // -- Request header: [protocol version][log context][request type] ------------------------------------------------

    /// Write the request header the client sends at the start of every request.
    static void writeRequestHeader(eckit::Stream& stream, RequestType type, const LogContext& context);

    /// Read and validate the request header. Throws on protocol version
    /// mismatch, installs the received log context into the ContextManager, and
    /// returns the request type.
    static RequestType readRequestHeader(eckit::Stream& stream);

    // -- Error block: [nErrors][error string]* (precedes every reply) -------------------------------------------------

    static void encodeErrors(eckit::Stream& stream, const std::vector<std::string>& errors);

    /// Decode the error block. Returns false when there were no errors. When
    /// errors are present, either throws eckit::RemoteException (raise=true) or
    /// logs them (raise=false) and returns true.
    static bool decodeErrors(eckit::Stream& stream, bool raise = true);

    // -- EXTRACT ------------------------------------------------------------------------------------------------------

    static void encodeExtractRequest(eckit::Stream& stream, const std::vector<ExtractionRequest>& requests);
    static std::vector<ExtractionRequest> decodeExtractRequest(eckit::Stream& stream);

    static void encodeExtractReply(eckit::Stream& stream, const std::vector<const ExtractionResult*>& results);
    static std::vector<std::unique_ptr<ExtractionResult>> decodeExtractReply(eckit::Stream& stream, size_t nRequests);

    // -- SCAN ---------------------------------------------------------------------------------------------------------

    static void encodeScanRequest(eckit::Stream& stream, const std::vector<metkit::mars::MarsRequest>& requests,
                                  bool byfiles);
    static std::vector<metkit::mars::MarsRequest> decodeScanRequest(eckit::Stream& stream, bool& byfiles);

    static void encodeScanReply(eckit::Stream& stream, size_t nFields);
    static size_t decodeScanReply(eckit::Stream& stream);

    // -- AXES ---------------------------------------------------------------------------------------------------------

    static void encodeAxesRequest(eckit::Stream& stream, const std::string& request, int level);
    static void decodeAxesRequest(eckit::Stream& stream, std::string& request, int& level);

    static void encodeAxesReply(eckit::Stream& stream,
                                const std::map<std::string, std::unordered_set<std::string>>& axes);
    static std::map<std::string, std::unordered_set<std::string>> decodeAxesReply(eckit::Stream& stream);

    // -- FORWARD_SCAN (server-to-server) ------------------------------------------------------------------------------
    // The reply is a bare field count, identical to the SCAN reply; reuse
    // encode/decodeScanReply for it.

    static void encodeForwardScanRequest(eckit::Stream& stream, const scanmap_t& scanmap);
    static scanmap_t decodeForwardScanRequest(eckit::Stream& stream);

    // -- FORWARD_EXTRACT (server-to-server) ---------------------------------------------------------------------------

    /// Decoded forwarded-extract request: the owning ExtractionItems plus a
    /// filemap of non-owning pointers into them (grouped by filename).
    struct ForwardExtractRequest {
        std::vector<std::unique_ptr<ExtractionItem>> items;
        filemap_t filemap;
    };

    /// Encodes the request; sorts each file's items by offset first (as the
    /// wire order is offset-ascending), hence the non-const filemap.
    static void encodeForwardExtractRequest(eckit::Stream& stream, filemap_t& filemap);
    static ForwardExtractRequest decodeForwardExtractRequest(eckit::Stream& stream);

    static void encodeForwardExtractReply(eckit::Stream& stream, const filemap_t& filemap);
    /// Reads the results back into the caller's filemap in place, asserting the
    /// per-file item counts match what was sent.
    static void decodeForwardExtractReply(eckit::Stream& stream, filemap_t& filemap);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
