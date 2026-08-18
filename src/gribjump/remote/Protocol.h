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

#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
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

/// v4 reply framing: an EXTRACT/FORWARD_EXTRACT reply is a sequence of tagged
/// chunks. A RESULTS chunk carries a batch of (requestIndex, ExtractionResult)
/// pairs; a terminal END chunk is followed by the error footer (identical
/// layout to the leading error block used by v3).
enum class ReplyChunkTag : uint16_t {
    RESULTS = 0,
    END     = 1
};

/// The protocol version the client advertises in every request header. Kept at
/// 3 (buffered reply) until the client is switched to the v4 streaming framing;
/// the server accepts both, see supportedProtocolVersions.
/// @todo: I don't really like bare lowercase constants like this.
constexpr uint16_t remoteProtocolVersion = 3;

/// The streaming protocol version (v4): EXTRACT/FORWARD_EXTRACT replies are sent
/// as batched result chunks + an error footer instead of a single buffered
/// block.
constexpr uint16_t streamingProtocolVersion = 4;

/// Protocol versions the server accepts.
inline constexpr std::array<uint16_t, 2> supportedProtocolVersions{remoteProtocolVersion, streamingProtocolVersion};

inline bool isSupportedProtocolVersion(uint16_t version) {
    for (uint16_t supported : supportedProtocolVersions) {
        if (supported == version) {
            return true;
        }
    }
    return false;
}


struct ProtocolVersion {
    uint16_t value = remoteProtocolVersion;

    /// v4+ replies stream results as chunks + an error footer; v3 buffers a
    /// single reply block.
    bool streaming() const { return value >= streamingProtocolVersion; }
};

//----------------------------------------------------------------------------------------------------------------------

class Protocol {
public:

    // -- Request header: [protocol version][log context][request type] ------------------------------------------------

    /// The decoded request header: the negotiated protocol version (validated
    /// against supportedProtocolVersions) plus the request type.
    struct RequestHeader {
        ProtocolVersion version;
        RequestType type;
    };

    /// Write the request header the client sends at the start of every request.
    static void writeRequestHeader(eckit::Stream& stream, RequestType type, const LogContext& context,
                                   uint16_t version);

    /// Read and validate the request header. Throws on an unsupported protocol
    /// version, installs the received log context into the ContextManager, and
    /// returns the negotiated version + request type.
    static RequestHeader readRequestHeader(eckit::Stream& stream);

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

    // -- EXTRACT reply, v4 streaming framing --------------------------------------------------------------------------
    // A sequence of RESULTS chunks terminated by an END chunk + error footer.
    // The encoders are batch-composable so the server can flush chunks as work
    // completes (in any order); decodeExtractReplyStreaming reassembles results
    // by requestIndex into an nRequests-sized vector, then reads the footer.

    static void encodeExtractResultChunk(eckit::Stream& stream,
                                         const std::vector<std::pair<size_t, const ExtractionResult*>>& batch);
    static void encodeExtractReplyEnd(eckit::Stream& stream, const std::vector<std::string>& errors);
    static std::vector<std::unique_ptr<ExtractionResult>> decodeExtractReplyStreaming(eckit::Stream& stream,
                                                                                      size_t nRequests,
                                                                                      bool raise = true);

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
