/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Wire-format / codec regression tests for the remote gribjump protocol.
///
/// These tests are deliberately FDB-free and socket-free: they drive the exact
/// production Protocol encode paths used by the real client and server
/// against an in-memory buffer. Their purpose is twofold:
///   1. round-trip: prove encode followed by decode reconstructs the object;
///   2. golden hash: pin the exact bytes on the wire so that any change to the
///      serialised format is detected and forces a deliberate protocol change.
///
/// Because the framed cases call Protocol directly (rather than
/// re-implementing the framing here), a change to the production encoders WILL
/// change these hashes and fail the test -- which is the point.
///
/// If a golden hash below changes, the wire protocol has changed. That must be
/// accompanied by a bump of `remoteProtocolVersion` and an update of the golden
/// values here (regenerate by running this test; the actual hash is printed on
/// failure).

#include <bitset>

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/ResizableMemoryStream.h"
#include "eckit/testing/Test.h"
#include "eckit/utils/MD5.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Metrics.h"
#include "gribjump/Types.h"
#include "gribjump/remote/Protocol.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// Helpers

/// Encode via a caller-provided lambda into a fresh buffer, returning the MD5
/// hash of the exact bytes written.
template <typename EncodeFn>
static std::string hashOfEncoded(EncodeFn&& encode, eckit::Buffer& buffer) {
    eckit::ResizableMemoryStream s(buffer);
    encode(s);
    return eckit::MD5(buffer.data(), s.position()).digest();
}

/// Assert a golden hash, printing the actual value on mismatch so it can be
/// copied back into the test when an intentional protocol change is made.
static void expectGolden(const std::string& actual, const std::string& expected, const std::string& what) {
    if (actual != expected) {
        eckit::Log::error() << "Wire-format golden mismatch for [" << what << "]\n"
                            << "  expected: " << expected << "\n"
                            << "  actual:   " << actual << "\n"
                            << "  -> the protocol may have changed; if intentional, bump "
                               "remoteProtocolVersion and update the golden."
                            << std::endl;
    }
    EXPECT_EQUAL(actual, expected);
}

/// Two fixed ExtractionRequests reused across framed tests.
static ExtractionRequest fixtureRequest0() {
    return ExtractionRequest("class=rd,expver=xxxx,levtype=sfc,param=151130,step=2", {{0, 5}, {20, 30}},
                             "33c7d6025995e1b4913811e77d38ec50");
}
static ExtractionRequest fixtureRequest1() {
    return ExtractionRequest("class=rd,expver=xxxx,levtype=sfc,param=151130,step=1", {{0, 5}, {20, 30}},
                             "33c7d6025995e1b4913811e77d38ec50");
}

/// A fixed MarsRequest built without touching FDB.
static metkit::mars::MarsRequest fixtureMarsRequest() {
    metkit::mars::MarsRequest req("retrieve");
    req.setValue("class", "rd");
    req.setValue("expver", "xxxx");
    req.setValue("levtype", "sfc");
    return req;
}

//-----------------------------------------------------------------------------
// Version guard: a change here must be deliberate and accompanied by fixture
// updates below.

CASE("Remote protocol version is pinned") {
    // Advertised version stays 3 until the client is switched to v4 framing.
    EXPECT_EQUAL(remoteProtocolVersion, 3);
    EXPECT_EQUAL(streamingProtocolVersion, 4);
    // Accept-set for the v3->v4 migration window.
    EXPECT_EQUAL(supportedProtocolVersions.size(), 2ul);
    EXPECT(isSupportedProtocolVersion(3));
    EXPECT(isSupportedProtocolVersion(4));
    EXPECT(!isSupportedProtocolVersion(2));
    EXPECT(!isSupportedProtocolVersion(5));
}

//-----------------------------------------------------------------------------
// Payload-type codecs

CASE("ExtractionRequest round-trips and matches golden") {
    ExtractionRequest req("class=rd,expver=xxxx,levtype=sfc,param=151130,step=2", {{0, 5}, {20, 30}},
                          "33c7d6025995e1b4913811e77d38ec50");

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded([&](eckit::Stream& s) { s << req; }, buffer);

    // Round-trip
    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    ExtractionRequest back(in);
    EXPECT_EQUAL(back.requestString(), req.requestString());
    EXPECT_EQUAL(back.gridHash(), req.gridHash());
    EXPECT(back.ranges() == req.ranges());

    expectGolden(hash, "48429df28fce61a37ed775e67b198fad", "ExtractionRequest");
}

CASE("ExtractionResult round-trips and matches golden") {
    std::vector<std::vector<double>> values        = {{1.0, 2.0, 3.0}, {4.0, 5.0}};
    std::vector<std::vector<std::bitset<64>>> mask = {{std::bitset<64>(0xAAAAAAAAAAAAAAAAULL)},
                                                      {std::bitset<64>(0x1), std::bitset<64>(0x0)}};
    ExtractionResult res(std::move(values), std::move(mask));

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded([&](eckit::Stream& s) { s << res; }, buffer);

    // Round-trip
    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    ExtractionResult back(in);
    EXPECT_EQUAL(back.nrange(), 2);
    EXPECT_EQUAL(back.nvalues(0), 3);
    EXPECT_EQUAL(back.nvalues(1), 2);
    EXPECT_EQUAL(back.values()[0][0], 1.0);
    EXPECT_EQUAL(back.values()[1][1], 5.0);
    EXPECT_EQUAL(back.mask()[0][0].to_ullong(), 0xAAAAAAAAAAAAAAAAULL);

    expectGolden(hash, "27c54466dff843cbb54b1be14fc75d15", "ExtractionResult");
}

CASE("LogContext round-trips and matches golden") {
    LogContext ctx("{\"origin\":\"test\",\"description\":\"test test test\"}");

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded([&](eckit::Stream& s) { s << ctx; }, buffer);

    // Round-trip
    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    LogContext back(in);
    std::stringstream a, b;
    a << ctx;
    b << back;
    EXPECT_EQUAL(a.str(), b.str());

    expectGolden(hash, "f553e35ef0cf22c4ad4c0ef1350a0714", "LogContext");
}

//-----------------------------------------------------------------------------
// Framed requests: full header + payload, exactly as the client sends them.

CASE("EXTRACT request frame matches golden") {
    std::vector<ExtractionRequest> requests = {fixtureRequest0(), fixtureRequest1()};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::writeRequestHeader(s, RequestType::EXTRACT, LogContext("{}"));
            Protocol::encodeExtractRequest(s, requests);
        },
        buffer);

    expectGolden(hash, "048fc1d09168779d0770f1db388a0a13", "EXTRACT frame");
}

CASE("AXES request frame matches golden") {
    std::string request = "class=rd,expver=xxxx";
    int level           = 3;

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::writeRequestHeader(s, RequestType::AXES, LogContext("{}"));
            Protocol::encodeAxesRequest(s, request, level);
        },
        buffer);

    expectGolden(hash, "6f150fbf7a397621a29f563379421406", "AXES frame");
}

CASE("SCAN request frame matches golden") {
    std::vector<metkit::mars::MarsRequest> requests = {fixtureMarsRequest()};
    bool byfiles                                    = false;

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::writeRequestHeader(s, RequestType::SCAN, LogContext("{}"));
            Protocol::encodeScanRequest(s, requests, byfiles);
        },
        buffer);

    expectGolden(hash, "6d728d928b5362ded48725a81bd32783", "SCAN frame");
}

CASE("FORWARD_SCAN request frame matches golden") {
    eckit::OffsetList offsets = {eckit::Offset(0), eckit::Offset(1024), eckit::Offset(2048)};
    scanmap_t scanmap;
    scanmap[eckit::PathName("/data/file.grib")] = offsets;

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::writeRequestHeader(s, RequestType::FORWARD_SCAN, LogContext("{}"));
            Protocol::encodeForwardScanRequest(s, scanmap);
        },
        buffer);

    expectGolden(hash, "c04f95d146c8186e88ae74d533cbb657", "FORWARD_SCAN frame");
}

CASE("FORWARD_EXTRACT request frame matches golden") {
    // Build a real ExtractionItem so the production encoder drives the bytes.
    // Note production sends an ExtractionRequest with an EMPTY request string
    // (only intervals + gridHash), plus the item URI.
    auto item = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(fixtureRequest0()));
    item->URI(eckit::URI("file", eckit::PathName("/data/file.grib")));

    filemap_t filemap;
    filemap["/data/file.grib"] = {item.get()};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::writeRequestHeader(s, RequestType::FORWARD_EXTRACT, LogContext("{}"));
            Protocol::encodeForwardExtractRequest(s, filemap);
        },
        buffer);

    expectGolden(hash, "5aa4ba19a9436ebca7cffea55ceae27b", "FORWARD_EXTRACT frame");
}

//-----------------------------------------------------------------------------
// Framed replies: the error block (nErrors) followed by the op-specific reply,
// exactly as the server sends them (reportErrors then replyToClient).

/// Build the fixed ExtractionResult used in reply fixtures.
static ExtractionResult fixtureResult() {
    std::vector<std::vector<double>> values        = {{1.0, 2.0, 3.0}, {4.0, 5.0}};
    std::vector<std::vector<std::bitset<64>>> mask = {{std::bitset<64>(0xAAAAAAAAAAAAAAAAULL)},
                                                      {std::bitset<64>(0x1), std::bitset<64>(0x0)}};
    return ExtractionResult(std::move(values), std::move(mask));
}

CASE("EXTRACT reply frame matches golden") {
    // Two results, each framed by the production encoder (incl. the currently
    // mandatory nfields == 1).
    ExtractionResult res0                        = fixtureResult();
    ExtractionResult res1                        = fixtureResult();
    std::vector<const ExtractionResult*> results = {&res0, &res1};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeErrors(s, {});
            Protocol::encodeExtractReply(s, results);
        },
        buffer);

    expectGolden(hash, "fbd78b1b117de6d51ddfc515f2bc8cdf", "EXTRACT reply");
}

//-----------------------------------------------------------------------------
// EXTRACT reply, v4 streaming framing: RESULTS chunks (each a batch of
// (requestIndex, result) pairs) terminated by an END chunk + error trailer.
// The v3 golden above is retained deliberately as the migration compatibility
// guard; these pin the new v4 bytes alongside it.

CASE("EXTRACT v4 reply (single chunk) round-trips and matches golden") {
    ExtractionResult res0 = fixtureResult();
    ExtractionResult res1 = fixtureResult();
    std::vector<std::pair<size_t, const ExtractionResult*>> batch = {{0, &res0}, {1, &res1}};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeExtractResultChunk(s, batch);
            Protocol::encodeExtractReplyEnd(s, {});
        },
        buffer);

    // Round-trip: results reassembled by index into an nRequests-sized vector.
    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    auto results = Protocol::decodeExtractReplyStreaming(in, 2);
    EXPECT_EQUAL(results.size(), 2ul);
    EXPECT(results[0] != nullptr);
    EXPECT(results[1] != nullptr);
    EXPECT_EQUAL(results[0]->nrange(), 2ul);
    EXPECT_EQUAL(results[1]->values()[1][1], 5.0);

    expectGolden(hash, "00f6ac451a792479d092d78ac151a900", "EXTRACT v4 reply single chunk");
}

CASE("EXTRACT v4 reply (multi-chunk, out of order) round-trips and matches golden") {
    ExtractionResult res0 = fixtureResult();
    ExtractionResult res1 = fixtureResult();
    ExtractionResult res2 = fixtureResult();

    // Deliver index 2 first, then indices 0 and 1 in a second chunk: the
    // streaming server flushes batches in completion order, not request order.
    std::vector<std::pair<size_t, const ExtractionResult*>> batchA = {{2, &res2}};
    std::vector<std::pair<size_t, const ExtractionResult*>> batchB = {{0, &res0}, {1, &res1}};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeExtractResultChunk(s, batchA);
            Protocol::encodeExtractResultChunk(s, batchB);
            Protocol::encodeExtractReplyEnd(s, {});
        },
        buffer);

    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    auto results = Protocol::decodeExtractReplyStreaming(in, 3);
    EXPECT_EQUAL(results.size(), 3ul);
    EXPECT(results[0] != nullptr);
    EXPECT(results[1] != nullptr);
    EXPECT(results[2] != nullptr);

    expectGolden(hash, "c8fb500cdcbf9ee7c55e4314ebf03a30", "EXTRACT v4 reply multi chunk");
}

CASE("EXTRACT v4 reply (empty) round-trips and matches golden") {
    eckit::Buffer buffer(1024);
    std::string hash = hashOfEncoded([&](eckit::Stream& s) { Protocol::encodeExtractReplyEnd(s, {}); }, buffer);

    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    auto results = Protocol::decodeExtractReplyStreaming(in, 0);
    EXPECT_EQUAL(results.size(), 0ul);

    expectGolden(hash, "c519ce71fd6991e837163772fc44b0ac", "EXTRACT v4 reply empty");
}

CASE("EXTRACT v4 reply (error trailer after partial results) matches golden and throws") {
    ExtractionResult res0                                        = fixtureResult();
    std::vector<std::pair<size_t, const ExtractionResult*>> batch = {{0, &res0}};
    std::vector<std::string> errors                              = {"boom: something failed"};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeExtractResultChunk(s, batch);
            Protocol::encodeExtractReplyEnd(s, errors);
        },
        buffer);

    // Default raise=true: the trailer errors throw, exactly like the v3 leading
    // error block.
    eckit::ResizableMemoryStream in(buffer);
    in.rewind();
    EXPECT_THROWS_AS(Protocol::decodeExtractReplyStreaming(in, 2), eckit::RemoteException);

    // raise=false: partial results returned, missing indices left null.
    eckit::ResizableMemoryStream in2(buffer);
    in2.rewind();
    auto results = Protocol::decodeExtractReplyStreaming(in2, 2, false);
    EXPECT(results[0] != nullptr);
    EXPECT(results[1] == nullptr);

    expectGolden(hash, "6a708b11665a886671f7618fddcd9144", "EXTRACT v4 reply error trailer");
}

CASE("AXES reply frame matches golden") {
    // The real server stores axis values in an unordered_set, so the on-the-wire
    // order of values within an axis is not guaranteed across platforms. To pin a
    // deterministic golden while still driving the production encoder, use a
    // single value per axis (axis *names* come out in std::map key order). Layer 2
    // asserts multi-value content order-insensitively against the real server.
    std::map<std::string, std::unordered_set<std::string>> axes = {
        {"levtype", {"sfc"}},
        {"step", {"2"}},
    };

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeErrors(s, {});
            Protocol::encodeAxesReply(s, axes);
        },
        buffer);

    expectGolden(hash, "d908c9f30353463e17d9501135ea6754", "AXES reply");
}

CASE("SCAN reply frame matches golden") {
    eckit::Buffer buffer(1024);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeErrors(s, {});
            Protocol::encodeScanReply(s, 3);
        },
        buffer);

    expectGolden(hash, "a1905c25d868591f286286f6b7f32f18", "SCAN reply");
}

CASE("FORWARD_SCAN reply frame matches golden") {
    eckit::Buffer buffer(1024);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeErrors(s, {});
            Protocol::encodeScanReply(s, 3);
        },
        buffer);

    expectGolden(hash, "a1905c25d868591f286286f6b7f32f18", "FORWARD_SCAN reply");
}

CASE("FORWARD_EXTRACT reply frame matches golden") {
    auto item = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(fixtureRequest0()));
    item->URI(eckit::URI("file", eckit::PathName("/data/file.grib")));
    item->result(std::make_unique<ExtractionResult>(fixtureResult()));

    filemap_t filemap;
    filemap["/data/file.grib"] = {item.get()};

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            Protocol::encodeErrors(s, {});
            Protocol::encodeForwardExtractReply(s, filemap);
        },
        buffer);

    expectGolden(hash, "2e7c0a83cbc4090adc5b0d8f1149a377", "FORWARD_EXTRACT reply");
}

CASE("Error reply block matches golden") {
    // The failure path: server encodes nErrors followed by the messages.
    std::vector<std::string> errors = {"boom: something failed", "and another"};

    eckit::Buffer buffer(2048);
    std::string hash = hashOfEncoded([&](eckit::Stream& s) { Protocol::encodeErrors(s, errors); }, buffer);

    expectGolden(hash, "2d75195a420c1d50665d85da18fdbfe7", "error reply block");
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
