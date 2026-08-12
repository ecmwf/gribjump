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
/// These tests are deliberately FDB-free and socket-free: they drive the same
/// eckit::Stream encode/decode paths used by the real client and server against
/// an in-memory buffer. Their purpose is twofold:
///   1. round-trip: prove encode followed by decode reconstructs the object;
///   2. golden hash: pin the exact bytes on the wire so that any change to the
///      serialised format is detected and forces a deliberate protocol change.
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

#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/Metrics.h"
#include "gribjump/remote/RemoteGribJump.h"

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

/// Write the request header exactly as RemoteGribJump::sendHeader does.
static void writeHeader(eckit::Stream& s, RequestType type, const LogContext& ctx) {
    s << remoteProtocolVersion;
    s << ctx;
    s << static_cast<uint16_t>(type);
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
    EXPECT_EQUAL(remoteProtocolVersion, 3);
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
            writeHeader(s, RequestType::EXTRACT, LogContext("{}"));
            size_t n = requests.size();
            s << n;
            for (auto& r : requests) {
                s << r;
            }
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
            writeHeader(s, RequestType::AXES, LogContext("{}"));
            s << request;
            s << level;
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
            writeHeader(s, RequestType::SCAN, LogContext("{}"));
            s << byfiles;
            size_t n = requests.size();
            s << n;
            for (auto& r : requests) {
                s << r;
            }
        },
        buffer);

    expectGolden(hash, "6d728d928b5362ded48725a81bd32783", "SCAN frame");
}

CASE("FORWARD_SCAN request frame matches golden") {
    eckit::OffsetList offsets = {eckit::Offset(0), eckit::Offset(1024), eckit::Offset(2048)};

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeHeader(s, RequestType::FORWARD_SCAN, LogContext("{}"));
            size_t nFiles = 1;
            s << nFiles;
            std::string fname = "/data/file.grib";
            s << fname;
            s << offsets;
        },
        buffer);

    expectGolden(hash, "c04f95d146c8186e88ae74d533cbb657", "FORWARD_SCAN frame");
}

CASE("FORWARD_EXTRACT request frame matches golden") {
    ExtractionRequest req = fixtureRequest0();
    eckit::URI uri("file", eckit::PathName("/data/file.grib"));

    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeHeader(s, RequestType::FORWARD_EXTRACT, LogContext("{}"));
            size_t nFiles = 1;
            s << nFiles;
            std::string fname = "/data/file.grib";
            s << fname;
            size_t nItems = 1;
            s << nItems;
            s << req;
            s << uri;
        },
        buffer);

    expectGolden(hash, "9b18caa1229e7bb475793eac944b0400", "FORWARD_EXTRACT frame");
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

/// Error block for the success case: zero errors.
static void writeNoErrors(eckit::Stream& s) {
    size_t nErrors = 0;
    s << nErrors;
}

CASE("EXTRACT reply frame matches golden") {
    // Two results, each framed with the (currently mandatory) nfields == 1.
    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeNoErrors(s);
            for (int i = 0; i < 2; i++) {
                size_t nfields = 1;
                s << nfields;
                ExtractionResult res = fixtureResult();
                s << res;
            }
        },
        buffer);

    expectGolden(hash, "fbd78b1b117de6d51ddfc515f2bc8cdf", "EXTRACT reply");
}

CASE("AXES reply frame matches golden") {
    // Note: the real server stores axis values in an unordered_set, so the
    // on-the-wire ordering of values is not guaranteed. This golden pins the
    // encoding structure using a fixed order; Layer 2 asserts decoded content
    // order-insensitively against the real server.
    std::vector<std::pair<std::string, std::vector<std::string>>> axes = {
        {"step", {"1", "2", "3"}},
        {"levtype", {"sfc"}},
    };

    eckit::Buffer buffer(4096);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeNoErrors(s);
            size_t naxes = axes.size();
            s << naxes;
            for (auto& [name, vals] : axes) {
                s << name;
                size_t n = vals.size();
                s << n;
                for (auto& v : vals) {
                    s << v;
                }
            }
        },
        buffer);

    expectGolden(hash, "226399410eb200d0a2c56e306268bbc4", "AXES reply");
}

CASE("SCAN reply frame matches golden") {
    eckit::Buffer buffer(1024);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeNoErrors(s);
            size_t nFields = 3;
            s << nFields;
        },
        buffer);

    expectGolden(hash, "a1905c25d868591f286286f6b7f32f18", "SCAN reply");
}

CASE("FORWARD_SCAN reply frame matches golden") {
    eckit::Buffer buffer(1024);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeNoErrors(s);
            size_t nfields = 3;
            s << nfields;
        },
        buffer);

    expectGolden(hash, "a1905c25d868591f286286f6b7f32f18", "FORWARD_SCAN reply");
}

CASE("FORWARD_EXTRACT reply frame matches golden") {
    eckit::Buffer buffer(8192);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            writeNoErrors(s);
            std::string fname = "/data/file.grib";
            s << fname;
            size_t nItems = 1;
            s << nItems;
            ExtractionResult res = fixtureResult();
            s << res;
        },
        buffer);

    expectGolden(hash, "2e7c0a83cbc4090adc5b0d8f1149a377", "FORWARD_EXTRACT reply");
}

CASE("Error reply block matches golden") {
    // The failure path: server encodes nErrors followed by the messages.
    std::vector<std::string> errors = {"boom: something failed", "and another"};

    eckit::Buffer buffer(2048);
    std::string hash = hashOfEncoded(
        [&](eckit::Stream& s) {
            size_t n = errors.size();
            s << n;
            for (auto& e : errors) {
                s << e;
            }
        },
        buffer);

    expectGolden(hash, "2d75195a420c1d50665d85da18fdbfe7", "error reply block");
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
