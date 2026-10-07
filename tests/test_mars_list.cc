/*
 * (C) Copyright 2026- ECMWF.
 * This software is licensed under the terms of the Apache Licence Version 2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <exception>
#include <thread>

#include "dhskit/ListAggregation.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPStream.h"
#include "eckit/testing/Test.h"
#include "gribjump/GribJump.h"

using namespace eckit::testing;
using namespace gribjump;

namespace {

// Implements the existing MARS lister v1 wire contract, without a MARS archive.
class MockMarsServer {
public:

    explicit MockMarsServer(dhskit::ListAggregation response, std::vector<std::string> errors = {}) : server_(0) {
        server_.socket();
        worker_ = std::thread([this, response = std::move(response), errors = std::move(errors)] {
            try {
                eckit::net::TCPSocket socket(server_.accept("mock MARS list", 10));
                eckit::net::InstantTCPStream stream(socket);
                uint16_t version, operation;
                stream >> version;
                stream >> operation;
                if (version != 1 || operation != 0) {
                    throw eckit::BadValue("Unexpected MARS list header");
                }
                request_ = metkit::mars::MarsRequest(stream);
                stream << errors.size();
                for (const auto& error : errors) {
                    stream << error;
                }
                if (errors.empty()) {
                    stream << response;
                }
            }
            catch (...) {
                error_ = std::current_exception();
            }
        });
    }

    ~MockMarsServer() {
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    Config config() const {
        Config config;
        config.set("type", "remote");
        config.set("uri", "localhost:1");  // extraction server is independent
        config.set("lister.type", "mars");
        config.set("lister.uri", "localhost:" + std::to_string(server_.localPort()));
        return config;
    }

    const metkit::mars::MarsRequest& request() {
        worker_.join();
        if (error_) {
            std::rethrow_exception(error_);
        }
        return request_;
    }

private:

    eckit::net::EphemeralTCPServer server_;
    std::thread worker_;
    std::exception_ptr error_;
    metkit::mars::MarsRequest request_;
};

}  // namespace

CASE("MARS listing decodes full field metadata and remote locations") {
    dhskit::ListAggregation response;
    auto& first = response.addShape({{"class", "od"}, {"date", "2025-11-30"}});
    first.files = {"marsfs://store-a/data/fields.grib", "marsfs://store-b/data/other.grib"};
    first.fields.push_back({{{"param", "130.128"}, {"levelist", "300"}}, 0, eckit::Offset(0), eckit::Length(100)});
    first.fields.push_back({{{"levelist", "400"}}, 1, eckit::Offset(4294967296LL), eckit::Length(200)});
    response.addShape({{"date", "2025-12-01"}});  // empty shapes are legal
    auto& last = response.addShape({{"date", "2025-12-02"}});
    last.files = {"/data/local.grib"};
    last.fields.push_back({{{"param", "131.128"}}, 0, eckit::Offset(42), eckit::Length(300)});

    MockMarsServer server(std::move(response));
    metkit::mars::MarsRequest selection("list");
    selection.values("param", {"130.128", "131.128"});
    // One sparse selector is sent unchanged; no defaults or minimum keys.
    auto iterator = GribJump(server.config()).list(selection);
    EXPECT_EQUAL(server.request().asString(), selection.asString());
    auto a = iterator.next();
    auto b = iterator.next();
    auto c = iterator.next();
    EXPECT(a && b && c);
    EXPECT(!iterator.next());
    EXPECT_EQUAL(a->uri().host(), "store-a");
    EXPECT_EQUAL(a->uri().path(), "/data/fields.grib");
    EXPECT_EQUAL(a->uri().scheme(), "file");
    EXPECT_EQUAL(a->uri().port(), 0);
    EXPECT_EQUAL(a->metadata().at("date"), "2025-11-30");
    EXPECT_EQUAL(b->uri().host(), "store-b");
    EXPECT_EQUAL(static_cast<long long>(b->offset()), 4294967296LL);
    EXPECT_EQUAL(static_cast<long long>(b->length()), 200);
    EXPECT_EQUAL(b->metadata().at("param"), "130.128");
    EXPECT_EQUAL(b->metadata().at("levelist"), "400");
    EXPECT_EQUAL(c->metadata().count("levelist"), 0);
    EXPECT_EQUAL(c->metadata().count("class"), 0);
    EXPECT_EQUAL(c->metadata().at("param"), "131.128");
    auto path = b->extractionRequest({{0, 10}});
    EXPECT_EQUAL(path.host(), "store-b");
    EXPECT_EQUAL(path.path(), "/data/other.grib");
    EXPECT_EQUAL(path.offset(), 4294967296ULL);
}

CASE("MARS listing empty results and server errors") {
    metkit::mars::MarsRequest selection("list");
    {
        MockMarsServer server({});
        EXPECT(!GribJump(server.config()).list(selection).next());
        EXPECT(server.request().empty());
    }
    {
        MockMarsServer server({}, {"invalid selection", "archive unavailable"});
        EXPECT_THROWS_AS(GribJump(server.config()).list(selection), eckit::RemoteException);
        EXPECT(server.request().empty());
    }
}

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
