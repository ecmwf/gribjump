/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <functional>
#include <future>
#include <thread>

#include "eckit/net/TCPStream.h"
#include "eckit/testing/Test.h"
#include "gribjump/GribJump.h"
#include "gribjump/remote/GribJumpUser.h"
#include "gribjump/remote/ServerStatus.h"
#include "protocol_test_helpers.h"

using namespace eckit::testing;
using namespace std::chrono_literals;

namespace gribjump::test {
namespace {

// Ephemeral TCP peer; no external server/data/config needed.
class Peer {
public:

    explicit Peer(std::function<void(eckit::Stream&)> serve, bool listen = true) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT(fd_ >= 0);
        sockaddr_in address{};
        address.sin_family      = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        socklen_t length = sizeof(address);
        ASSERT(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        port_ = ntohs(address.sin_port);
        // A bound but non-listening socket reserves a reliably unreachable port.
        if (!listen)
            return;
        ASSERT(::listen(fd_, 8) == 0);
        thread_ = std::thread([this, serve] {
            for (;;) {
                int connected = ::accept(fd_, nullptr, nullptr);
                if (connected < 0)
                    return;
                FdSocket socket(connected);
                eckit::net::InstantTCPStream stream(socket);
                try {
                    serve(stream);
                }
                catch (...) {
                    error_ = std::current_exception();
                }
            }
        });
    }
    ~Peer() {
        ::shutdown(fd_, SHUT_RDWR);
        if (thread_.joinable())
            thread_.join();
        ::close(fd_);
    }
    std::string endpoint() const { return "127.0.0.1:" + std::to_string(port_); }
    void check() {
        ::shutdown(fd_, SHUT_RDWR);
        if (thread_.joinable())
            thread_.join();
        if (error_)
            std::rethrow_exception(error_);
    }

private:

    int fd_, port_;
    std::thread thread_;
    std::exception_ptr error_;
};

eckit::Value snapshot() {
    return eckit::JSONParser::decodeString(ServerStatus::instance().json());
}
eckit::Value requestStatus() {
    return snapshot()["active_requests"][0];
}

class TestTask : public Task {
public:

    TestTask(TaskGroup& group, size_t id, std::function<void()> run) : Task(group, id), run_(std::move(run)) {}
    void executeImpl() override { run_(); }
    void info() const override {}

private:

    std::function<void()> run_;
};

struct Release {
    std::promise<void>& promise;
    bool released = false;
    void release() {
        if (!released) {
            promise.set_value();
            released = true;
        }
    }
    ~Release() { release(); }
};

}  // namespace

CASE("status requires configured servers and a finite positive timeout") {
    GribJump gj(Config{});
    EXPECT_THROWS_AS(gj.status(), eckit::UserError);
    EXPECT_THROWS_AS(gj.status(0), eckit::BadValue);
    EXPECT_THROWS_AS(gj.status(-1), eckit::BadValue);
    EXPECT_THROWS_AS(gj.status(std::numeric_limits<double>::infinity()), eckit::BadValue);
}

CASE("status v4 bypasses engine construction; v3 receives an explicit error") {
    for (uint16_t version : {remoteProtocolVersion, streamingProtocolVersion}) {
        DuplexTestStream stream(
            encodeRequest([&](eckit::Stream& out) { writeHeaderVersion(out, version, RequestType::STATUS); }));
        dispatchRequest(stream);  // deliberately no engine injected
        DuplexTestStream reply(stream.written());
        if (version == remoteProtocolVersion) {
            EXPECT_THROWS_AS(Protocol::decodeErrors(reply), eckit::RemoteException);
        }
        else {
            EXPECT(!Protocol::decodeErrors(reply));
            auto document = eckit::JSONParser::decodeString(Protocol::decodeStatusReply(reply));
            EXPECT_EQUAL(document["schema_version"].as<long long>(), 1);
            EXPECT_EQUAL(document["active_requests"].size(), 0);
        }
    }
}

CASE("status aggregates uri and servermap once each, retaining opaque JSON and partial errors") {
    const std::string payload =
        R"({"future_section":{"arbitrary":[1,"two",null]},"message":"quotes \" and newline\n"})";
    Peer good([&](eckit::Stream& stream) {
        auto header = Protocol::readRequestHeader(stream);
        EXPECT_EQUAL(header.version.value, streamingProtocolVersion);
        EXPECT(header.type == RequestType::STATUS);
        Protocol::encodeErrors(stream, {});
        Protocol::encodeStatusReply(stream, payload);
    });
    Peer old([](eckit::Stream& stream) {
        uint16_t version;
        stream >> version;  // released v3 servers reject before reading the rest
        if (version == remoteProtocolVersion) {
            LogContext context(stream);
            uint16_t type;
            stream >> type;
            eckit::SeriousBug error("Unknown request type: 5");
            stream << static_cast<const std::exception&>(error);
        }
        else {
            EXPECT_EQUAL(version, streamingProtocolVersion);
            eckit::SeriousBug error(
                "Gribjump remote-protocol mismatch: Serverside supports version: 3, Clientside version: 4");
            stream << static_cast<const std::exception&>(error);
        }
    });
    Peer earlyV4([](eckit::Stream& stream) {
        Protocol::readRequestHeader(stream);
        eckit::SeriousBug error("Unknown request type: 5");
        stream << static_cast<const std::exception&>(error);
    });
    Peer unreachable([](eckit::Stream&) {}, false);
    Peer stalled([](eckit::Stream& stream) {
        Protocol::readRequestHeader(stream);
        std::this_thread::sleep_for(350ms);
    });
    Peer malformed([](eckit::Stream& stream) {
        Protocol::readRequestHeader(stream);
        Protocol::encodeErrors(stream, {});
        Protocol::encodeStatusReply(stream, "not json");
    });

    Config config;
    config.set("type", "remote");
    config.set("uri", good.endpoint());
    config.set("clientProtocolVersion", 3);  // status independently uses v4
    std::vector<eckit::LocalConfiguration> map;
    for (const auto* peer : {&good, &old, &earlyV4, &unreachable, &stalled, &malformed, &good}) {
        eckit::LocalConfiguration entry;
        entry.set("fdb", "catalogue:" + std::to_string(1000 + map.size()));
        entry.set("gribjump", peer->endpoint());
        map.push_back(entry);
    }
    config.set("servermap", map);
    auto start    = std::chrono::steady_clock::now();
    auto document = eckit::JSONParser::decodeString(GribJump(config).status(0.15));
    EXPECT(std::chrono::steady_clock::now() - start < 2s);
    EXPECT_EQUAL(document["servers"].size(), 6);
    std::map<std::string, eckit::Value> replies;
    for (const auto& server : document["servers"].as<eckit::ValueList>())
        replies.emplace(server["endpoint"].as<std::string>(), server);
    EXPECT_EQUAL(replies.at(good.endpoint())["state"].as<std::string>(), "ok");
    EXPECT_EQUAL(replies.at(good.endpoint())["status"]["future_section"]["arbitrary"].size(), 3);
    for (const auto* peer : {&old, &earlyV4}) {
        EXPECT_EQUAL(replies.at(peer->endpoint())["state"].as<std::string>(), "unsupported");
        EXPECT(replies.at(peer->endpoint())["error"].as<std::string>().find("gribjump.status unsupported") !=
               std::string::npos);
    }
    for (const auto* peer : {&unreachable, &stalled})
        EXPECT_EQUAL(replies.at(peer->endpoint())["state"].as<std::string>(), "unreachable");
    EXPECT_EQUAL(replies.at(malformed.endpoint())["state"].as<std::string>(), "error");
    good.check();
    old.check();
    earlyV4.check();
    stalled.check();
    malformed.check();
}

CASE("status sees long-running real handler/tasks even with every worker busy") {
    std::promise<void> started, release;
    auto released = release.get_future().share();
    struct BlockingEngine : MockEngine {
        std::promise<void>& started;
        std::shared_future<void> release;
        BlockingEngine(std::promise<void>& s, std::shared_future<void> r) : started(s), release(r) {}
        TaskOutcome<size_t> scan(const MarsRequests&, bool) override {
            TaskGroup group;
            group.enqueueTask<TestTask>([&] {
                started.set_value();
                release.wait();
            });
            group.enqueueTask<TestTask>([] {});
            group.waitForTasks();
            return {2, group.report()};
        }
    } engine(started, released);
    auto active = std::async(std::launch::async, [&] {
        DuplexTestStream stream(encodeRequest([](eckit::Stream& out) {
            writeHeaderVersion(out, streamingProtocolVersion, RequestType::SCAN,
                               R"({"job":"long-running","nested":{"quoted":"a\"b"}})");
            Protocol::encodeScanRequest(out, {}, false);
        }));
        dispatchRequest(stream, &engine);
    });
    Release cleanup{release};
    EXPECT(started.get_future().wait_for(2s) == std::future_status::ready);
    // Wait for the handler to finish submission and enter its task wait.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (requestStatus()["phase"].as<std::string>() != "waiting_for_tasks" &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();

    Peer peer([](eckit::Stream& stream) { dispatchRequest(stream); });
    Config config;
    // A local backend with servermap alone also supports status.
    eckit::LocalConfiguration entry;
    entry.set("mars", "catalogue:1234");
    entry.set("gribjump", peer.endpoint());
    config.set("servermap", std::vector<eckit::LocalConfiguration>{entry});
    auto document = eckit::JSONParser::decodeString(GribJump(config).status(1));
    auto status   = document["servers"][0]["status"];
    auto request  = status["active_requests"][0];
    cleanup.release();
    active.get();
    peer.check();

    EXPECT_EQUAL(status["active_requests"].size(), 1);  // excludes status itself
    EXPECT_EQUAL(status["threads"]["worker_count"].as<long long>(), 1);
    EXPECT_EQUAL(status["threads"]["busy"].as<long long>(), 1);
    EXPECT_EQUAL(request["operation"].as<std::string>(), "scan");
    EXPECT_EQUAL(request["phase"].as<std::string>(), "waiting_for_tasks");
    EXPECT(request["elapsed_seconds"].as<double>() > 0);
    EXPECT_EQUAL(request["context"]["job"].as<std::string>(), "long-running");
    EXPECT_EQUAL(request["context"]["nested"]["quoted"].as<std::string>(), "a\"b");
    EXPECT_EQUAL(request["tasks"]["total"].as<long long>(), 2);
    EXPECT_EQUAL(request["tasks"]["running"].as<long long>(), 1);
    EXPECT_EQUAL(request["tasks"]["queued"].as<long long>(), 1);
    EXPECT_EQUAL(status["threads"]["workers"][0]["request_id"].as<long long>(), request["id"].as<long long>());
    EXPECT_EQUAL(snapshot()["active_requests"].size(), 0);
}

CASE("request counts survive task group destruction and distinguish completion failure cancellation") {
    {
        StatusRequestScope scope(RequestType::EXTRACT, LogContext("{\"job\":42}"));
        {
            TaskGroup success;
            success.enqueueTask<TestTask>([] {});
            success.waitForTasks();
        }
        {
            TaskGroup failed;
            // Keep pending tasks queued until the whole group is submitted.
            std::promise<void> release;
            auto ready = release.get_future().share();
            failed.enqueueTask<TestTask>([ready] {
                ready.wait();
                throw std::runtime_error("test failure");
            });
            failed.enqueueTask<TestTask>([] {});
            release.set_value();
            failed.waitForTasks();
            failed.report();  // moving errors out must not erase monitoring counts
        }
        auto counts = requestStatus()["tasks"];
        EXPECT_EQUAL(counts["total"].as<long long>(), 3);
        EXPECT_EQUAL(counts["completed"].as<long long>(), 1);
        EXPECT_EQUAL(counts["failed"].as<long long>(), 1);
        EXPECT_EQUAL(counts["cancelled"].as<long long>(), 1);
        EXPECT_EQUAL(counts["running"].as<long long>(), 0);
        EXPECT_EQUAL(counts["queued"].as<long long>(), 0);
    }
    EXPECT_EQUAL(snapshot()["active_requests"].size(), 0);
    EXPECT(!ServerStatus::current());
}

CASE("handler phases cover body receive and reply, including reply disconnect cleanup") {
    class ObservingStream : public DuplexTestStream {
    public:

        ObservingStream(std::vector<char> bytes, bool fail) : DuplexTestStream(std::move(bytes)), fail_(fail) {}
        long read(void* buffer, long size) override {
            if (snapshot()["active_requests"].size()) {
                EXPECT_EQUAL(requestStatus()["phase"].as<std::string>(), "receiving");
                received = true;
            }
            return DuplexTestStream::read(buffer, size);
        }
        long write(const void* buffer, long size) override {
            EXPECT_EQUAL(requestStatus()["phase"].as<std::string>(), "sending_results");
            sent = true;
            if (fail_)
                throw std::runtime_error("client disconnected");
            return DuplexTestStream::write(buffer, size);
        }
        bool received = false, sent = false;

    private:

        bool fail_;
    };
    for (bool fail : {false, true}) {
        ObservingStream stream(encodeRequest([](eckit::Stream& out) {
                                   writeHeaderVersion(out, streamingProtocolVersion, RequestType::SCAN);
                                   Protocol::encodeScanRequest(out, {}, false);
                               }),
                               fail);
        MockEngine engine;
        if (fail)
            EXPECT_THROWS(dispatchRequest(stream, &engine));
        else
            dispatchRequest(stream, &engine);
        EXPECT(stream.received);
        EXPECT(stream.sent);
        EXPECT_EQUAL(snapshot()["active_requests"].size(), 0);
        EXPECT(!ServerStatus::current());
    }
}

CASE("handler receive and execute exceptions remove requests from status") {
    DuplexTestStream invalid(encodeRequest([](eckit::Stream& stream) {
        writeHeaderVersion(stream, streamingProtocolVersion, RequestType::SCAN);
        // Deliberately missing the request body.
    }));
    MockEngine engine;
    EXPECT_THROWS(dispatchRequest(invalid, &engine));
    EXPECT_EQUAL(snapshot()["active_requests"].size(), 0);
    EXPECT(!ServerStatus::current());

    struct ThrowingEngine : MockEngine {
        TaskOutcome<size_t> scan(const MarsRequests&, bool) override { throw std::runtime_error("execute failed"); }
    } throwing;
    DuplexTestStream stream(encodeRequest([](eckit::Stream& out) {
        writeHeaderVersion(out, streamingProtocolVersion, RequestType::SCAN);
        Protocol::encodeScanRequest(out, {}, false);
    }));
    EXPECT_THROWS(dispatchRequest(stream, &throwing));
    EXPECT_EQUAL(snapshot()["active_requests"].size(), 0);
    EXPECT(!ServerStatus::current());
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
