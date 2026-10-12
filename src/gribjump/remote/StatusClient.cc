/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#include "gribjump/remote/StatusClient.h"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <set>
#include <sstream>
#include <thread>

#include "gribjump/remote/RemoteGribJump.h"

namespace gribjump {
namespace {

class StatusUnreachable : public eckit::Exception {
public:

    explicit StatusUnreachable(const std::string& reason) : eckit::Exception(reason) {}
};

class StatusDisconnected : public StatusUnreachable {
public:

    using StatusUnreachable::StatusUnreachable;
};

struct Socket {
    int fd = -1;
    ~Socket() { reset(); }
    void reset(int value = -1) {
        if (fd >= 0)
            ::close(fd);
        fd = value;
    }
};

// The ordinary TCPClient's timeout uses process-global SIGALRM and its retry
// loop can sleep for minutes. Status uses a separate, nonblocking transport:
// no signals, connection retry loops, detached threads, or changes to extraction timeouts.
class StatusConnection : public ClientConnection, public eckit::Stream {
public:

    StatusConnection(const std::string& host, int port, double timeout) {
        addrinfo hints{};
        hints.ai_socktype   = SOCK_STREAM;
        hints.ai_family     = AF_UNSPEC;
        addrinfo* addresses = nullptr;
        int error           = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &addresses);
        if (error)
            throw StatusUnreachable("Cannot resolve " + host + ": " + gai_strerror(error));
        std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> owned(addresses, freeaddrinfo);
        deadline_ = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(static_cast<long long>(std::ceil(timeout * 1000)));
        std::string reason = "No usable address";
        for (auto* address = addresses; address; address = address->ai_next) {
            socket_.reset(::socket(address->ai_family, address->ai_socktype, address->ai_protocol));
            if (socket_.fd < 0) {
                reason = std::strerror(errno);
                continue;
            }
            if (::fcntl(socket_.fd, F_SETFL, O_NONBLOCK) < 0) {
                reason = std::strerror(errno);
                continue;
            }
            // Avoid SIGPIPE on platforms without MSG_NOSIGNAL.
#ifdef SO_NOSIGPIPE
            int yes = 1;
            ::setsockopt(socket_.fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
#endif
            if (::connect(socket_.fd, address->ai_addr, address->ai_addrlen) == 0)
                return;
            if (errno == EINPROGRESS) {
                wait(POLLOUT);
                int result       = 0;
                socklen_t length = sizeof(result);
                if (::getsockopt(socket_.fd, SOL_SOCKET, SO_ERROR, &result, &length) < 0)
                    result = errno;
                if (!result)
                    return;
                reason = std::strerror(result);
            }
            else
                reason = std::strerror(errno);
        }
        throw StatusUnreachable("Cannot connect: " + reason);
    }
    eckit::Stream& stream() override { return *this; }

private:

    void wait(short events) {
        for (;;) {
            auto remaining = deadline_ - std::chrono::steady_clock::now();
            if (remaining <= std::chrono::steady_clock::duration::zero())
                throw StatusUnreachable("Status query timed out");
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
            pollfd descriptor{socket_.fd, events, 0};
            int result = ::poll(&descriptor, 1, static_cast<int>(ms + 1));
            if (result > 0)
                return;  // read/write/getsockopt diagnoses EOF and socket errors
            if (result < 0 && errno != EINTR)
                throw StatusUnreachable(std::strerror(errno));
        }
    }
    long read(void* buffer, long length) override {
        flushRequest();
        long done = 0;
        while (done < length) {
            wait(POLLIN);
            auto count = ::recv(socket_.fd, static_cast<char*>(buffer) + done, length - done, 0);
            if (count > 0)
                done += count;
            else if (!count)
                throw StatusDisconnected("Server closed the status connection");
            else if (errno == ECONNRESET)
                throw StatusDisconnected(std::strerror(errno));
            else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                throw StatusUnreachable(std::strerror(errno));
        }
        return done;
    }
    long write(const void* buffer, long length) override {
        // Send the small header together on the first read, rather than one
        // syscall per eckit type tag. Older servers may reject the very first
        // version field; avoid continuing tiny writes after that rejection.
        request_.append(static_cast<const char*>(buffer), length);
        return length;
    }
    void flushRequest() {
        size_t done = 0;
        while (done < request_.size()) {
            wait(POLLOUT);
#ifdef MSG_NOSIGNAL
            constexpr int flags = MSG_NOSIGNAL;
#else
            constexpr int flags = 0;
#endif
            auto count = ::send(socket_.fd, request_.data() + done, request_.size() - done, flags);
            if (count > 0)
                done += count;
            else if (!count)
                throw StatusUnreachable("Server closed the status connection");
            else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                throw StatusUnreachable(std::strerror(errno));
        }
        request_.clear();
    }
    std::string name() const override { return "gribjump.status"; }
    std::string request_;
    Socket socket_;
    std::chrono::steady_clock::time_point deadline_;
};

class StatusTransport : public ClientTransport {
public:

    StatusTransport(std::string host, int port, double timeout) :
        host_(std::move(host)), port_(port), timeout_(timeout) {}
    std::unique_ptr<ClientConnection> connect() override {
        return std::make_unique<StatusConnection>(host_, port_, timeout_);
    }

private:

    std::string host_;
    int port_;
    double timeout_;
};
std::string queryStatus(const std::string& host, int port, double timeout) {
    const auto started = std::chrono::steady_clock::now();
    RemoteGribJump remote(std::make_unique<StatusTransport>(host, port, timeout), streamingProtocolVersion);
    try {
        return remote.status();
    }
    catch (const StatusDisconnected&) {
        // A v3 server rejects the first version field and closes with unread
        // header bytes. TCP may reset before its exception reaches us. Probe
        // the unknown opcode with a v3 header so it can consume the full header
        // and return its error cleanly. Never infer unsupported from a reset
        // alone, or from a new server's explicit "requires v4" response.
        const double remaining =
            timeout - std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        if (remaining > 0) {
            bool unsupported = false;
            std::string reason;
            try {
                StatusConnection probe(host, port, remaining);
                auto& stream = probe.stream();
                Protocol::writeRequestHeader(stream, RequestType::STATUS, ContextManager::instance().context(),
                                             remoteProtocolVersion);
                Protocol::decodeErrors(stream);
            }
            catch (const eckit::RemoteException& e) {
                reason      = e.what();
                unsupported = reason.find("Unknown request type") != std::string::npos;
            }
            catch (...) {
            }  // Preserve the original connection failure.
            if (unsupported)
                throw StatusUnsupported(reason);
        }
        throw;
    }
}

struct Reply {
    std::string state;
    std::string payload;
};
}  // namespace

std::string remoteStatus(const ConfigOptions& options, double timeoutSeconds, const LogContext& context) {
    if (!std::isfinite(timeoutSeconds) || timeoutSeconds <= 0 || timeoutSeconds > 86400) {
        throw eckit::BadValue("status timeout must be positive and at most 86400 seconds");
    }
    std::set<std::pair<std::string, int>> unique;
    const auto add = [&](const eckit::net::Endpoint& endpoint) { unique.emplace(endpoint.host(), endpoint.port()); };
    if (!options.remoteURI().empty())
        add(eckit::net::Endpoint(options.remoteURI()));
    for (const auto& entry : options.serverMap())
        add(entry.second);
    if (unique.empty())
        throw eckit::UserError("gribjump.status requires remote servers configured via uri or servermap", Here());

    const std::vector<std::pair<std::string, int>> endpoints(unique.begin(), unique.end());
    std::vector<Reply> replies(endpoints.size());
    std::atomic<size_t> next{0};
    const auto query = [&] {
        for (size_t i; (i = next.fetch_add(1)) < endpoints.size();) {
            try {
                ContextManager::instance().set(context);
                const auto& [host, port] = endpoints[i];
                auto payload             = queryStatus(host, port, timeoutSeconds);
                // Validate only JSON syntax, never depend on diagnostic fields.
                eckit::JSONParser::decodeString(payload);
                replies[i] = {"ok", std::move(payload)};
            }
            catch (const StatusUnsupported& e) {
                replies[i] = {"unsupported", e.what()};
            }
            catch (const StatusUnreachable& e) {
                replies[i] = {"unreachable", e.what()};
            }
            catch (const std::exception& e) {
                replies[i] = {"error", e.what()};
            }
            catch (...) {
                replies[i] = {"error", "Unknown status query failure"};
            }
        }
    };
    std::vector<std::thread> threads;
    try {
        for (size_t i = 0; i < std::min<size_t>(8, endpoints.size()); ++i)
            threads.emplace_back(query);
    }
    catch (...) {
        for (auto& thread : threads)
            thread.join();
        throw;
    }
    for (auto& thread : threads)
        thread.join();

    std::ostringstream out;
    eckit::JSON json(out);
    json.startObject();
    json << "schema_version" << 1 << "servers";
    json.startList();
    for (size_t i = 0; i < endpoints.size(); ++i) {
        json.startObject();
        json << "endpoint" << endpoints[i].first + ":" + std::to_string(endpoints[i].second);
        json << "state" << replies[i].state;
        if (replies[i].state == "ok")
            json << "status" << eckit::JSONParser::decodeString(replies[i].payload);
        else
            json << "error" << replies[i].payload;
        json.endObject();
    }
    json.endList();
    json.endObject();
    return out.str();
}
}  // namespace gribjump
