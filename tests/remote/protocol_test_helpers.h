/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// Shared helpers for the remote-protocol tests: a mock engine, a duplex
/// in-memory stream, and small encoding utilities.

#pragma once

#include <algorithm>
#include <bitset>
#include <cstring>
#include <string>
#include <vector>

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/ResizableMemoryStream.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/utils/Literals.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Metrics.h"
#include "gribjump/Task.h"
#include "gribjump/remote/Protocol.h"

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// A mock engine returning deterministic, canned results. Records the inputs it
// was called with so tests can assert the server decoded the request correctly.

class MockEngine : public EngineIface {
public:

    // Canned result content, reused for every extracted field.
    static ExtractionResult cannedResult() {
        std::vector<std::vector<double>> values        = {{10.0, 20.0}, {30.0}};
        std::vector<std::vector<std::bitset<64>>> mask = {{std::bitset<64>(0x3)}, {std::bitset<64>(0x1)}};
        return ExtractionResult(std::move(values), std::move(mask));
    }

    TaskOutcome<ResultsMap> extract(ExtractionRequests& requests) override {
        lastExtractRequests = requests.size();
        ResultsMap map;
        for (auto& req : requests) {
            auto item = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(req));
            item->result(std::make_unique<ExtractionResult>(cannedResult()));
            map.emplace(req.requestString(), std::move(item));
        }
        return {std::move(map), makeReport()};
    }

    TaskOutcome<size_t> scan(const MarsRequests& requests, bool byfiles) override {
        lastScanRequests = requests.size();
        lastByfiles      = byfiles;
        return {scanNFields, makeReport()};
    }

    TaskOutcome<size_t> scheduleScanTasks(const scanmap_t& scanmap) override {
        lastScanmapFiles   = scanmap.size();
        lastScanmapOffsets = 0;
        for (auto& [fname, offsets] : scanmap) {
            lastScanmapOffsets += offsets.size();
        }
        return {scanNFields, makeReport()};
    }

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) override {
        lastAxesRequest = request;
        lastAxesLevel   = level;
        return {{"step", {"1", "2", "3"}}, {"levtype", {"sfc"}}};
    }

    TaskReport scheduleExtractionTasks(filemap_t& filemap, bool forward) override {
        lastFilemapFiles = filemap.size();
        lastFilemapItems = 0;
        for (auto& [fname, items] : filemap) {
            lastFilemapItems += items.size();
            for (auto* item : items) {
                item->result(std::make_unique<ExtractionResult>(cannedResult()));
            }
        }
        return makeReport();
    }

    // Recorded inputs / configurable outputs
    size_t scanNFields         = 42;
    size_t lastExtractRequests = 0;
    size_t lastScanRequests    = 0;
    bool lastByfiles           = false;
    size_t lastScanmapFiles    = 0;
    size_t lastScanmapOffsets  = 0;
    size_t lastFilemapFiles    = 0;
    size_t lastFilemapItems    = 0;
    std::string lastAxesRequest;
    int lastAxesLevel = -1;

    // When non-empty, execute() reports these as server-side errors.
    std::vector<std::string> errors;

private:

    TaskReport makeReport() const {
        std::vector<std::string> copy = errors;
        return TaskReport(std::move(copy));
    }
};

//-----------------------------------------------------------------------------
// A minimal duplex, socket-like stream: reads consume a pre-filled request
// buffer; writes accumulate into a separate reply buffer. This mirrors the
// full-duplex TCP stream the server normally talks over, so a single
// dispatchRequest() call can read the request and write the reply.

class DuplexTestStream : public eckit::Stream {
public:

    explicit DuplexTestStream(std::vector<char> request) : readBuf_(std::move(request)) {}

    long read(void* out, long len) override {
        long avail = static_cast<long>(readBuf_.size()) - static_cast<long>(rpos_);
        long n     = std::min(len, avail);
        if (n > 0) {
            std::memcpy(out, readBuf_.data() + rpos_, n);
            rpos_ += n;
        }
        return n;
    }

    long write(const void* in, long len) override {
        const char* p = static_cast<const char*>(in);
        writeBuf_.insert(writeBuf_.end(), p, p + len);
        return len;
    }

    void rewind() override { rpos_ = 0; }
    std::string name() const override { return "DuplexTestStream"; }

    const std::vector<char>& written() const { return writeBuf_; }

private:

    std::vector<char> readBuf_;
    std::vector<char> writeBuf_;
    size_t rpos_ = 0;
};

//-----------------------------------------------------------------------------
// Small encoding utilities

/// Encode a request frame via a lambda, returning the raw bytes.
template <typename EncodeFn>
inline std::vector<char> encodeRequest(EncodeFn&& encode) {
    eckit::Buffer buffer(16 * 1024);
    eckit::ResizableMemoryStream s(buffer);
    encode(s);
    const char* p = static_cast<const char*>(buffer.data());
    return std::vector<char>(p, p + s.position());
}

/// Write the request header exactly as the client does.
inline void writeHeader(eckit::Stream& s, RequestType type, const std::string& ctx = "{}") {
    Protocol::writeRequestHeader(s, type, LogContext(ctx));
}

inline ExtractionRequest fixtureRequest(int step) {
    return ExtractionRequest("class=rd,expver=xxxx,step=" + std::to_string(step), {{0, 2}, {5, 6}},
                             "33c7d6025995e1b4913811e77d38ec50");
}

}  // namespace test
}  // namespace gribjump
