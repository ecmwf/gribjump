/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// The seam between the streaming (v4) engine and the wire: as results become
/// available the engine hands batches to a ResultSink, which encodes and sends
/// one RESULTS chunk per batch. The engine owns the batching/byte-budget policy
/// and frees results after a batch is sent; the sink only encodes. This keeps
/// the sink mockable while the same code path runs over a real socket via
/// StreamResultSink.

#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "eckit/serialisation/Stream.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/remote/Protocol.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class ResultSink {
public:

    virtual ~ResultSink() = default;

    /// Encode and send one RESULTS chunk carrying a batch of
    /// (requestIndex, result) pairs. Called once per batch, possibly many times
    /// per request and in any completion order.
    virtual void writeResults(const std::vector<std::pair<size_t, const ExtractionResult*>>& batch) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

/// The production sink: encodes each batch as a v4 RESULTS chunk straight onto
/// the client stream. The terminating END chunk + error trailer are written by
/// the request handler once the stream is drained, not here.
class StreamResultSink : public ResultSink {
public:

    explicit StreamResultSink(eckit::Stream& stream) : stream_(stream) {}

    void writeResults(const std::vector<std::pair<size_t, const ExtractionResult*>>& batch) override {
        if (batch.empty()) {
            return;
        }
        Protocol::encodeExtractResultChunk(stream_, batch);
    }

private:

    eckit::Stream& stream_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
