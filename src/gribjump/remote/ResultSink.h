/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "eckit/serialisation/Stream.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/remote/Protocol.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

/// The seam between engine and the wire used for streaming results. Abstracted to make it mockable by tests.
class ResultSink {
public:

    virtual ~ResultSink() = default;

    /// Encode and send one chunk carrying a batch of (requestIndex, result) pairs.
    virtual void writeResults(const std::vector<std::pair<size_t, const ExtractionResult*>>& batch) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

/// The production sink: encodes each batch as a v4 RESULTS chunk straight onto the client stream.
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
