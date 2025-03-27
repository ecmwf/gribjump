/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include <future>
#include "eckit/container/Queue.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/message/Message.h"
#include "gribjump/info/InfoExtractor.h"

namespace fdb5 {
class FieldLocation;
}

namespace gribjump {

using Key = std::string;

// NB: This object creates a consumer thread (in ctor) and accepts futures from producers (add(), flush()).
// Note that it *is not* safe to have multiple producers calling add() and/or flush() concurrently.
class InfoAggregator {
    using locPair = std::pair<std::future<std::shared_ptr<const fdb5::FieldLocation>>, std::unique_ptr<JumpInfo>>;

public:

    InfoAggregator();
    ~InfoAggregator();

    void add(std::future<std::shared_ptr<const fdb5::FieldLocation>> future, eckit::MemoryHandle& handle,
             eckit::Offset offset);

    void flush();

private:

    void insert(const eckit::URI& uri, std::unique_ptr<JumpInfo> info);
    void close();

private:

    InfoExtractor extractor_;
    std::map<std::string, size_t> count_;
    eckit::Queue<locPair> futures_;
    std::thread consumer_;
};

// Simpler aggregator which does not create a consumer thread. Instead, it blocks in add() while waiting for the future.
class SerialAggregator {
public:

    SerialAggregator();
    ~SerialAggregator();

    void add(std::future<std::shared_ptr<const fdb5::FieldLocation>> future, eckit::MemoryHandle& handle,
             eckit::Offset offset);
    void flush();

private:

    void insert(const eckit::URI& uri, std::unique_ptr<JumpInfo> info);

private:

    InfoExtractor extractor_;
    std::map<std::string, size_t> count_;
};
}  // namespace gribjump