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

#include "eckit/filesystem/URI.h"
#include "eckit/message/Message.h"
#include "gribjump/info/InfoExtractor.h"


namespace gribjump {

using Key = std::string;

class InfoAggregator { // rename...

public:
    InfoAggregator() = default;

    // Extract the JumpInfo from a message.
    void add(const Key& key, eckit::DataHandle& handle);

    // Store the location provided by fdb callback
    void add(const Key& key, const eckit::URI& location);

    // Give infos to the cache and persist them.
    void flush();

private:

    // When both adds have been called, the second will call insert.
    void insert(const eckit::URI& uri, JumpInfo* info);

private:
    std::mutex mutex_; // Must lock before touching any maps

    // should be references or pointers, most likely.
    std::map<Key, eckit::URI> keyToLocation;
    std::map<Key, JumpInfo*> keyToJumpInfo;

    size_t count_ = 0; // Should always equal size of location_to_jumpinfo_

    InfoExtractor extractor_;
};
} // namespace gribjump