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
#include "eckit/io/DataHandle.h"

#include "fdb5/database/FieldLocation.h"

#include "gribjump/info/InfoAggregator.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoCache.h"

namespace {
bool isGrib(eckit::DataHandle& handle) {
    char buffer[4];
    eckit::Offset offset = handle.position();
    handle.read(buffer, 4);
    handle.seek(offset);
    return buffer[0] == 'G' && buffer[1] == 'R' && buffer[2] == 'I' && buffer[3] == 'B';
}
} // namespace

namespace gribjump {

static const size_t AGGREGATOR_QUEUE_SIZE = 8;

InfoAggregator::InfoAggregator(): futures_(AGGREGATOR_QUEUE_SIZE) {
    consumer_ = std::thread([this]() {
        for (;;) {
            locPair elem;
            if (futures_.pop(elem) == -1) break;
            std::shared_ptr<fdb5::FieldLocation> location = elem.first.get();
            std::unique_ptr<JumpInfo> info = std::move(elem.second);
            insert(location->fullUri(), std::move(info));
            consumed_++;
            cv_.notify_one();
        }
    });
}

InfoAggregator::~InfoAggregator() {
    close();
}

void InfoAggregator::add(std::future<std::shared_ptr<fdb5::FieldLocation>> future, eckit::MemoryHandle& handle, eckit::Offset offset) {

    handle.openForRead();
    eckit::AutoClose closer(handle);

    if (!isGrib(handle)) {
        eckit::Log::warning() << "Warning: Gribjump InfoAggregator received non-grib message. Skipping..." << std::endl;
        return;
    }

    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(handle, offset));
    futures_.emplace(std::move(future), std::move(info));
    produced_++;
}

void InfoAggregator::insert(const eckit::URI& uri, std::unique_ptr<JumpInfo> info) {
    eckit::Offset offset(std::stoll(uri.fragment()));
    eckit::PathName path = uri.path();

    count_[info->packingType()]++;

    InfoCache::instance().insert(path, offset, std::move(info));
}

void InfoAggregator::flush() {
    LOG_DEBUG_LIB(LibGribJump) << "InfoAggregator flush" << std::endl;

    // NB: We don't close queue because we can have multiple flushes in the lifetime of the aggregator.
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return produced_ == consumed_; });
    ASSERT(produced_ == consumed_);
    ASSERT(futures_.empty());
    
    bool append = true;
    InfoCache::instance().flush(append);

    if (LibGribJump::instance().debug()) {
        LOG_DEBUG_LIB(LibGribJump) << "Flush stats:" << std::endl;
        for (const auto& [key, value] : count_) {
            LOG_DEBUG_LIB(LibGribJump) << "  " << value << " " << key << std::endl;
        }
    }

    produced_ = 0;
    consumed_ = 0;
    count_.clear();
}

void InfoAggregator::close() {
    futures_.close();
    if (consumer_.joinable()) {
        consumer_.join();
    }
}

SerialAggregator::SerialAggregator() {
}

SerialAggregator::~SerialAggregator() {
}

void SerialAggregator::add(std::future<std::shared_ptr<fdb5::FieldLocation>> future, eckit::MemoryHandle& handle, eckit::Offset offset) {

    handle.openForRead();
    eckit::AutoClose closer(handle);

    if (!isGrib(handle)) {
        eckit::Log::warning() << "Warning: Gribjump SerialAggregator received non-grib message. Skipping..." << std::endl;
        return;
    }

    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(handle, offset));
    std::shared_ptr<fdb5::FieldLocation> location = future.get();
    insert(location->fullUri(), std::move(info));
}

void SerialAggregator::insert(const eckit::URI& uri, std::unique_ptr<JumpInfo> info) {
    eckit::Offset offset(std::stoll(uri.fragment()));
    eckit::PathName path = uri.path();

    count_[info->packingType()]++;

    InfoCache::instance().insert(path, offset, std::move(info));
}

void SerialAggregator::flush() {
    LOG_DEBUG_LIB(LibGribJump) << "SerialAggregator flush" << std::endl;

    bool append = true;
    InfoCache::instance().flush(append);

    if (LibGribJump::instance().debug()) {
        LOG_DEBUG_LIB(LibGribJump) << "Flush stats:" << std::endl;
        for (const auto& [key, value] : count_) {
            LOG_DEBUG_LIB(LibGribJump) << "  " << value << " " << key << std::endl;
        }
    }
}

} // namespace gribjump