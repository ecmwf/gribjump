/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0.
 */
#include "gribjump/ReadStats.h"

namespace gribjump {

thread_local TaskReadScope* TaskReadScope::current_ = nullptr;

TaskReadScope::TaskReadScope(std::atomic<size_t>& groupBytes) :
    previous_(current_), groupBytes_(groupBytes) {
    current_ = this;
}

TaskReadScope::~TaskReadScope() {
    current_ = previous_;
    groupBytes_.fetch_add(bytes_, std::memory_order_relaxed);
}

void TaskReadScope::requested(long length) {
    if (current_ && length > 0) {
        current_->bytes_ += static_cast<size_t>(length);
    }
}

}  // namespace gribjump
