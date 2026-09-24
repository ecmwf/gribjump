/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/ExecutionContext.h"
#include "gribjump/Lister.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump {

ExecutionContext::ExecutionContext(const ConfigOptions& options) : options_(options) {}
ExecutionContext::~ExecutionContext() = default;

FDBLister& ExecutionContext::lister() {
    std::call_once(listerOnce_, [this] { lister_ = std::make_unique<FDBLister>(options_); });
    return *lister_;
}

InfoCache& ExecutionContext::cache() {
    std::call_once(cacheOnce_, [this] { cache_ = std::make_unique<InfoCache>(options_); });
    return *cache_;
}

}  // namespace gribjump
