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

#include <memory>
#include <mutex>

#include "gribjump/Config.h"

namespace gribjump {

class FDBLister;
class InfoCache;

/// Configuration and services belonging to one local GribJump object.
/// Task groups retain shared ownership while work is running on the process-wide pool.
class ExecutionContext {
public:

    explicit ExecutionContext(const ConfigOptions& options = ConfigOptions::instance());
    ~ExecutionContext();

    const ConfigOptions& options() const { return options_; }
    FDBLister& lister();
    InfoCache& cache();

private:

    const ConfigOptions options_;
    std::once_flag listerOnce_;
    std::once_flag cacheOnce_;
    std::unique_ptr<FDBLister> lister_;
    std::unique_ptr<InfoCache> cache_;
};

}  // namespace gribjump
