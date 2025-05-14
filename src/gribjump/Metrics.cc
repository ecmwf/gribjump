/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "gribjump/Metrics.h"
#include "eckit/log/Log.h"
#include "eckit/runtime/Main.h"
#include "gribjump/LibGribJump.h"

namespace {
std::string iso(time_t t) {
    char buf[80];
    ::strftime(buf, sizeof(buf), "%FT%TZ", gmtime(&t));
    return std::string(buf);
}
}  // namespace

namespace gribjump {

// --------------------------------------------------------------------------------------------------------------------------------
thread_local LogContext ContextManager::context_;

// --------------------------------------------------------------------------------------------------------------------------------

Metrics::Metrics() : created_(std::time(nullptr)) {}

Metrics::~Metrics() {}

void Metrics::add(const std::string& name, const eckit::Value& value) {
    values_[name] = value;
}

void Metrics::report() {

    time_t now = std::time(nullptr);

    std::ostringstream oss;
    eckit::JSON j(oss, false);

    j.startObject();
    j << "process" << eckit::Main::instance().name();
    j << "start_time" << iso(created_);
    j << "end_time" << iso(now);
    j << "run_time" << timer_.elapsed();

    for (const auto& [name, value] : values_) {
        j << name << value;
    }
    j << "context" << ContextManager::instance().context();
    j.endObject();

    eckit::Log::metrics() << oss.str() << std::endl;
}

// --------------------------------------------------------------------------------------------------------------------------------

MetricsManager::MetricsManager() {}

MetricsManager& MetricsManager::instance() {
    static MetricsManager instance;
    return instance;
}

MetricsManager::~MetricsManager() {}


void MetricsManager::set(const std::string& name, const eckit::Value& value) {
    metrics().add(name, value);
}

Metrics& MetricsManager::metrics() {
    static thread_local Metrics metrics;
    return metrics;
}

void MetricsManager::report() {
    metrics().report();
}


// --------------------------------------------------------------------------------------------------------------------------------
ContextManager::ContextManager() {}

ContextManager& ContextManager::instance() {
    static ContextManager instance;
    return instance;
}

ContextManager::~ContextManager() {}

void ContextManager::set(const LogContext& context) {
    context_ = context;
    LOG_DEBUG_LIB(LibGribJump) << "Context set to: " << context_ << std::endl;
}

LogContext& ContextManager::context() {
    return context_;
}

}  // namespace gribjump
