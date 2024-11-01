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

#include "eckit/runtime/Main.h"
#include "gribjump/Metrics.h"

namespace {
std::string iso(time_t t) {
    char buf[80];
    ::strftime(buf, sizeof(buf), "%FT%TZ", gmtime(&t));
    return std::string(buf);
}
} // namespace

namespace gribjump {

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
    j << "context" << context_;
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

void MetricsManager::setContext(const LogContext& context) {
    metrics().addContext(context);
}

Metrics& MetricsManager::metrics() {
    static thread_local Metrics metrics;
    return metrics;
}

void MetricsManager::report() {
    metrics().report();
}

} // namespace gribjump
