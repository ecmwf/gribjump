/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <string>
#include <unordered_map>
#include "eckit/config/LocalConfiguration.h"
#include "eckit/net/Endpoint.h"

namespace gribjump {

class Config : public eckit::LocalConfiguration {
public:

    using ServerMap = std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint>;

    Config();
    Config(const eckit::PathName);

    ServerMap serverMap() const;
    const std::string& path() const { return path_; }

private:
    std::string path_;
};

/// Immutable per-object options. Construct after eckit::Main initialization.
/// All values, including environment/resource overrides, are resolved at construction.
class ConfigOptions {
public:

    /// Object defaults from the library's configuration file and environment/resources.
    static const ConfigOptions& defaultOptions();
    explicit ConfigOptions(const Config&);

    const std::string& configType() const { return type_; }
    const std::string& remoteURI() const { return uri_; }
    const Config::ServerMap& serverMap() const { return serverMap_; }
    bool ignoreGrid() const { return ignoreGrid_; }
    bool ignoreYearMonth() const { return ignoreYearMonth_; }
    bool allowMissing() const { return allowMissing_; }
    bool inefficientExtraction() const { return inefficientExtraction_; }
    bool forwardExtraction() const { return forwardExtraction_; }
    bool forwardScan() const { return forwardScan_; }
    bool scanCorrupted() const { return scanCorrupted_; }

private:

    const std::string type_;
    const std::string uri_;
    const Config::ServerMap serverMap_;
    const bool ignoreGrid_;
    const bool ignoreYearMonth_;
    const bool allowMissing_;
    const bool inefficientExtraction_;
    const bool forwardExtraction_;
    const bool forwardScan_;
    const bool scanCorrupted_;
};

/// Process-wide cache, worker pool, listener, logging and binding/plugin settings.
/// The first configure() or get() fixes these settings for the process. Services
/// themselves (e.g. worker threads) are still created lazily. Construct/use only
/// after eckit::Main initialization.
class ProcessOptions {
public:

    /// Initialize from file/environment defaults if not already configured.
    static const ProcessOptions& get();

    /// Overlay explicitly supplied process keys on file defaults on first use.
    /// Later calls accept compatible values; conflicting settings throw BadValue.
    /// Omitted keys retain their established values. Calls are synchronized.
    static void configure(const Config&);

    int serverPort() const { return config_.getInt("server.port"); }
    size_t numThreads() const { return config_.getUnsigned("threads"); }
    bool requestParsing() const { return config_.getBool("requestParsing"); }
    bool cacheEnabled() const { return config_.getBool("cache.enabled"); }
    std::string cacheDirectory() const { return config_.getString("cache.directory"); }
    bool cacheShadowFdb() const { return config_.getBool("cache.shadowfdb"); }
    int cacheSize() const { return config_.getInt("cache.size"); }
    bool cacheLazy() const { return config_.getBool("cache.lazy"); }
    bool fdbEnableGribjump() const { return config_.getBool("fdbEnableGribjump"); }
    bool fdbDisableGribjump() const { return config_.getBool("fdbDisableGribjump"); }
    std::string pluginSelect() const { return config_.getString("plugin.select"); }

private:

    explicit ProcessOptions(const Config&);
    static const ProcessOptions& initialize(const Config&);
    const Config config_;  // Canonical, fully resolved process settings.
};

}  // namespace gribjump
