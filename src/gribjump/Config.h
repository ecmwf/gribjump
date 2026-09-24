/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include <string>
#include <unordered_map>
#include "eckit/config/LocalConfiguration.h"
#include "eckit/net/Endpoint.h"

namespace gribjump {

class Config : public eckit::LocalConfiguration {
public:  // types

    using ServerMap = std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint>;

public:

    Config();
    Config(const eckit::PathName);

    ServerMap serverMap() const { return loadServerMap(); }

    ///@note : Will be empty if default config is used
    const std::string& path() const { return path_; }

private:

    ServerMap loadServerMap() const;

private:

    std::string path_;
};

/// @brief Immutable object options and centralised process-wide resource definitions.
///
/// Object settings are copied and environment/resource overrides resolved at construction.
/// serverPort(), numThreads(), requestParsing(), fdbEnableGribjump(), fdbDisableGribjump()
/// and pluginSelect() remain process-wide, independent of the supplied Config.
///
/// @note Construct only after eckit::Main has finished initialising.
class ConfigOptions {
public:

    /// Process-default options. Explicit GribJump objects never modify these.
    static ConfigOptions& instance();

    /// Snapshot configuration and resolve environment/resource overrides once.
    explicit ConfigOptions(const Config&);
    const Config::ServerMap& serverMap() const { return serverMap_; }

    /// Reject process-wide keys in an explicitly supplied object configuration.
    static void validateInstanceConfig(const Config&);

    // -- General options --

    /// Implementation type: "local" or "remote". YAML: type. Default: "local".
    std::string configType() const;

    /// URI of remote server (host:port). Required when type is "remote". YAML: uri. Default: "" (empty).
    std::string remoteURI() const;

    // -- Server options --

    /// Server port. Env: GRIBJUMP_SERVER_PORT. YAML: server.port. Default: 9777.
    int serverPort() const;

    // -- Worker options --

    /// Number of worker threads. Env: GRIBJUMP_THREADS. Resource: gribjumpThreads. YAML: threads. Default: 1.
    size_t numThreads() const;

    // -- Extraction options --

    /// If true, ignore grid hash checks during extraction. Env: GRIBJUMP_IGNORE_GRID. YAML: ignoreGridHash.
    /// Default: false.
    bool ignoreGrid() const;

    /// If true, ignore year/month keys when date is present. Env: GRIBJUMP_IGNORE_YEARMONTH.
    /// YAML: ignoreYearMonth. Default: true.
    bool ignoreYearMonth() const;

    /// If true, enable request parsing. Env: GRIBJUMP_REQUEST_PARSING (takes precedence).
    /// YAML: requestParsing. Default: false.
    /// Request parsing can be a bottleneck when we have many MARS requests, so this remains configurable
    /// and may need to be revisited in the future.
    bool requestParsing() const;

    /// If true, allow missing fields when listing. Env: GRIBJUMP_ALLOW_MISSING. Resource: allowMissing.
    /// YAML: allowMissing. Default: false.
    bool allowMissing() const;

    // -- Forwarding options --

    /// If true, use inefficient extraction for remote URIs (reads full messages). YAML: inefficientExtraction.
    /// Default: false.
    bool inefficientExtraction() const;

    /// If true, forward extraction requests to remote servers. YAML: forwardExtraction. Default: false.
    bool forwardExtraction() const;

    /// If true, forward scan requests to remote servers. YAML: forwardScan. Default: false.
    bool forwardScan() const;

    // -- Cache options --

    /// If true, the info cache is enabled. YAML: cache.enabled. Default: true.
    bool cacheEnabled() const;

    /// Directory for persisting cache files. YAML: cache.directory. Default: "" (empty).
    std::string cacheDirectory() const;

    /// If true, cache files are stored alongside the data files in FDB. YAML: cache.shadowfdb.
    /// Default: true when cache.directory is empty.
    bool cacheShadowFdb() const;

    /// In-memory LRU cache size. Resource: gribjumpCacheSize. YAML: cache.size. Default: 1024.
    int cacheSize() const;

    /// If true, construct JumpInfo on the fly on cache miss. Resource: gribjumpLazyInfo. YAML: cache.lazy.
    /// Default: true.
    bool cacheLazy() const;

    // -- Scan options --

    /// If true, attempt to scan corrupted GRIB files. Env: GRIBJUMP_SCAN_CORRUPTED.
    /// YAML: scanCorrupted. Default: false.
    bool scanCorrupted() const;

    // -- FDB Plugin options --

    /// Enable GribJump as FDB plugin. Resource: fdbEnableGribjump. Env: FDB_ENABLE_GRIBJUMP. Default: false.
    /// @note Can only be read after eckit::main has finished initialising.
    bool fdbEnableGribjump() const;

    /// Emergency off-switch for the FDB plugin. Resource: fdbDisableGribjump. Env: FDB_DISABLE_GRIBJUMP.
    /// Default: false.
    /// @note Can only be read after eckit::main has finished initialising.
    bool fdbDisableGribjump() const;

    /// Plugin select expression for filtering FDB keys. YAML: plugin.select. Default: "" (empty).
    std::string pluginSelect() const;

private:

    const Config config_;
    const Config::ServerMap serverMap_;
    const bool ignoreGrid_;
    const bool ignoreYearMonth_;
    const bool allowMissing_;
    const int cacheSize_;
    const bool cacheLazy_;
    const bool scanCorrupted_;
};

}  // namespace gribjump