/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/Config.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <mutex>

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LogRouter.h"

namespace gribjump {

Config::Config() = default;

Config::Config(const eckit::PathName path) : eckit::LocalConfiguration(eckit::YAMLConfiguration(path)), path_(path) {}

Config::ServerMap Config::serverMap() const {
    ServerMap map;
    for (const auto& server : getSubConfiguration("servermap").getSubConfigurations()) {
        map[server.getString("fdb")] = server.getString("gribjump");
    }
    return map;
}

const ConfigOptions& ConfigOptions::defaultOptions() {
    static const ConfigOptions options(LibGribJump::instance().config());
    return options;
}

ConfigOptions::ConfigOptions(const Config& config) :
    type_(config.getString("type", "local")),
    uri_(config.getString("uri", "")),
    serverMap_(config.serverMap()),
    ignoreGrid_(eckit::Resource<bool>("$GRIBJUMP_IGNORE_GRID", config.getBool("ignoreGridHash", false))),
    ignoreYearMonth_(eckit::Resource<bool>("$GRIBJUMP_IGNORE_YEARMONTH", config.getBool("ignoreYearMonth", true))),
    allowMissing_(eckit::Resource<bool>("allowMissing;$GRIBJUMP_ALLOW_MISSING", config.getBool("allowMissing", false))),
    inefficientExtraction_(config.getBool("inefficientExtraction", false)),
    forwardExtraction_(config.getBool("forwardExtraction", false)),
    forwardScan_(config.getBool("forwardScan", false)),
    scanCorrupted_(eckit::Resource<bool>("$GRIBJUMP_SCAN_CORRUPTED", config.getBool("scanCorrupted", false))) {}

namespace {

const char* const boolKeys[]   = {"cache.enabled", "cache.shadowfdb", "cache.lazy", "requestParsing"};
const char* const intKeys[]    = {"threads", "server.port", "cache.size"};
const char* const stringKeys[] = {"cache.directory", "plugin.select"};

// Copy leaves individually so omitted settings in a section retain their values.
void overlayProcessConfig(Config& target, const Config& source) {
    for (const auto* key : boolKeys) {
        if (source.has(key))
            target.set(key, source.getBool(key));
    }
    for (const auto* key : intKeys) {
        if (source.has(key))
            target.set(key, source.getInt(key));
    }
    for (const auto* key : stringKeys) {
        if (source.has(key))
            target.set(key, source.getString(key));
    }
    for (const auto& alias : source.getSubConfiguration("logging").keys()) {
        const std::string key = "logging." + alias;
        target.set(key, source.getString(key));
    }
}

Config resolveProcessConfig(const Config& source) {
    Config result;
    result.set("threads", int(eckit::Resource<int>("$GRIBJUMP_THREADS;gribjumpThreads", source.getInt("threads", 1))));
    result.set("server.port", int(eckit::Resource<int>("$GRIBJUMP_SERVER_PORT", source.getInt("server.port", 9777))));
    result.set("requestParsing",
               bool(eckit::Resource<bool>("$GRIBJUMP_REQUEST_PARSING", source.getBool("requestParsing", false))));
    result.set("cache.enabled", source.getBool("cache.enabled", true));
    result.set("cache.directory", source.getString("cache.directory", ""));
    result.set("cache.shadowfdb", source.getBool("cache.shadowfdb", result.getString("cache.directory").empty()));
    result.set("cache.size", int(eckit::Resource<int>("gribjumpCacheSize", source.getInt("cache.size", 1024))));
    result.set("cache.lazy", bool(eckit::Resource<bool>("gribjumpLazyInfo", source.getBool("cache.lazy", true))));
    result.set("plugin.select", source.getString("plugin.select", ""));
    // Enabling/disabling the archive plugin remains an environment/resource-only control.
    result.set("fdbEnableGribjump", bool(eckit::Resource<bool>("fdbEnableGribjump;$FDB_ENABLE_GRIBJUMP", false)));
    result.set("fdbDisableGribjump", bool(eckit::Resource<bool>("fdbDisableGribjump;$FDB_DISABLE_GRIBJUMP", false)));
    if (result.getInt("threads") <= 0 || result.getInt("cache.size") <= 0) {
        throw eckit::BadValue("threads and cache.size must be positive");
    }
    for (const auto& alias : source.getSubConfiguration("logging").keys()) {
        const std::string key = "logging." + alias;
        auto channel          = source.getString(key);
        std::transform(channel.begin(), channel.end(), channel.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (channel != "debug" && channel != "info" && channel != "error" && channel != "default") {
            throw eckit::BadValue("Unknown logging channel: " + channel);
        }
        result.set(key, channel);
    }
    return result;
}

void checkCompatible(const Config& established, const Config& config) {
    Config supplied;
    overlayProcessConfig(supplied, config);
    if (supplied.keys().empty()) {
        return;
    }
    const Config candidate = resolveProcessConfig(supplied);
    auto check             = [&](const std::string& key) {
        if (supplied.has(key) && (!established.has(key) || established.getString(key) != candidate.getString(key))) {
            throw eckit::BadValue("Process-wide option is already configured: " + key);
        }
    };
    for (const auto* key : boolKeys)
        check(key);
    for (const auto* key : intKeys)
        check(key);
    for (const auto* key : stringKeys)
        check(key);
    for (const auto& alias : supplied.getSubConfiguration("logging").keys())
        check("logging." + alias);
}

}  // namespace

ProcessOptions::ProcessOptions(const Config& config) : config_(resolveProcessConfig(config)) {}

const ProcessOptions& ProcessOptions::initialize(const Config& config) {
    static std::mutex mutex;
    static std::unique_ptr<const ProcessOptions> options;
    std::lock_guard<std::mutex> lock(mutex);

    if (options) {
        if (config.keys().empty()) {
            return *options;
        }
        checkCompatible(options->config_, config);
    }
    else {
        Config candidate(LibGribJump::instance().config());
        overlayProcessConfig(candidate, config);
        auto resolved = std::unique_ptr<const ProcessOptions>(new ProcessOptions(candidate));
        // Validate everything before configuring logging or publishing the snapshot.
        LogRouter::instance().configure(resolved->config_);
        options = std::move(resolved);
    }
    return *options;
}

const ProcessOptions& ProcessOptions::get() {
    static const ProcessOptions& options = initialize(Config());
    return options;
}

void ProcessOptions::configure(const Config& config) {
    initialize(config);
}

}  // namespace gribjump
