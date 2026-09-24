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

#include "gribjump/Config.h"
#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {

// Config options:
// - type          // Whether GribJump will work locally or forward work to a remote server. Allowed values are `local`
// or `remote`.
// - server        // Configuration for gribjump-server.
//   - port        // The port to listen on for incoming work.
// - uri           // host:port of remote server to forward work to (requires type:remote)
// - threads       // The number of worker threads for gribjump.extract. Default is 1.
// - cache         // Configuration of the cache.
//   - shadowfdb   // If true, the cache files will be stored in the same directory as data files. DEFAULT=true
//   - directory   // The directory where the cache will be stored, instead of shadowing the FDB.
//   - enabled     // Whether to look at the cache at all. DEFAULT=true
// - plugin        // Configuration for using GribJump as a plugin to FDB, which generates jumpinfos on the fly for
// fdb.archive()
//                 // NOTE Plugin cannot be enabled from config, one must set the envar FDB_ENABLE_GRIBJUMP
//                 // NOTE Setting env FDB_DISABLE_GRIBJUMP will override this setting and disable the plugin.
//   - select      // Defines regex for selecting which FDB keys to generate jumpinfo for. If unset, no jumpinfos will
//   be generated.
//                 // example `select: date=(20*),stream=(oper|test)`.

Config::Config() {}

Config::Config(const eckit::PathName path) : eckit::LocalConfiguration(eckit::YAMLConfiguration(path)), path_{path} {}

Config::ServerMap Config::loadServerMap() const {
    // e.g. yaml
    // servermap:
    //  - fdb: "host1:port1"
    //    gribjump: "host2:port2"
    //  - fdb: "host3:port3"
    //    gribjump: "host4:port4"
    // becomes map:
    // { "host1:port1": "host2:port2", "host3:port3": "host4:port4" }
    Config::ServerMap map;
    eckit::LocalConfiguration conf                    = getSubConfiguration("servermap");
    std::vector<eckit::LocalConfiguration> serverList = conf.getSubConfigurations();

    for (const auto& server : serverList) {
        map[server.getString("fdb")] = server.getString("gribjump");
    }

    return map;
}

// --------------------------------------------------------------------------------------------------
// ConfigOptions: Centralised definitions of all eckit::Resource-based configuration options.
// --------------------------------------------------------------------------------------------------

ConfigOptions& ConfigOptions::instance() {
    static ConfigOptions instance(LibGribJump::instance().config());
    return instance;
}

ConfigOptions::ConfigOptions(const Config& config) :
    config_(config),
    serverMap_(config.serverMap()),
    ignoreGrid_(eckit::Resource<bool>("$GRIBJUMP_IGNORE_GRID", config.getBool("ignoreGridHash", false))),
    ignoreYearMonth_(eckit::Resource<bool>("$GRIBJUMP_IGNORE_YEARMONTH", config.getBool("ignoreYearMonth", true))),
    allowMissing_(eckit::Resource<bool>("allowMissing;$GRIBJUMP_ALLOW_MISSING", config.getBool("allowMissing", false))),
    cacheSize_(eckit::Resource<int>("gribjumpCacheSize", config.getInt("cache.size", 1024))),
    cacheLazy_(eckit::Resource<bool>("gribjumpLazyInfo", config.getBool("cache.lazy", true))),
    scanCorrupted_(eckit::Resource<bool>("$GRIBJUMP_SCAN_CORRUPTED", config.getBool("scanCorrupted", false))) {
    if (cacheSize_ <= 0) {
        throw eckit::BadValue("cache.size must be positive");
    }
}

void ConfigOptions::validateInstanceConfig(const Config& config) {
    for (const char* key : {"threads", "server", "logging", "plugin", "requestParsing"}) {
        if (config.has(key)) {
            throw eckit::BadValue(std::string("GribJump instance config cannot set process-wide option: ") + key);
        }
    }
}

std::string ConfigOptions::configType() const {
    return config_.getString("type", "local");
}

std::string ConfigOptions::remoteURI() const {
    return config_.getString("uri", "");
}

int ConfigOptions::serverPort() const {
    static int value =
        eckit::Resource<int>("$GRIBJUMP_SERVER_PORT", LibGribJump::instance().config().getInt("server.port", 9777));
    return value;
}

size_t ConfigOptions::numThreads() const {
    static size_t value = eckit::Resource<size_t>("$GRIBJUMP_THREADS;gribjumpThreads",
                                                  LibGribJump::instance().config().getInt("threads", 1));
    return value;
}

bool ConfigOptions::ignoreGrid() const {
    return ignoreGrid_;
}

bool ConfigOptions::ignoreYearMonth() const {
    return ignoreYearMonth_;
}

bool ConfigOptions::requestParsing() const {
    static bool value = eckit::Resource<bool>("$GRIBJUMP_REQUEST_PARSING",
                                              LibGribJump::instance().config().getBool("requestParsing", false));
    return value;
}

bool ConfigOptions::allowMissing() const {
    return allowMissing_;
}

bool ConfigOptions::inefficientExtraction() const {
    return config_.getBool("inefficientExtraction", false);
}

bool ConfigOptions::forwardExtraction() const {
    return config_.getBool("forwardExtraction", false);
}

bool ConfigOptions::forwardScan() const {
    return config_.getBool("forwardScan", false);
}

bool ConfigOptions::cacheEnabled() const {
    return config_.getBool("cache.enabled", true);
}

std::string ConfigOptions::cacheDirectory() const {
    return config_.getString("cache.directory", "");
}

bool ConfigOptions::cacheShadowFdb() const {
    std::string cacheDir = cacheDirectory();
    return config_.getBool("cache.shadowfdb", cacheDir.empty());
}

int ConfigOptions::cacheSize() const {
    return cacheSize_;
}

bool ConfigOptions::cacheLazy() const {
    return cacheLazy_;
}

bool ConfigOptions::scanCorrupted() const {
    return scanCorrupted_;
}

bool ConfigOptions::fdbEnableGribjump() const {
    static bool value = eckit::Resource<bool>("fdbEnableGribjump;$FDB_ENABLE_GRIBJUMP", false);
    return value;
}

bool ConfigOptions::fdbDisableGribjump() const {
    static bool value = eckit::Resource<bool>("fdbDisableGribjump;$FDB_DISABLE_GRIBJUMP", false);
    return value;
}

std::string ConfigOptions::pluginSelect() const {
    return LibGribJump::instance().config().getString("plugin.select", "");
}

}  // namespace gribjump
