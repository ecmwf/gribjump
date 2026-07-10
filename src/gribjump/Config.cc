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
#include "eckit/filesystem/PathName.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/LogRouter.h"

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
//   - enable      // Whether to look at the cache at all. DEFAULT=true
// - plugin        // Configuration for using GribJump as a plugin to FDB, which generates jumpinfos on the fly for
// fdb.archive()
//                 // NOTE Plugin cannot be enabled from config, one must set the envar FDB_ENABLE_GRIBJUMP
//                 // NOTE Setting env FDB_DISABLE_GRIBJUMP will override this setting and disable the plugin.
//   - select      // Defines regex for selecting which FDB keys to generate jumpinfo for. If unset, no jumpinfos will
//   be generated.
//                 // example `select: date=(20*),stream=(oper|test)`.

Config::Config() {}

Config::Config(const eckit::PathName path) :
    eckit::LocalConfiguration(eckit::YAMLConfiguration(path)), serverMap_{loadServerMap()}, path_{path} {
    LogRouter::instance().configure(*this);
}

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

    // @todo: handling of the servermap in general could be improved, especially now with the addition of mars.
    for (const auto& server : serverList) {
        // src can now be 'fdb' or 'mars'
        auto src_uri = server.getString("fdb", "");
        if (src_uri.empty()) {
            src_uri = server.getString("mars", "");
        }
        if (src_uri.empty()) {
            throw eckit::SeriousBug("Invalid servermap config: each entry must have either 'fdb' or 'mars' key");
        }
        auto gj_uri = server.getString("gribjump");
        map[src_uri] = gj_uri;
    }

    return map;
}

// --------------------------------------------------------------------------------------------------
// ConfigOptions: Centralised definitions of all eckit::Resource-based configuration options.
// --------------------------------------------------------------------------------------------------

ConfigOptions& ConfigOptions::instance() {
    static ConfigOptions instance;
    return instance;
}

std::string ConfigOptions::configType() const {
    return LibGribJump::instance().config().getString("type", "local");
}

std::string ConfigOptions::remoteURI() const {
    return LibGribJump::instance().config().getString("uri", "");
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
    static bool value = eckit::Resource<bool>("$GRIBJUMP_IGNORE_GRID",
                                              LibGribJump::instance().config().getBool("ignoreGridHash", false));
    return value;
}

bool ConfigOptions::ignoreYearMonth() const {
    static bool value = eckit::Resource<bool>("$GRIBJUMP_IGNORE_YEARMONTH", true);
    return value;
}

bool ConfigOptions::requestParsing() const {
    static bool value = eckit::Resource<bool>("$GRIBJUMP_REQUEST_PARSING",
                                              LibGribJump::instance().config().getBool("requestParsing", false));
    return value;
}

bool ConfigOptions::allowMissing() const {
    static bool value = eckit::Resource<bool>("allowMissing;$GRIBJUMP_ALLOW_MISSING",
                                              LibGribJump::instance().config().getBool("allowMissing", false));
    return value;
}

bool ConfigOptions::inefficientExtraction() const {
    return LibGribJump::instance().config().getBool("inefficientExtraction", false);
}

bool ConfigOptions::forwardExtraction() const {
    return LibGribJump::instance().config().getBool("forwardExtraction", false);
}

bool ConfigOptions::forwardScan() const {
    return LibGribJump::instance().config().getBool("forwardScan", false);
}

bool ConfigOptions::cacheEnabled() const {
    return LibGribJump::instance().config().getBool("cache.enabled", true);
}

std::string ConfigOptions::cacheDirectory() const {
    return LibGribJump::instance().config().getString("cache.directory", "");
}

bool ConfigOptions::cacheShadowFdb() const {
    std::string cacheDir = cacheDirectory();
    return LibGribJump::instance().config().getBool("cache.shadowfdb", cacheDir.empty());
}

int ConfigOptions::cacheSize() const {
    static int value =
        eckit::Resource<int>("gribjumpCacheSize", LibGribJump::instance().config().getInt("cache.size", 1024));
    return value;
}

bool ConfigOptions::cacheLazy() const {
    static bool value =
        eckit::Resource<bool>("gribjumpLazyInfo", LibGribJump::instance().config().getBool("cache.lazy", true));
    return value;
}

bool ConfigOptions::scanCorrupted() const {
    static bool value = eckit::Resource<bool>("$GRIBJUMP_SCAN_CORRUPTED", false);
    return value;
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
