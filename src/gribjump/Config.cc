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
#include "gribjump/remote/Protocol.h"

namespace gribjump {

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

    for (const auto& server : serverList) {
        map[server.getString("fdb")] = server.getString("gribjump");
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

size_t ConfigOptions::clientProtocolVersion() const {
    static size_t value = eckit::Resource<size_t>(
        "$GRIBJUMP_CLIENT_PROTOCOL_VERSION",
        LibGribJump::instance().config().getInt("clientProtocolVersion", streamingProtocolVersion));
    return value;
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

size_t ConfigOptions::streamingFlushBytes() const {
    static size_t value =
        eckit::Resource<size_t>("$GRIBJUMP_STREAMING_FLUSH_BYTES",
                                LibGribJump::instance().config().getUnsigned("streaming.flushBytes", 8 * 1024 * 1024));
    return value;
}

size_t ConfigOptions::streamingByteBudget() const {
    static size_t value = eckit::Resource<size_t>(
        "$GRIBJUMP_STREAMING_BYTE_BUDGET",
        LibGribJump::instance().config().getUnsigned("streaming.byteBudget", 128 * 1024 * 1024));
    return value;
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
