/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <fstream>
#include <future>

#include "eckit/filesystem/TmpDir.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/testing/Test.h"

#include "gribjump/Config.h"
#include "gribjump/ExecutionContext.h"
#include "gribjump/GribJump.h"
#include "gribjump/GribJumpException.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Lister.h"
#include "gribjump/info/InfoCache.h"

using namespace eckit::testing;

namespace gribjump::test {
namespace {

struct Fixture {
    eckit::TmpDir data;
    eckit::PathName path = data / "input.grib";

    Fixture() {
        std::ifstream input("extract_ranges.grib", std::ios::binary);
        ASSERT(input);
        std::ofstream output(path.asString(), std::ios::binary);
        output << input.rdbuf();
        ASSERT(output);
    }
};

Config configFor(const eckit::PathName& directory, bool lazy = true) {
    Config config;
    config.set("cache.directory", directory.asString());
    config.set("cache.lazy", lazy);
    return config;
}

size_t extract(GribJump& gj, const eckit::PathName& path, const std::string& hash = "wrong-hash") {
    PathExtractionRequests requests{PathExtractionRequest(path.asString(), "file", 0, "", 0, {{0, 5}}, hash)};
    auto results = gj.extract(requests).dumpVector();
    ASSERT(results.size() == 1);
    return results[0]->total_values();
}

}  // namespace

// Environment changes deliberately precede every test that starts worker threads.
CASE("resource overrides are resolved once per options object") {
    Config config;
    config.set("ignoreGridHash", false);
    SetEnv enabled("GRIBJUMP_IGNORE_GRID", "1");
    ConfigOptions first(config);
    EXPECT(first.ignoreGrid());
    {
        SetEnv disabled("GRIBJUMP_IGNORE_GRID", "0");
        ConfigOptions second(config);
        EXPECT(!second.ignoreGrid());
        EXPECT(first.ignoreGrid());
    }
}

CASE("explicit configuration does not replace process defaults") {
    eckit::TmpFile file;
    {
        std::ofstream out(file.asString());
        out << "type: local\ncache:\n  enabled: false\n";
    }
    SetEnv env("GRIBJUMP_CONFIG_FILE", file.asString());
    EXPECT(!ConfigOptions::instance().cacheEnabled());
    Config config;
    config.set("cache.enabled", true);
    GribJump explicitObject(config);
    GribJump defaultObject;
    EXPECT(!ConfigOptions::instance().cacheEnabled());
    EXPECT(!LibGribJump::instance().config().getBool("cache.enabled"));
}

CASE("options snapshot configuration including programmatic server maps") {
    Config config;
    config.set("ignoreGridHash", true);
    config.set("ignoreYearMonth", false);
    config.set("allowMissing", true);
    config.set("cache.size", 7);
    config.set("cache.lazy", false);
    config.set("scanCorrupted", true);
    config.set("forwardExtraction", true);
    config.set("forwardScan", true);
    config.set("inefficientExtraction", true);
    eckit::LocalConfiguration server;
    server.set("fdb", "localhost:9000");
    server.set("gribjump", "localhost:9001");
    config.set("servermap", std::vector<eckit::LocalConfiguration>{server});
    ConfigOptions first(config);
    config.set("ignoreGridHash", false);
    config.set("cache.size", 19);
    server.set("gribjump", "localhost:9002");
    config.set("servermap", std::vector<eckit::LocalConfiguration>{server});
    ConfigOptions second(config);
    EXPECT(first.ignoreGrid());
    EXPECT(!second.ignoreGrid());
    EXPECT(!first.ignoreYearMonth());
    EXPECT(first.allowMissing());
    EXPECT_EQUAL(first.cacheSize(), 7);
    EXPECT_EQUAL(second.cacheSize(), 19);
    EXPECT(!first.cacheLazy());
    EXPECT(first.scanCorrupted());
    EXPECT(first.forwardExtraction());
    EXPECT(first.forwardScan());
    EXPECT(first.inefficientExtraction());
    EXPECT_EQUAL(first.serverMap().at(eckit::net::Endpoint("localhost:9000")).port(), 9001);
    EXPECT_EQUAL(second.serverMap().at(eckit::net::Endpoint("localhost:9000")).port(), 9002);
}

CASE("explicit configs reject process-wide settings and invalid cache sizes") {
    for (const char* key : {"threads", "server.port", "logging.debug", "plugin.select", "requestParsing"}) {
        Config config;
        config.set(key, "1");
        EXPECT_THROWS_AS(GribJump{config}, eckit::BadValue);
    }
    Config invalid;
    invalid.set("cache.size", 0);
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::BadValue);
}

CASE("factory selection uses the supplied config without changing defaults") {
    Config remote;
    remote.set("type", "remote");
    EXPECT_THROWS_AS(GribJump{remote}, eckit::UserError);  // URI required
    remote.set("uri", "localhost:9001");
    EXPECT_NO_THROW(GribJump{remote});  // construction does not connect
    Config invalid;
    invalid.set("type", "unknown-config-test-type");
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::SeriousBug);
    EXPECT_NO_THROW(GribJump{});
    EXPECT_EQUAL(ConfigOptions::instance().configType(), "local");
}

CASE("loading a Config file does not configure process logging") {
    eckit::TmpFile file;
    {
        std::ofstream out(file.asString());
        out << "logging:\n  debug: not-a-log-channel\n";
    }
    EXPECT_NO_THROW(Config{file});
    Config config(file);
    EXPECT_THROWS_AS(GribJump{config}, eckit::BadValue);
}

CASE("listing missing-field policy belongs to each context") {
    // No worker threads have been started yet: safely configure a temporary FDB.
    eckit::TmpDir directory;
    const std::string fdbConfig =
        "type: local\nengine: toc\nschema: schema\nspaces:\n"
        "- roots:\n  - path: " +
        directory.asString() + "\n";
    SetEnv env("FDB5_CONFIG", fdbConfig);
    Config strictConfig;
    strictConfig.set("allowMissing", false);
    Config looseConfig;
    looseConfig.set("allowMissing", true);
    ExecutionContext strict{ConfigOptions(strictConfig)};
    ExecutionContext loose{ConfigOptions(looseConfig)};
    const std::string requestString =
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,"
        "step=1,stream=oper,time=1200,type=fc";
    auto request = fdb5::FDBToolRequest::requestsFromString(requestString)[0].request();
    ExItemMap items;
    items.emplace(requestString, std::make_unique<ExtractionItem>(
                                     std::make_unique<ExtractionRequest>(requestString, Ranges{{0, 5}}, "unused")));
    EXPECT_THROWS_AS(strict.lister().fileMap(request, items), DataNotFoundException);
    EXPECT(loose.lister().fileMap(request, items).empty());
    EXPECT_THROWS_AS(strict.lister().fileMap(request, items), DataNotFoundException);
}

CASE("grid validation remains isolated in either construction order and on workers") {
    Fixture fixture;
    for (bool strictFirst : {false, true}) {
        Config strictConfig;
        strictConfig.set("cache.enabled", false);
        strictConfig.set("ignoreGridHash", false);
        Config looseConfig(strictConfig);
        looseConfig.set("ignoreGridHash", true);
        GribJump first(strictFirst ? strictConfig : looseConfig);
        GribJump second(strictFirst ? looseConfig : strictConfig);
        GribJump& strict = strictFirst ? first : second;
        GribJump& loose  = strictFirst ? second : first;
        // Mutating the caller's configuration must not change either object.
        strictConfig.set("ignoreGridHash", true);
        looseConfig.set("ignoreGridHash", false);
        EXPECT_EQUAL(extract(loose, fixture.path), 5);
        EXPECT_THROWS_AS(extract(strict, fixture.path), eckit::SeriousBug);
        EXPECT_EQUAL(extract(loose, fixture.path), 5);

        auto accepted = std::async(std::launch::async, [&] {
            for (int i = 0; i < 8; ++i) {
                if (extract(loose, fixture.path) != 5)
                    return false;
            }
            return true;
        });
        auto rejected = std::async(std::launch::async, [&] {
            for (int i = 0; i < 8; ++i) {
                try {
                    extract(strict, fixture.path);
                }
                catch (const eckit::SeriousBug&) {
                    continue;
                }
                return false;
            }
            return true;
        });
        EXPECT(accepted.get());
        EXPECT(rejected.get());
    }
    EXPECT(!(fixture.path + ".gribjump").exists());
}

CASE("objects can be constructed concurrently with conflicting configs") {
    Fixture fixture;
    auto run = [&](bool ignoreGrid) {
        Config config;
        config.set("cache.enabled", false);
        config.set("ignoreGridHash", ignoreGrid);
        for (int i = 0; i < 8; ++i) {
            GribJump gj(config);
            try {
                if (extract(gj, fixture.path) != 5 || !ignoreGrid) {
                    return false;
                }
            }
            catch (const eckit::SeriousBug&) {
                if (ignoreGrid) {
                    return false;
                }
            }
        }
        return true;
    };
    auto strict = std::async(std::launch::async, run, false);
    auto loose  = std::async(std::launch::async, run, true);
    EXPECT(strict.get());
    EXPECT(loose.get());
}

CASE("cache directory and lazy policy belong to each object") {
    Fixture fixture;
    eckit::TmpDir firstDir;
    eckit::TmpDir secondDir;
    Config firstConfig = configFor(firstDir, false);
    firstConfig.set("ignoreGridHash", true);
    GribJump first(firstConfig);
    // Initialize first's cache before constructing the second object.
    EXPECT_THROWS_AS(extract(first, fixture.path), eckit::SeriousBug);
    Config secondConfig = configFor(secondDir, true);
    secondConfig.set("ignoreGridHash", true);
    GribJump second(secondConfig);
    EXPECT_EQUAL(extract(second, fixture.path), 5);
    EXPECT_THROWS_AS(extract(first, fixture.path), eckit::SeriousBug);
    EXPECT_EQUAL(first.scan({fixture.path}), 3);
    EXPECT((firstDir / "input.grib.gribjump").exists());
    EXPECT(!(secondDir / "input.grib.gribjump").exists());
    EXPECT_EQUAL(extract(first, fixture.path), 5);
    EXPECT_EQUAL(second.scan({fixture.path}), 3);
    EXPECT((secondDir / "input.grib.gribjump").exists());
    EXPECT_EQUAL(first.scan({fixture.path}), 0);
    EXPECT(!(fixture.path + ".gribjump").exists());

    // A disabled cache must neither use an existing disk cache nor write one.
    Config disabledConfig(firstConfig);
    disabledConfig.set("cache.enabled", false);
    GribJump disabled(disabledConfig);
    EXPECT_THROWS_AS(extract(disabled, fixture.path), eckit::SeriousBug);
    EXPECT_EQUAL(disabled.scan({fixture.path}), 3);
    EXPECT_EQUAL(extract(first, fixture.path), 5);
}

CASE("context caches are distinct even when directories match") {
    Fixture fixture;
    eckit::TmpDir directory;
    Config lazyConfig   = configFor(directory, true);
    Config strictConfig = configFor(directory, false);
    ExecutionContext lazy{ConfigOptions(lazyConfig)};
    ExecutionContext strict{ConfigOptions(strictConfig)};
    EXPECT(lazy.cache().get(fixture.path, eckit::Offset(0)) != nullptr);
    EXPECT_THROWS_AS(strict.cache().get(fixture.path, eckit::Offset(0)), JumpInfoExtractionDisabled);
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
