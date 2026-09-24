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
#include "gribjump/GribJump.h"
#include "gribjump/GribJumpException.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Lister.h"
#include "gribjump/LogRouter.h"
#include "gribjump/info/InfoCache.h"

using namespace eckit::testing;

namespace gribjump::test {
namespace {

std::unique_ptr<eckit::TmpDir> cacheDirectory;

struct Fixture {
    eckit::TmpDir data;
    eckit::PathName path = data / (data.baseName() + ".grib");

    Fixture() {
        std::ifstream input("extract_ranges.grib", std::ios::binary);
        ASSERT(input);
        std::ofstream output(path.asString(), std::ios::binary);
        output << input.rdbuf();
        ASSERT(output);
    }
};

size_t extract(GribJump& gj, const eckit::PathName& path) {
    PathExtractionRequests requests{PathExtractionRequest(path.asString(), "file", 0, "", 0, {{0, 5}}, "wrong-hash")};
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

CASE("first object configures process settings without replacing object defaults") {
    cacheDirectory = std::make_unique<eckit::TmpDir>();
    eckit::TmpFile file;
    {
        std::ofstream out(file.asString());
        out << "type: local\nignoreGridHash: false\nthreads: 1\ncache:\n  size: 31\n  directory: "
            << cacheDirectory->asString() << "\n";
    }
    SetEnv env("GRIBJUMP_CONFIG_FILE", file.asString());
    EXPECT(!ConfigOptions::defaultOptions().ignoreGrid());  // Reading object defaults does not freeze process settings.
    Config config;
    config.set("threads", 2);
    config.set("cache.size", 7);
    config.set("ignoreGridHash", true);
    config.set("logging.progress", "info");
    GribJump first(config);
    EXPECT_EQUAL(ProcessOptions::get().numThreads(), 2);
    EXPECT_EQUAL(ProcessOptions::get().cacheSize(), 7);
    EXPECT_EQUAL(ProcessOptions::get().cacheDirectory(), cacheDirectory->asString());
    EXPECT(&LogRouter::instance().get("progress") == &eckit::Log::info());
    EXPECT(!ConfigOptions::defaultOptions().ignoreGrid());
    EXPECT_EQUAL(LibGribJump::instance().config().getInt("threads"), 1);
    // Repeated and omitted settings are accepted; the caller's Config is independent.
    EXPECT_NO_THROW(GribJump{config});
    EXPECT_NO_THROW(GribJump{});
    config.set("threads", 3);
    EXPECT_THROWS_AS(GribJump{config}, eckit::BadValue);
    EXPECT_EQUAL(ProcessOptions::get().numThreads(), 2);
}

CASE("all per-object options are snapshots including programmatic server maps") {
    Config config;
    config.set("ignoreGridHash", true);
    config.set("ignoreYearMonth", false);
    config.set("allowMissing", true);
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
    config.set("forwardExtraction", false);
    server.set("gribjump", "localhost:9002");
    config.set("servermap", std::vector<eckit::LocalConfiguration>{server});
    ConfigOptions second(config);
    EXPECT(first.ignoreGrid());
    EXPECT(!second.ignoreGrid());
    EXPECT(!first.ignoreYearMonth());
    EXPECT(first.allowMissing());
    EXPECT(first.scanCorrupted());
    EXPECT(first.forwardExtraction());
    EXPECT(!second.forwardExtraction());
    EXPECT(first.forwardScan());
    EXPECT(first.inefficientExtraction());
    EXPECT_EQUAL(first.serverMap().at(eckit::net::Endpoint("localhost:9000")).port(), 9001);
    EXPECT_EQUAL(second.serverMap().at(eckit::net::Endpoint("localhost:9000")).port(), 9002);
}

CASE("conflicting process settings fail without altering established state") {
    const auto& options = ProcessOptions::get();
    for (const char* key : {"threads", "server.port", "cache.size"}) {
        Config conflict;
        conflict.set(key, 999);
        EXPECT_THROWS_AS(GribJump{conflict}, eckit::BadValue);
    }
    for (const char* key : {"cache.enabled", "cache.shadowfdb", "cache.lazy", "requestParsing"}) {
        Config conflict;
        const bool current = std::string(key) == "cache.enabled" || std::string(key) == "cache.lazy";
        conflict.set(key, !current);
        EXPECT_THROWS_AS(GribJump{conflict}, eckit::BadValue);
    }
    for (const char* key : {"cache.directory", "plugin.select", "logging.progress"}) {
        Config conflict;
        conflict.set(key, "debug");
        EXPECT_THROWS_AS(GribJump{conflict}, eckit::BadValue);
    }
    Config invalid;
    invalid.set("cache.size", 0);
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::BadValue);
    EXPECT_EQUAL(options.numThreads(), 2);
    EXPECT_EQUAL(options.cacheSize(), 7);
    EXPECT_EQUAL(options.cacheDirectory(), cacheDirectory->asString());
    EXPECT(&LogRouter::instance().get("progress") == &eckit::Log::info());
}

CASE("factory selection uses the supplied object config") {
    Config remote;
    remote.set("type", "remote");
    EXPECT_THROWS_AS(GribJump{remote}, eckit::UserError);
    remote.set("uri", "localhost:9001");
    EXPECT_NO_THROW(GribJump{remote});
    Config invalid;
    invalid.set("type", "unknown-config-test-type");
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::SeriousBug);
    EXPECT_NO_THROW(GribJump{});
    EXPECT_EQUAL(ConfigOptions::defaultOptions().configType(), "local");
}

CASE("loading a Config file has no logging side effects") {
    eckit::TmpFile file;
    {
        std::ofstream out(file.asString());
        out << "logging:\n  progress: not-a-log-channel\n";
    }
    EXPECT_NO_THROW(Config{file});
    EXPECT_THROWS_AS(GribJump{Config(file)}, eckit::BadValue);
    EXPECT(&LogRouter::instance().get("progress") == &eckit::Log::info());
}

CASE("listing missing-field policy belongs to each lister") {
    eckit::TmpDir directory;
    const std::string fdbConfig =
        "type: local\nengine: toc\nschema: schema\nspaces:\n- roots:\n  - path: " + directory.asString() + "\n";
    SetEnv env("FDB5_CONFIG", fdbConfig);
    Config strictConfig;
    strictConfig.set("allowMissing", false);
    Config looseConfig;
    looseConfig.set("allowMissing", true);
    FDBLister strict{ConfigOptions(strictConfig)};
    FDBLister loose{ConfigOptions(looseConfig)};
    const std::string requestString =
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc";
    auto request = fdb5::FDBToolRequest::requestsFromString(requestString)[0].request();
    ExItemMap items;
    items.emplace(requestString, std::make_unique<ExtractionItem>(
                                     std::make_unique<ExtractionRequest>(requestString, Ranges{{0, 5}}, "unused")));
    EXPECT_THROWS_AS(strict.fileMap(request, items), DataNotFoundException);
    EXPECT(loose.fileMap(request, items).empty());
    EXPECT_THROWS_AS(strict.fileMap(request, items), DataNotFoundException);
}

CASE("grid validation is isolated in both construction orders and on workers") {
    Fixture fixture;
    for (bool strictFirst : {false, true}) {
        Config strictConfig;
        strictConfig.set("ignoreGridHash", false);
        Config looseConfig;
        looseConfig.set("ignoreGridHash", true);
        GribJump first(strictFirst ? strictConfig : looseConfig);
        GribJump second(strictFirst ? looseConfig : strictConfig);
        GribJump& strict = strictFirst ? first : second;
        GribJump& loose  = strictFirst ? second : first;
        strictConfig.set("ignoreGridHash", true);
        looseConfig.set("ignoreGridHash", false);
        EXPECT_EQUAL(extract(loose, fixture.path), 5);
        EXPECT_THROWS_AS(extract(strict, fixture.path), eckit::SeriousBug);
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
}

CASE("concurrent constructors accept compatible process settings and isolate object settings") {
    Fixture fixture;
    auto run = [&](bool ignoreGrid) {
        Config config;
        config.set("threads", 2);
        config.set("cache.size", 7);
        config.set("ignoreGridHash", ignoreGrid);
        for (int i = 0; i < 8; ++i) {
            GribJump gj(config);
            try {
                if (extract(gj, fixture.path) != 5 || !ignoreGrid)
                    return false;
            }
            catch (const eckit::SeriousBug&) {
                if (ignoreGrid)
                    return false;
            }
        }
        return true;
    };
    auto strict = std::async(std::launch::async, run, false);
    auto loose  = std::async(std::launch::async, run, true);
    EXPECT(strict.get());
    EXPECT(loose.get());
}

CASE("objects share the process cache and reject changes after use") {
    Fixture fixture;
    Config config;
    config.set("ignoreGridHash", true);
    GribJump first(config);
    EXPECT_EQUAL(first.scan({fixture.path}), 3);
    GribJump second(config);
    EXPECT_EQUAL(second.scan({fixture.path}), 0);
    EXPECT((*cacheDirectory / (fixture.path.baseName() + ".gribjump")).exists());
    EXPECT(!(fixture.path + ".gribjump").exists());
    EXPECT_EQUAL(extract(first, fixture.path), 5);
    auto info = InfoCache::instance().get(fixture.path, eckit::Offset(0));
    EXPECT_EQUAL(extract(second, fixture.path), 5);
    EXPECT(InfoCache::instance().get(fixture.path, eckit::Offset(0)) == info);
    config.set("cache.lazy", false);
    EXPECT_THROWS_AS(GribJump{config}, eckit::BadValue);
    EXPECT_EQUAL(extract(second, fixture.path), 5);
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
