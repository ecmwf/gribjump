/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <fstream>
#include <future>

#include "eckit/filesystem/TmpDir.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/testing/Test.h"
#include "gribjump/GribJump.h"
#include "gribjump/GribJumpException.h"
#include "gribjump/info/InfoCache.h"

using namespace eckit::testing;
using namespace gribjump;

// Each mode is a separate CTest process: process configuration has no reset API.
CASE("process configuration initialization") {
    const char* modeEnv    = std::getenv("GRIBJUMP_TEST_PROCESS_MODE");
    const std::string mode = modeEnv ? modeEnv : "disabled";
    eckit::TmpFile file;
    {
        std::ofstream out(file.asString());
        out << "threads: 1\ncache:\n  enabled: true\n  size: 64\n";
    }
    // This executable has one test; establish its environment before starting threads.
    ASSERT(::setenv("GRIBJUMP_CONFIG_FILE", file.asString().c_str(), 1) == 0);

    if (mode == "defaults") {
        // A service can fix the configuration before the first GribJump object.
        InfoCache::instance();
        Config matching;
        matching.set("threads", 1);
        EXPECT_NO_THROW(GribJump{matching});
        matching.set("threads", 2);
        EXPECT_THROWS_AS(GribJump{matching}, eckit::BadValue);
        EXPECT_EQUAL(ProcessOptions::get().numThreads(), 1);
        return;
    }

    if (mode == "environment") {
        ASSERT(::setenv("GRIBJUMP_THREADS", "3", 1) == 0);
        ASSERT(::setenv("GRIBJUMP_REQUEST_PARSING", "1", 1) == 0);
        Config config;
        config.set("threads", 2);
        config.set("requestParsing", false);
        ProcessOptions::configure(config);
        GribJump first(config);
        EXPECT_EQUAL(ProcessOptions::get().numThreads(), 3);
        EXPECT(ProcessOptions::get().requestParsing());
        // Compare effective settings after applying environment/resource overrides.
        config.set("threads", 4);
        EXPECT_NO_THROW(GribJump{config});
        return;
    }

    if (mode == "race") {
        std::promise<void> start;
        auto ready  = start.get_future().share();
        auto create = [ready](int threads) {
            Config config;
            config.set("threads", threads);
            ready.wait();
            try {
                GribJump gj(config);
                return threads;
            }
            catch (const eckit::BadValue&) {
                return 0;
            }
        };
        auto a = std::async(std::launch::async, create, 2);
        auto b = std::async(std::launch::async, create, 3);
        start.set_value();
        const int first  = a.get();
        const int second = b.get();
        EXPECT((first == 2 && second == 0) || (first == 0 && second == 3));
        EXPECT_EQUAL(ProcessOptions::get().numThreads(), first + second);
        return;
    }

    // A failed initialization must not publish or freeze a partial configuration.
    Config invalid;
    invalid.set("threads", 0);
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::BadValue);
    invalid = Config();
    invalid.set("logging.progress", "not-a-channel");
    EXPECT_THROWS_AS(GribJump{invalid}, eckit::BadValue);

    const bool lazy = mode != "disabled-strict";
    Config config;
    config.set("threads", 2);
    config.set("cache.enabled", false);
    config.set("cache.lazy", lazy);
    config.set("ignoreGridHash", true);
    GribJump gj(config);
    EXPECT_EQUAL(ProcessOptions::get().numThreads(), 2);
    EXPECT_EQUAL(ProcessOptions::get().cacheSize(), 64);  // omitted key retains file default

    eckit::TmpDir directory;
    const auto path = directory / "disabled.grib";
    {
        std::ifstream input("extract_ranges.grib", std::ios::binary);
        ASSERT(input);
        std::ofstream output(path.asString(), std::ios::binary);
        output << input.rdbuf();
        ASSERT(output);
    }
    // An invalid on-disk index must be ignored when the cache is disabled.
    const auto indexPath = path + ".gribjump";
    {
        std::ofstream out(indexPath.asString());
        out << "not-an-index";
    }
    PathExtractionRequests requests{PathExtractionRequest(path.asString(), "file", 0, "", 0, {{0, 5}}, "")};
    if (lazy) {
        EXPECT_EQUAL(gj.extract(requests).dumpVector()[0]->total_values(), 5);
        auto first  = InfoCache::instance().get(path, eckit::Offset(0));
        auto second = InfoCache::instance().get(path, eckit::Offset(0));
        EXPECT(first != second);  // no memory cache either
    }
    else {
        EXPECT_THROWS_AS(gj.extract(requests), eckit::SeriousBug);
    }
    EXPECT_EQUAL(gj.scan({path}), 3);
    std::ifstream unchanged(indexPath.asString());
    std::string contents;
    std::getline(unchanged, contents);
    EXPECT_EQUAL(contents, "not-an-index");
    config.set("cache.enabled", true);
    EXPECT_THROWS_AS(GribJump{config}, eckit::BadValue);
}

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
