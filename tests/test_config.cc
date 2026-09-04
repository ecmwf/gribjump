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

#include "eckit/filesystem/TmpFile.h"
#include "eckit/testing/Test.h"

#include "gribjump/Config.h"
#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

// Verify that LibGribJump::setConfig() replaces the active config so that
// ConfigOptions reads the new values without a process restart.
CASE("setConfig_overrides_singleton") {
    // Load a config with cache disabled.
    Config base;
    base.set("cache.enabled", false);
    LibGribJump::instance().setConfig(Config(base));

    EXPECT_EQUAL(ConfigOptions::instance().cacheEnabled(), false);

    // Override to re-enable cache.
    Config override;
    override.set("cache.enabled", true);
    LibGribJump::instance().setConfig(Config(override));

    EXPECT_EQUAL(ConfigOptions::instance().cacheEnabled(), true);
}

// Verify that GribJump(cfg) sets the config before building impl_, so
// the impl_ type and other options reflect the supplied config.
CASE("GribJump_config_constructor_overrides_env_file") {
    // Write a baseline config file: cache disabled.
    eckit::TmpFile baseFile;
    {
        std::ofstream f(baseFile.asString());
        f << "cache:\n  enabled: false\n";
    }

    // Install the baseline via setConfig (simulates GRIBJUMP_CONFIG_FILE load).
    LibGribJump::instance().setConfig(Config(eckit::PathName(baseFile)));
    EXPECT_EQUAL(ConfigOptions::instance().cacheEnabled(), false);

    // Now supply an override config via the GribJump constructor.
    Config overrideCfg;
    overrideCfg.set("cache.enabled", true);
    GribJump gj(overrideCfg);

    EXPECT_EQUAL(ConfigOptions::instance().cacheEnabled(), true);
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
