/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "gribjump/GribJump.h"

using namespace eckit::testing;

CASE("FDB only loads GribJump configuration when the plugin is enabled") {
    // Each mode runs in a separate process: the plugin switches are cached.
    const char* modeEnv    = ::getenv("GRIBJUMP_TEST_PLUGIN_MODE");
    const std::string mode = modeEnv ? modeEnv : "disabled";
    SetEnv enable("FDB_ENABLE_GRIBJUMP", mode == "disabled" ? "0" : "1");
    SetEnv disable("FDB_DISABLE_GRIBJUMP", mode == "override" ? "1" : "0");

    eckit::TmpDir directory;
    const eckit::PathName missing = directory / "missing-gribjump.yaml";
    EXPECT(!missing.exists());
    SetEnv config("GRIBJUMP_CONFIG_FILE", missing.asString().c_str());
    const std::string fdbConfig =
        "type: local\nengine: toc\nschema: schema\nspaces:\n- roots:\n  - path: " + directory.asString() + "\n";
    SetEnv fdbEnv("FDB5_CONFIG", fdbConfig.c_str());

    if (mode == "enabled") {
        EXPECT_THROWS_AS(fdb5::FDB{}, eckit::CantOpenFile);
    }
    else {
        fdb5::FDB fdb;
        // Skipping an inactive plugin must not hide errors for explicit GribJump use.
        EXPECT_THROWS_AS(gribjump::GribJump{}, eckit::CantOpenFile);
    }
}

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
