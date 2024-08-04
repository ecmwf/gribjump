/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cmath>
#include <fstream>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"

#include "gribjump/GribJump.h"
#include "gribjump/FDBPlugin.h"
#include "gribjump/info/InfoCache.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/InfoExtractor.h"

#include "fdb5/api/FDB.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"
using namespace eckit::testing;

namespace gribjump {
namespace test {


//-----------------------------------------------------------------------------
// See also tests/tools/callback_vs_scan.sh.in
CASE( "test_plugin" ){

    ::setenv("FDB_ENABLE_GRIBJUMP", "1", 1);

    // write the test_plugin.yaml file
    std::string s1 = eckit::LocalPathName::cwd();
    eckit::PathName configPath(s1.c_str());
    configPath /= "test_plugin.yaml";
    std::ofstream ofs(configPath);
    ofs << "---\n"
        << "select: expver=(xxx*)\n";
    ofs.close();

    ::setenv("GRIBJUMP_FDB_CONFIG_FILE", configPath.asString().c_str(), 1);
    ::setenv("GRIBJUMP_FDB_SHADOW", "1", 1);

    std::string s = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(s.c_str());
    tmpdir.mkdir();

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        schema: schema
        spaces:
        - roots:
          - path: ")XX" + tmpdir + R"XX("
    )XX");
    std::cout << config_str << std::endl;

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    eckit::PathName gribName = "extract_ranges.grib"; // contains 3 messages

    fdb5::FDB fdb; // callback should be automatically registered
    fdb.archive(*gribName.fileHandle());
    fdb.flush();

    fdb.archive(*gribName.fileHandle());
    fdb.flush();

    // Look at the gribjump files in tmpdir
    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dir;
    tmpdir.childrenRecursive(files, dir);
    size_t count = 0;
    for (const auto& file : files) {
        if (file.extension() != ".gribjump") continue;
        ++count;
        std::cout << file << std::endl;
        FileCache cache = FileCache(file);
        cache.print(std::cout);
        EXPECT(cache.size() == 6);
    }
    EXPECT(count == 1 ); // because we should be appending to the same file
}
//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
