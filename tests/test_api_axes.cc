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

#include "eckit/testing/Test.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/filesystem/LocalPathName.h"

#include "fdb5/api/FDB.h"

#include "gribjump/GribJump.h"
#include "gribjump/FDBService.h"
#include "gribjump/tools/EccodesExtract.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"


using namespace eckit::testing;

namespace gribjump {
namespace test {

CASE ("test_api_axes") {

    // --------------------------------------------------------------------------------------------
    
    // Prep: Write test data to FDB

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

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "axes.grib";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    // --------------------------------------------------------------------------------------------

    GribJump gj;
    std::map<std::string, std::unordered_set<std::string>> axes = gj.axes("class=rd,expver=xxxx");

    EXPECT(axes.size() != 0);

    // Print the contents
    for (const auto& axis : axes) {
        std::cout << axis.first << ": ";
        for (const auto& value : axis.second) {
            std::cout << value << ", ";
        }
        std::cout << std::endl;
    }

    std::map<std::string , std::unordered_set<std::string>> expectedValues = {
        {"class", {"rd"}},
        {"date", {"20230508", "20230509"}},
        {"domain", {"g"}},
        {"expver", {"xxxx"}},
        {"levtype", {"sfc"}},
        {"levelist", {""}}, // XXX I think long term, we *do* want to include this, especially if axes includes sfc and pl, need to be able to assign some kind of null level to sfc.
        {"param", {"151130"}},
        {"step", {"3", "2", "1"}},
        {"stream", {"oper"}},
        {"time", {"1200"}},
        {"type", {"fc"}},
    };

    // compare:
    EXPECT(axes.size() == expectedValues.size());
    for (const auto& axis : axes) {
        EXPECT(expectedValues.find(axis.first) != expectedValues.end());
        EXPECT(axis.second.size() == expectedValues[axis.first].size());
        for (const auto& value : axis.second) {
            EXPECT(expectedValues[axis.first].find(value) != expectedValues[axis.first].end());
        }
    }
}

// --------------------------------------------------------------------------------------------


}  // namespace test
}  // namespace gribjump

int main(int argc, char **argv)
{
    // print the current directoy
    return run_tests ( argc, argv );
}
