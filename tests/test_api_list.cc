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
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Offset.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/FieldLocation.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/Types.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/api/ListRequest.h"
#include "gribjump/tools/EccodesExtract.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"


using namespace eckit::testing;

namespace gribjump {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

metkit::mars::MarsRequest parseMarsRequest(const std::string& request) {
    std::istringstream in(request); 
    metkit::mars::MarsParser parser(in);
    metkit::mars::MarsExpansion expand(false, true);
    auto v = expand.expand(parser.parse());
    ASSERT(v.size() == 1);
    return v[0];
}

CASE("test ListRequest constructors") {
    std::string string_request= "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/3/1,stream=oper,time=1200,type=fc";

    // map representation
    std::map<std::string, std::string> map_request = {
        {"class", "rd"},
        {"date", "20230508"},
        {"domain", "g"},
        {"expver", "xxxx"},
        {"levtype", "sfc"},
        {"param", "151130"},
        {"step", "2/3/1"},
        {"stream", "oper"},
        {"time", "1200"},
        {"type", "fc"}};

    // mars request representation
    metkit::mars::MarsRequest mars_request = parseMarsRequest("retrieve," + string_request);

    ListRequest req_from_string = ListRequest(string_request);
    ListRequest req_from_map = ListRequest(map_request);
    ListRequest req_from_mars = ListRequest(mars_request);

    // Check that the map representation is the same for all three
    EXPECT_EQUAL(req_from_string.map(), req_from_map.map());
    EXPECT_EQUAL(req_from_string.map(), req_from_mars.map());
    EXPECT_EQUAL(req_from_map.map(), req_from_mars.map());
}

CASE("test list FDB") {

    // --------------------------------------------------------------------------------------------

    // Prep: Write test data to FDB

    std::string cwd = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(cwd.c_str());
    tmpdir.mkdir();

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        schema: schema
        spaces:
        - roots:
          - path: ")XX" + tmpdir +
                                 R"XX("
    )XX");

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "extract_ranges.grib";
    std::string gridHash = "33c7d6025995e1b4913811e77d38ec50";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    // --------------------------------------------------------------------------------------------

    // Test 1: List 3 fields. Order is not guaranteed.
    std::string request = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/3/1,stream=oper,time=1200,type=fc";

    // dictionary representation
    std::map<std::string, std::string> request_dict = {
        {"class", "rd"},
        {"date", "20230508"},
        {"domain", "g"},
        {"expver", "xxxx"},
        {"levtype", "sfc"},
        {"param", "151130"},
        {"step", "2/3/1"},
        {"stream", "oper"},
        {"time", "1200"},
        {"type", "fc"}
    };

//     // todo: imp
    ListRequest r = ListRequest(request_dict);

    GribJump gj;
    // gj.list(r); // NOTIMP yet
//     auto it = gribjump.list(request);
    
//     for (auto& element : it) {
//         std::cout << "req: " << element << std::endl;

//         // Expect mars request representation to be implemeneted.
        
//         // todo, a marsRequest impl
//         metkit::MarsRequest mars_request = element.marsRequest();

//         // todo, a json impl
//         std::string json = element.json();


//     }
}

// CASE("test list mars mock") {
//     // todo: mock mars result...
// }


// --------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump

int main(int argc, char** argv) {
    // print the current directoy
    return run_tests(argc, argv);
}
