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

#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/DataHandle.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"

#include "fdb5/api/FDB.h"

#include "metkit/codes/GribHandle.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"

#include "gribjump/GribInfo.h"
#include "gribjump/JumpHandle.h"
#include "gribjump/info/JumpInfoFactory.h"
#include "gribjump/jumper/SimpleJumper.h"
#include "gribjump/jumper/CcsdsJumper.h"
#include "gribjump/jumper/JumperFactory.h"
#include "gribjump/tools/EccodesExtract.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/Engine.h"
#include "gribjump/LibGribJump.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE ("test_engine_basic") {

    eckit::PathName gribName = "extract_ranges.grib";

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

    fdb5::FDB fdb;
    fdb.archive(*gribName.fileHandle());
    fdb.flush();


    std::vector<metkit::mars::MarsRequest> requests;
    {
        std::istringstream s(
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc\n"
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc\n"
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc\n"
            );
        metkit::mars::MarsParser parser(s);
        auto parsedRequests = parser.parse();
        metkit::mars::MarsExpension expand(/* inherit */ false);
        requests = expand.expand(parsedRequests);
    }

    std::vector<std::vector<Interval>> allIntervals = {
        {std::make_pair(0, 5),  std::make_pair(20, 30)},
        {std::make_pair(0, 5),  std::make_pair(20, 30)},
        {std::make_pair(0, 5),  std::make_pair(20, 30)}
    };

    Engine engine;
    std::map<metkit::mars::MarsRequest, std::vector<ExtractionItem*>> results = engine.extract(requests, allIntervals, false);


    // print contents of map
    for (auto& [req, exs] : results) {
        LOG_DEBUG_LIB(LibGribJump) << "Request: " << req << std::endl;
        for (auto& ex : exs) {
            ex->debug_print();
        }
    }

    // Check correct values 

    for (size_t i = 0; i < requests.size(); i++) {
        metkit::mars::MarsRequest req = requests[i];
        std::vector<Interval> intervals = allIntervals[i];
        auto exs = results[req];
        auto comparisonValues = eccodesExtract(req, intervals);
        for (size_t j = 0; j < exs.size(); j++) {
            for (size_t k = 0; k < comparisonValues[j].size(); k++) {
                for (size_t l = 0; l < comparisonValues[j][k].size(); l++) {

                    double v = exs[j]->values()[k][l];
                    if (std::isnan(v)) {
                        EXPECT(comparisonValues[j][k][l] == 9999);
                        continue;
                    }

                    EXPECT(comparisonValues[j][k][l] == v);
                }
            }
        }
    }


    // Test 2
    
    // Same request, all in one
    {
        std::istringstream s(
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/3,stream=oper,time=1200,type=fc\n"
            );
        metkit::mars::MarsParser parser(s);
        auto parsedRequests = parser.parse();
        metkit::mars::MarsExpension expand(/* inherit */ false);
        requests = expand.expand(parsedRequests);
    }

    // results = engine.extract(requests, allIntervals, true); // NOTIMP;


}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
