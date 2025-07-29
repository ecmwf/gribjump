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
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/serialisation/FileStream.h"

#include "metkit/codes/GribHandle.h"
#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/GribJumpException.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/tools/EccodesExtract.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "path_tools.cc"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
// use the same tmpdir between tests
static eckit::PathName tmpdir;
static eckit::PathName gribName = "extract_ranges.grib";
static std::string gridHash     = "33c7d6025995e1b4913811e77d38ec50";

//-----------------------------------------------------------------------------
const std::string fdbConfig(const eckit::PathName& tmpdir) {
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
    return config_str;
}

// setup fdb
const std::string setupFDB(const eckit::PathName& tmpdir) {

    tmpdir.mkdir();

    const std::string config_str = fdbConfig(tmpdir);

    eckit::testing::SetEnv fdbconfig("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    fdb.archive(*gribName.fileHandle());
    fdb.flush();

    return config_str;
}


size_t expectedCount(std::vector<std::vector<Interval>> allIntervals) {
    // count the number of values expected given the intervals
    size_t count = 0;
    for (auto& intervals : allIntervals) {
        for (auto& interval : intervals) {
            count += interval.second - interval.first;
        }
    }

    return count;
}

CASE("Engine: pre-test setup") {
    tmpdir = eckit::TmpDir(eckit::LocalPathName::cwd().c_str());
    setupFDB(tmpdir);
}


CASE("Engine: Basic extraction") {
    // // --- Setup
    eckit::testing::SetEnv fdbconfig("FDB5_CONFIG", fdbConfig(tmpdir).c_str());
    eckit::testing::SetEnv allowmissing("GRIBJUMP_ALLOW_MISSING",
                                        "0");  // We have deliberately missing data in the request.

    // --- Extract (test 1)
    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1000,stream=oper,time=1200,type="
        "fc"  // Deliberately missing data
    };

    std::vector<std::vector<Interval>> allIntervals = {{std::make_pair(0, 5), std::make_pair(20, 30)},
                                                       {std::make_pair(0, 5), std::make_pair(20, 30)},
                                                       {std::make_pair(0, 5), std::make_pair(20, 30)},
                                                       {std::make_pair(0, 5), std::make_pair(20, 30)}};

    Engine engine;
    ExtractionRequests exRequests;
    for (size_t i = 0; i < requests.size(); i++) {
        exRequests.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
    }
    // We expect a throw due to missing data
    EXPECT_THROWS_AS(engine.extract(exRequests), DataNotFoundException);

    // drop the final request
    exRequests.pop_back();

    auto [results, report] = engine.extract(exRequests);
    EXPECT_NO_THROW(report.raiseErrors());

    // print contents of map
    for (auto& [req, ex] : results) {
        LOG_DEBUG_LIB(LibGribJump) << "Request: " << req << std::endl;
        ex->debug_print();
    }

    // Check correct values
    size_t count = 0;
    for (size_t i = 0; i < 3; i++) {
        metkit::mars::MarsRequest req   = fdb5::FDBToolRequest::requestsFromString(requests[i])[0].request();
        std::vector<Interval> intervals = allIntervals[i];
        auto& ex                        = results[requests[i]];
        auto comparisonValues           = eccodesExtract(req, intervals);
        ASSERT(comparisonValues.size() == 1);  // @todo: drop a dimension in the eccodesExtract functions
        size_t j = 0;
        for (size_t k = 0; k < comparisonValues[j].size(); k++) {
            for (size_t l = 0; l < comparisonValues[j][k].size(); l++) {
                count++;
                double v = ex->values()[k][l];
                if (std::isnan(v)) {
                    EXPECT(comparisonValues[j][k][l] == 9999);
                    continue;
                }

                EXPECT(comparisonValues[j][k][l] == v);
            }
        }
    }
    // only count the 3 intervals with data
    EXPECT(count == 45);

    fdb5::FDB fdb;
    std::vector<std::string> filenames = {};
    for (size_t i = 0; i < requests.size() - 1; i++) {
        std::string mars_str = "retrieve," + requests[i];
        std::string path_str = get_path_name_from_mars_req(mars_str, fdb);
        filenames.push_back(path_str);
    }

    std::string scheme = "file";

    std::vector<size_t> offsets = {0, 226, 452};

    std::vector<PathExtractionRequest> exPathRequests;
    for (size_t i = 0; i < filenames.size(); i++) {
        exPathRequests.push_back(PathExtractionRequest(filenames[i], scheme, offsets[i], allIntervals[i], gridHash));
    }

    auto [results_path, report_path] = engine.extract(exPathRequests);
    EXPECT_NO_THROW(report_path.raiseErrors());

    // print contents of map
    for (auto& [req, ex] : results_path) {
        LOG_DEBUG_LIB(LibGribJump) << "Request: " << req << std::endl;
        ex->debug_print();
    }

    // Check correct values
    size_t count_path = 0;
    for (size_t i = 0; i < 3; i++) {
        metkit::mars::MarsRequest req   = fdb5::FDBToolRequest::requestsFromString(requests[i])[0].request();
        std::vector<Interval> intervals = allIntervals[i];
        auto& ex                        = results_path[exPathRequests[i].requestString()];
        auto comparisonValues           = eccodesExtract(req, intervals);
        ASSERT(comparisonValues.size() == 1);  // @todo: drop a dimension in the eccodesExtract functions
        size_t j = 0;
        for (size_t k = 0; k < comparisonValues[j].size(); k++) {
            for (size_t l = 0; l < comparisonValues[j][k].size(); l++) {
                count_path++;
                double v = ex->values()[k][l];
                if (std::isnan(v)) {
                    EXPECT(comparisonValues[j][k][l] == 9999);
                    continue;
                }

                EXPECT(comparisonValues[j][k][l] == v);
            }
        }
    }
    // only count the 3 intervals with data
    EXPECT(count_path == 45);


#if 0
    // --- Extract (test 2)
    // Same request, all in one (test flattening)
    /// @todo, currently, the user cannot know order of the results after flattening, making this feature not very useful.
    /// We impose an order internally (currently, alphabetical).
    
    allIntervals = {
        {std::make_pair(0, 5),  std::make_pair(20, 30)},
    };

    requests = {
        fdb5::FDBToolRequest::requestsFromString("class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1/2/3,stream=oper,time=1200,type=fc")[0].request()
    };

    ASSERT(requests.size() == 1);

    exRequests.clear();
    exRequests.push_back(ExtractionRequest(requests[0], allIntervals[0], gridHash));

    results = engine.extract(exRequests, true);
    EXPECT_NO_THROW(engine.raiseErrors());

    // print contents of map
    for (auto& [req, exs] : results) {
        LOG_DEBUG_LIB(LibGribJump) << "Request: " << req << std::endl;
        for (auto& ex : exs) {
            ex->debug_print();
        }
    }

    // compare results

    metkit::mars::MarsRequest req = requests[0];
    auto& exs = results[req];
    auto comparisonValues = eccodesExtract(req, allIntervals[0])[0]; // [0] Because each archived field has identical values.
    count = 0;
    for (size_t j = 0; j < exs.size(); j++) {
        auto values = exs[j]->values();
        for (size_t k = 0; k < values.size(); k++) {
            for (size_t l = 0; l < values[k].size(); l++) {
                count++;
                double v = values[k][l];
                if (std::isnan(v)) {
                    EXPECT(comparisonValues[k][l] == 9999);
                    continue;
                }

                EXPECT(comparisonValues[k][l] == v);
            }
        }
    }
    EXPECT(count == 45);
#endif

    /// @todo: request touching multiple files?
    /// @todo: request involving unsupported packingType?
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
