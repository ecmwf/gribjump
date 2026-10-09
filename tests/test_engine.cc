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
#include <map>
#include <memory>

#include "eckit/testing/Test.h"

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/serialisation/FileStream.h"

#include "metkit/mars/MarsExpansion.h"
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


//-----------------------------------------------------------------------------
// Sinks for exercising the streaming (v4) extraction path.

// Records the streamed results by request index (deep-copying values) and how
// many chunks were emitted.
class RecordingSink : public ResultSink {
public:

    void writeResults(const std::vector<std::pair<size_t, const ExtractionResult*>>& batch) override {
        chunks++;
        for (const auto& [index, result] : batch) {
            EXPECT(received.emplace(index, result->values()).second);  // deep copy; reject duplicate indices
            masks.emplace(index, result->mask());
        }
    }

    size_t chunks = 0;
    std::map<size_t, ExValues> received;
    std::map<size_t, ExMask> masks;
};

// Fails on the first write, simulating a client disconnecting mid-stream. The
// engine must still drain outstanding tasks (not destroy the TaskGroup out from
// under running workers) and rethrow.
class ThrowingSink : public ResultSink {
public:

    void writeResults(const std::vector<std::pair<size_t, const ExtractionResult*>>&) override {
        writes++;
        throw eckit::SeriousBug("sink write failed (simulated client disconnect)");
    }

    size_t writes = 0;
};

// Exercise the real proxy-side request resolution and buffered-result emission.
// Substitute only forwarding: extract the resolved fields locally to simulate
// completed downstream replies without requiring a network server.
class BufferedForwardingEngine : public Engine {
public:

    using Engine::Engine;

    TaskReport scheduleExtractionTasks(filemap_t& filemap, bool forward) override {
        EXPECT(forward);
        calls++;
        return Engine::scheduleExtractionTasks(filemap, false);
    }

    size_t calls = 0;
};

void expectStreamed(const RecordingSink& sink, const std::map<size_t, ExValues>& expected) {
    EXPECT_EQUAL(sink.received.size(), expected.size());
    EXPECT_EQUAL(sink.masks.size(), expected.size());
    // EXPECT_EQUAL captures its operands in a lambda; capturing structured
    // bindings requires C++20, so use ordinary references in this C++17 test.
    for (const auto& entry : expected) {
        const auto& ranges = entry.second;
        const auto& values = sink.received.at(entry.first);
        const auto& masks  = sink.masks.at(entry.first);
        EXPECT_EQUAL(values.size(), ranges.size());
        EXPECT_EQUAL(masks.size(), ranges.size());
        for (size_t r = 0; r < ranges.size(); ++r) {
            EXPECT_EQUAL(values[r].size(), ranges[r].size());
            EXPECT_EQUAL(masks[r].size(), ((ranges[r].size() + 63) / 64));
            for (size_t i = 0; i < ranges[r].size(); ++i) {
                const bool missing = ranges[r][i] == 9999;  // ecCodes reference missing value
                EXPECT_EQUAL(masks[r][i / 64].test(i % 64), !missing);
                if (missing) {
                    EXPECT(std::isnan(values[r][i]));
                }
                else {
                    EXPECT_EQUAL(values[r][i], ranges[r][i]);
                }
            }
        }
    }
}

// Owns both the files and items independently of the engine. Files contain two
// copies of the GRIB fixture so callers can exercise distinct, nonzero offsets.
// No FDB archive or catalogue lookup is needed to construct this filemap.
struct StreamingFilemap {
    eckit::TmpDir directory;
    std::vector<std::unique_ptr<ExtractionItem>> owned;
    filemap_t files;
    std::map<size_t, ExValues> expected;

    void add(const std::string& name, size_t index, long long offset, const Ranges& ranges) {
        const eckit::PathName path = directory / name;
        if (!path.exists()) {
            std::ifstream input(gribName.asString(), std::ios::binary);
            std::ofstream output(path.asString(), std::ios::binary);
            ASSERT(input && output);
            output << input.rdbuf();
            input.clear();
            input.seekg(0);
            output << input.rdbuf();
            ASSERT(output);
        }
        eckit::URI uri("file", path);
        uri.fragment(std::to_string(offset));
        auto item = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>("", ranges, gridHash));
        item->URI(uri);
        item->streamIndex(index);
        files[path.asString()].push_back(item.get());
        owned.push_back(std::move(item));
        expected.emplace(index, eccodesExtract(path, {eckit::Offset(offset)}, ranges).front());
    }
};

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
    EXPECT(results.size() == exRequests.size());
    for (const auto& entry : results) {
        EXPECT_EQUAL(entry.first, entry.second->request());
    }

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
        exPathRequests.push_back(
            PathExtractionRequest(filenames[i], scheme, offsets[i], "", 0, allIntervals[i], gridHash));
    }

    auto [results_path, report_path] = engine.extract(exPathRequests);
    EXPECT_NO_THROW(report_path.raiseErrors());
    EXPECT(results_path.size() == exPathRequests.size());
    for (const auto& entry : results_path) {
        EXPECT_EQUAL(entry.first, entry.second->request());
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

CASE("Engine: request streaming preserves original indices and values") {
    eckit::testing::SetEnv fdbconfig("FDB5_CONFIG", fdbConfig(tmpdir).c_str());

    // Request order differs from canonical-key and file-offset order. Distinct
    // ranges let the value assertions detect accidental reindexing on delegation.
    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc"};

    std::vector<Ranges> allIntervals = {{{0, 5}, {20, 30}}, {{10, 14}}, {{30, 33}, {50, 55}}};

    Engine engine;
    ExtractionRequests exRequests;
    std::map<size_t, ExValues> expected;
    for (size_t i = 0; i < requests.size(); i++) {
        exRequests.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
        const auto request = fdb5::FDBToolRequest::requestsFromString(requests[i])[0].request();
        expected.emplace(i, eccodesExtract(request, allIntervals[i]).front());
    }

    RecordingSink sink;
    TaskReport report = engine.extractStreaming(exRequests, sink);
    EXPECT_NO_THROW(report.raiseErrors());

    expectStreamed(sink, expected);
}

CASE("Engine: forwarded request streaming batches buffered results and propagates sink failures") {
    eckit::testing::SetEnv fdbconfig("FDB5_CONFIG", fdbConfig(tmpdir).c_str());

    // The result map is ordered by MARS key, not by these original request indices.
    const std::vector<std::string> selections = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc"};
    const std::vector<Ranges> ranges = {{{10, 14}}, {{0, 50}}, {{30, 33}, {50, 55}}};
    ExtractionRequests requests;
    std::map<size_t, ExValues> expected;
    for (size_t i = 0; i < selections.size(); ++i) {
        requests.emplace_back(selections[i], ranges[i], gridHash);
        const auto request = fdb5::FDBToolRequest::requestsFromString(selections[i])[0].request();
        expected.emplace(i, eccodesExtract(request, ranges[i]).front());
    }

    // Cover per-result flushes (including an empty final flush), a full chunk
    // followed by a partial final chunk, and all results in a final-only flush.
    for (const size_t flushBytes : {size_t{1}, size_t{128}, size_t{1024 * 1024}}) {
        Config config;
        config.set("forwardExtraction", true);
        config.set("streaming.flushBytes", flushBytes);
        BufferedForwardingEngine engine{ConfigOptions(config)};
        RecordingSink sink;
        const auto report = engine.extractStreaming(requests, sink);
        EXPECT_NO_THROW(report.raiseErrors());
        EXPECT_EQUAL(engine.calls, 1);
        expectStreamed(sink, expected);
        const size_t expectedChunks = flushBytes == 1 ? 3 : (flushBytes == 128 ? 2 : 1);
        EXPECT_EQUAL(sink.chunks, expectedChunks);

        // No worker tasks remain when these buffered writes start. Sink errors
        // must still propagate, whether raised in a threshold or final flush.
        ThrowingSink throwing;
        EXPECT_THROWS_AS(engine.extractStreaming(requests, throwing), eckit::SeriousBug);
        EXPECT_EQUAL(engine.calls, 2);
        EXPECT_EQUAL(throwing.writes, 1);

        RecordingSink recovered;
        const auto recoveredReport = engine.extractStreaming(requests, recovered);
        EXPECT_NO_THROW(recoveredReport.raiseErrors());
        EXPECT_EQUAL(engine.calls, 3);
        expectStreamed(recovered, expected);
    }
}

CASE("Engine: prebuilt filemap streams locally and preserves caller indices") {
    for (const size_t flushBytes : {size_t{1}, size_t{1024 * 1024}}) {
        StreamingFilemap fixture;
        // Both file ordering and the task's offset sort differ from these indices.
        fixture.add("b.grib", 42, static_cast<long long>(gribName.size()), {{0, 5}, {20, 30}});
        fixture.add("b.grib", 7, 0, {{10, 14}});
        fixture.add("a.grib", 99, 0, {{30, 33}, {50, 55}});

        Config config;
        config.set("lister.type", "remote");    // any catalogue lookup would fail
        config.set("forwardExtraction", true);  // must not forward an already resolved filemap
        config.set("streaming.flushBytes", flushBytes);
        config.set("streaming.byteBudget", flushBytes);
        Engine engine{ConfigOptions(config)};
        RecordingSink sink;
        const auto report = engine.extractStreaming(fixture.files, sink);
        EXPECT_NO_THROW(report.raiseErrors());
        expectStreamed(sink, fixture.expected);
        EXPECT_EQUAL(sink.chunks, (flushBytes == 1 ? 3u : 1u));
        for (const auto& item : fixture.owned) {
            EXPECT(fixture.expected.count(item->streamIndex()) == 1);
            EXPECT(!item->result());  // the sink consumed the result, not the caller's item
        }
    }
}

CASE("Engine: prebuilt filemap drains after a sink failure and the engine can be reused") {
    Config config;
    config.set("lister.type", "remote");
    config.set("forwardExtraction", true);
    config.set("streaming.flushBytes", 1);
    config.set("streaming.byteBudget", 1);
    Engine engine{ConfigOptions(config)};
    {
        StreamingFilemap fixture;
        for (size_t i = 0; i < 12; ++i) {
            fixture.add(std::to_string(i) + ".grib", 100 + i, 0, {{0, 100}});
        }
        ThrowingSink sink;
        EXPECT_THROWS_AS(engine.extractStreaming(fixture.files, sink), eckit::SeriousBug);
        EXPECT_EQUAL(sink.writes, 1);
        // Destroy caller-owned items and files immediately: no task may still
        // reference them after the exception has escaped the engine.
    }
    StreamingFilemap next;
    next.add("next.grib", 51, 0, {{0, 5}, {20, 30}});
    RecordingSink sink;
    const auto report = engine.extractStreaming(next.files, sink);
    EXPECT_NO_THROW(report.raiseErrors());
    expectStreamed(sink, next.expected);
}

CASE("Engine: streaming survives a client disconnecting mid-stream") {
    // The sink throws on write; extractStreaming must drain outstanding tasks
    // and rethrow rather than hang or destroy the TaskGroup under live workers.
    eckit::testing::SetEnv fdbconfig("FDB5_CONFIG", fdbConfig(tmpdir).c_str());

    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc"};

    std::vector<std::vector<Interval>> allIntervals(requests.size(), {std::make_pair(0, 5), std::make_pair(20, 30)});

    Engine engine;
    ExtractionRequests exRequests;
    for (size_t i = 0; i < requests.size(); i++) {
        exRequests.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
    }

    ThrowingSink sink;
    EXPECT_THROWS_AS(engine.extractStreaming(exRequests, sink), eckit::Exception);
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
