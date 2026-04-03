// C++ benchmarks for gribjump using Google Benchmark
//
// Uses the native gribjump::GribJump C++ API (same as what CXX wraps).
//
// Build: python3 scripts/bench_compare.py --build
// Run: python3 scripts/bench_compare.py --cpp
//
// Benchmark names use `cpp/` prefix for comparison with Rust benchmarks.
// Extraction benchmarks require FDB setup via environment.

#include <benchmark/benchmark.h>
#include <eckit/runtime/Main.h>
#include <gribjump/GribJump.h>
#include <gribjump/LibGribJump.h>
#include <cstdlib>
#include <cstring>
#include <vector>

// Initialize eckit Main singleton (required for gribjump)
// This is done via static initialization before benchmarks run
namespace {
struct EckitInitializer {
    EckitInitializer() {
        // Use a minimal argc/argv for eckit initialization
        static const char* argv[] = {"gribjump_bench", nullptr};
        eckit::Main::initialise(1, const_cast<char**>(argv));
    }
};
static EckitInitializer eckit_init;
}  // namespace

// Check if FDB is configured for extraction benchmarks
static bool fdb_available() {
    const char* fdb_config = std::getenv("FDB5_CONFIG");
    return fdb_config != nullptr && strlen(fdb_config) > 0;
}

static void BM_HandleCreation(benchmark::State& state) {
    for (auto _ : state) {
        gribjump::GribJump gj;
        benchmark::DoNotOptimize(&gj);
    }
}
BENCHMARK(BM_HandleCreation)->Name("cpp/handle_creation");

static void BM_RequestCreation(benchmark::State& state) {
    std::vector<gribjump::Range> ranges = {{0, 10}, {20, 30}};
    std::string request                 = "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200";

    for (auto _ : state) {
        gribjump::ExtractionRequest req(request, ranges);
        benchmark::DoNotOptimize(&req);
    }
}
BENCHMARK(BM_RequestCreation)->Name("cpp/request_creation");

static void BM_RangeCreation(benchmark::State& state) {
    for (auto _ : state) {
        gribjump::Range range = {0, 100};
        benchmark::DoNotOptimize(&range);
    }
}
BENCHMARK(BM_RangeCreation)->Name("cpp/range_creation");

static void BM_RequestManyRanges(benchmark::State& state) {
    // 100 ranges
    std::vector<gribjump::Range> ranges;
    ranges.reserve(100);
    for (int i = 0; i < 100; i++) {
        ranges.push_back({static_cast<size_t>(i * 10), static_cast<size_t>(i * 10 + 5)});
    }
    std::string request = "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200";

    for (auto _ : state) {
        gribjump::ExtractionRequest req(request, ranges);
        benchmark::DoNotOptimize(&req);
    }
}
BENCHMARK(BM_RequestManyRanges)->Name("cpp/request_100_ranges");

static void BM_Extract(benchmark::State& state) {
    if (!fdb_available()) {
        state.SkipWithMessage("FDB5_CONFIG not set");
        return;
    }

    gribjump::GribJump gj;
    std::vector<gribjump::Range> ranges = {{0, 5}};
    std::string request =
        "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";
    std::string gridhash = "33c7d6025995e1b4913811e77d38ec50";

    for (auto _ : state) {
        std::vector<gribjump::ExtractionRequest> requests;
        requests.emplace_back(request, ranges, gridhash);

        auto iter = gj.extract(requests);
        benchmark::DoNotOptimize(&iter);
    }
}
BENCHMARK(BM_Extract)->Name("cpp/extract");

static void BM_ExtractWithValues(benchmark::State& state) {
    if (!fdb_available()) {
        state.SkipWithMessage("FDB5_CONFIG not set");
        return;
    }

    gribjump::GribJump gj;
    std::vector<gribjump::Range> ranges = {{0, 5}};
    std::string request =
        "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";
    std::string gridhash = "33c7d6025995e1b4913811e77d38ec50";

    for (auto _ : state) {
        std::vector<gribjump::ExtractionRequest> requests;
        requests.emplace_back(request, ranges, gridhash);

        auto iter = gj.extract(requests);

        size_t total_values = 0;
        while (iter.hasNext()) {
            auto result = iter.next();
            total_values += result->total_values();
        }

        benchmark::DoNotOptimize(total_values);
    }
}
BENCHMARK(BM_ExtractWithValues)->Name("cpp/extract_with_values");

static void BM_ExtractLarge(benchmark::State& state) {
    if (!fdb_available()) {
        state.SkipWithMessage("FDB5_CONFIG not set");
        return;
    }

    gribjump::GribJump gj;
    // Extract 100 values instead of 5
    std::vector<gribjump::Range> ranges = {{0, 100}};
    std::string request =
        "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";
    std::string gridhash = "33c7d6025995e1b4913811e77d38ec50";

    for (auto _ : state) {
        std::vector<gribjump::ExtractionRequest> requests;
        requests.emplace_back(request, ranges, gridhash);

        auto iter = gj.extract(requests);

        size_t total_values = 0;
        while (iter.hasNext()) {
            auto result = iter.next();
            total_values += result->total_values();
        }

        benchmark::DoNotOptimize(total_values);
    }
}
BENCHMARK(BM_ExtractLarge)->Name("cpp/extract_large");

static void BM_Axes(benchmark::State& state) {
    if (!fdb_available()) {
        state.SkipWithMessage("FDB5_CONFIG not set");
        return;
    }

    gribjump::GribJump gj;
    std::string request = "class=rd,expver=xxxx,stream=oper";

    for (auto _ : state) {
        auto axes = gj.axes(request, 3);
        benchmark::DoNotOptimize(&axes);
    }
}
BENCHMARK(BM_Axes)->Name("cpp/axes");

BENCHMARK_MAIN();
