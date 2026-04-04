// C++ benchmarks for gribjump using Google Benchmark
//
// Uses the native gribjump::GribJump C++ API (same as what CXX wraps).
//
// Build: python3 scripts/bench_compare.py --build
// Run: python3 scripts/bench_compare.py --cpp
//
// Benchmark names use `cpp/` prefix for comparison with Rust benchmarks.
// These benchmarks measure FFI overhead, not gribjump core performance.

#include <benchmark/benchmark.h>
#include <eckit/runtime/Main.h>
#include <gribjump/GribJump.h>
#include <gribjump/LibGribJump.h>
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

BENCHMARK_MAIN();
