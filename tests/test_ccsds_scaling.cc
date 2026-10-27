/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// test_ccsds_scaling.cc:
// Creates a small set of integers and compresses them using libaec.
// Then we ask the CCSDS decompressor in GribJump to decompress them and apply different scale factors.
// This is a regression test for a sign error in the handling of scale factors.

#include <cmath>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include <libaec.h>

#include "eckit/testing/Test.h"
#include "gribjump/compression/compressors/Ccsds.h"

using namespace eckit::testing;

namespace gribjump::test {
namespace {

// Scale factors are signed GRIB metadata.
static_assert(std::is_same_v<decltype(std::declval<mc::CcsdsParams>().decimal_scale_factor()), long>);
static_assert(std::is_same_v<decltype(std::declval<mc::CcsdsParams>().binary_scale_factor()), long>);

template <typename Sample>
void test_scaling(unsigned int bits_per_sample) {
    std::vector<Sample> samples(257);
    const uint32_t mask = (uint32_t{1} << bits_per_sample) - 1;
    for (size_t i = 0; i < samples.size(); ++i) {
        samples[i] = static_cast<Sample>((i * 17 + 3) & mask);
    }

    // Encode native-endian integers directly, independently of GRIB scaling.
    // No external data files are used by this regression test.
    aec_stream stream{};
    stream.flags               = AEC_DATA_PREPROCESS;
    const uint16_t endian_test = 1;
    if (reinterpret_cast<const unsigned char*>(&endian_test)[0] == 0) {
        stream.flags |= AEC_DATA_MSB;
    }
    stream.bits_per_sample = bits_per_sample;
    stream.block_size      = 8;
    stream.rsi             = 4;
    stream.next_in         = reinterpret_cast<const unsigned char*>(samples.data());
    stream.avail_in        = samples.size() * sizeof(Sample);
    eckit::Buffer buffer(stream.avail_in * 2 + 1024);
    stream.next_out  = reinterpret_cast<unsigned char*>(buffer.data());
    stream.avail_out = buffer.size();
    ASSERT(aec_buffer_encode(&stream) == AEC_OK);
    eckit::Buffer encoded(buffer.data(), stream.total_out);
    auto accessor = std::make_shared<mc::MemoryAccessor>(encoded);

    // Include negative exponents, positive exponents,
    // odd/even decimal exponents, and the zero case,
    for (long binary_scale : {-38L, -18L, 0L, 3L}) {
        for (long decimal_scale : {-9L, -3L, -2L, 0L, 2L, 3L, 9L}) {
            mc::CcsdsDecompressor<double> decoder;
            decoder.flags(stream.flags)
                .block_size(stream.block_size)
                .rsi(stream.rsi)
                .bits_per_sample(bits_per_sample)
                .reference_value(1.25)
                .binary_scale_factor(binary_scale)
                .decimal_scale_factor(decimal_scale);
            decoder.n_elems(samples.size());
            EXPECT_EQUAL(decoder.binary_scale_factor(), binary_scale);
            EXPECT_EQUAL(decoder.decimal_scale_factor(), decimal_scale);

            // The GRIB formula is (X * 2^E + R) * 10^-D
            auto expected = [&](size_t i) {
                return (samples[i] * std::pow(2.0, binary_scale) + 1.25) * std::pow(10.0, -decimal_scale);
            };
            auto check = [&](const std::vector<double>& values, size_t offset) {
                for (size_t i = 0; i < values.size(); ++i) {
                    const double value = expected(offset + i);
                    EXPECT(std::abs(values[i] - value) <= std::abs(value) * 1e-12);
                }
            };

            // Exercise the full-buffer overload.
            const auto all = decoder.decode(encoded);
            EXPECT_EQUAL(all.size(), samples.size());
            check(all, 0);

            // Exercise the range overload, including RSI boundaries and the tail.
            // The full decode also computes the offsets used by range decoding.
            EXPECT(decoder.offsets().has_value());
            const std::vector<mc::Block> ranges{{0, 1}, {31, 5}, {63, 66}, {255, 2}};
            for (const auto& range : ranges) {
                const auto part = decoder.decode(accessor, range);
                EXPECT_EQUAL(part.size(), range.second);
                check(part, range.first);
            }
        }
    }
}

}  // namespace

CASE("CCSDS signed scaling, one-byte samples") {
    test_scaling<uint8_t>(5);
}

CASE("CCSDS signed scaling, two-byte samples") {
    test_scaling<uint16_t>(12);
}

CASE("CCSDS signed scaling, four-byte samples") {
    test_scaling<uint32_t>(24);
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
