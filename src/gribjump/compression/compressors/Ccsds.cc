/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
 
#include "gribjump/compression/compressors/Ccsds.h"

#include <algorithm>
#include <memory>
#include <libaec.h>

#include "gribjump/compression/compressors/Aec.h"

namespace  {

/* Return n to the power of s */
template <typename T>
constexpr T codes_power(long s, long n) {
    T divisor = 1.0;
    if (s == 0)
        return 1.0;
    if (s == 1)
        return n;
    while (s < 0) {
        divisor /= n;
        s++;
    }
    while (s > 0) {
        divisor *= n;
        s--;
    }
    return divisor;
}

bool is_big_endian() {
    unsigned char is_big_endian = 0;
    unsigned short endianess_test = 1;
    return reinterpret_cast<const char*>(&endianess_test)[0] == is_big_endian;
}

size_t modify_aec_flags(size_t flags) {
    // ECC-1602: Performance improvement: enabled the use of native data types
    flags &= ~AEC_DATA_3BYTE;  // disable support for 3-bytes per value
    if (is_big_endian())
        flags |= AEC_DATA_MSB; // enable big-endian
    else
        flags &= ~AEC_DATA_MSB;  // enable little-endian
    return flags;
}

} // namespace

namespace mc {

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
CcsdsParams::Offsets CcsdsDecompressor<ValueType>::decode_offsets(const eckit::Buffer& in_buf){

    Offsets offsets;
    switch (auto nbytes = sample_nbytes()) {
        case 1:
            offsets = decode_offsets_<uint8_t>(in_buf);
            break;
        case 2:
            offsets = decode_offsets_<uint16_t>(in_buf);
            break;
        case 4:
            offsets = decode_offsets_<uint32_t>(in_buf);
            break;
        default:
            std::stringstream ss;
            ss << nbytes;
            throw eckit::SeriousBug("Invalid number of bytes per sample: " + ss.str(), Here());
    }

    return offsets;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename CcsdsDecompressor<ValueType>::Values CcsdsDecompressor<ValueType>::decode(const eckit::Buffer& in_buf) {
    auto bscale = codes_power<double>(binary_scale_factor_, 2);
    auto dscale = codes_power<double>(decimal_scale_factor_, -10);
    auto flags = modify_aec_flags(flags_);

    Values values;

    switch (auto nbytes = sample_nbytes()) {
        case 1:
            values = decode_all_<uint8_t>(in_buf, bscale, dscale);
            break;
        case 2:
            values = decode_all_<uint16_t>(in_buf, bscale, dscale);
            break;
        case 4:
            values = decode_all_<uint32_t>(in_buf, bscale, dscale);
            break;
        default:
            std::stringstream ss;
            ss << nbytes;
            throw eckit::SeriousBug("Invalid number of bytes per sample: " + ss.str(), Here());
    }

    return values;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename CcsdsDecompressor<ValueType>::Values CcsdsDecompressor<ValueType>::decode(const std::shared_ptr<DataAccessor> accessor, const Block& range){
    if (range.second == 0) {
        return Values{};
    }
    
    auto bscale = codes_power<double>(binary_scale_factor_, 2);
    auto dscale = codes_power<double>(decimal_scale_factor_, -10);
    Values values;

    switch (auto nbytes = sample_nbytes()) {
        case 1:
        values = decode_range_<uint8_t>(accessor, range, bscale, dscale);
        break;
        case 2:
        values = decode_range_<uint16_t>(accessor, range, bscale, dscale);
        break;
        case 4:
        values = decode_range_<uint32_t>(accessor, range, bscale, dscale);
        break;
        default:
        std::stringstream ss;
        ss << nbytes;
        throw eckit::SeriousBug("Invalid number of bytes per sample: " + ss.str(), Here());
    }

    return values;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
size_t CcsdsDecompressor<ValueType>::sample_nbytes() const { 
    size_t nbytes = (bits_per_sample_ + 7) / 8;
    if (nbytes == 3) {
        nbytes = 4;
    }
    return nbytes;
}
// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
template <typename SimpleValueType>
typename CcsdsDecompressor<ValueType>::Values CcsdsDecompressor<ValueType>::decode_range_ (const std::shared_ptr<DataAccessor> accessor, const Block& simple_range, double bscale, double dscale) {
    AecDecompressor<SimpleValueType> aec{};
    auto flags = modify_aec_flags(flags_);
    aec.flags(flags);
    aec.bits_per_sample(bits_per_sample_);
    aec.block_size(block_size_);
    aec.rsi(rsi_);
    aec.offsets(offsets_);

    typename AecDecompressor<SimpleValueType>::Values simple_values = aec.decode(accessor, simple_range);
    
    Values values(simple_values.size());
    std::transform(simple_values.begin(), simple_values.end(), values.begin(), [&](const auto& simple_value) {
        return (simple_value * bscale + reference_value_) * dscale;
    });

    return values;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
template <typename SimpleValueType>
CcsdsParams::Offsets CcsdsDecompressor<ValueType>::decode_offsets_ (const typename AecDecompressor<SimpleValueType>::CompressedData& in_buf) {
    AecDecompressor<SimpleValueType> aec{};
    auto flags = modify_aec_flags(flags_);
    aec.flags(flags);
    aec.bits_per_sample(bits_per_sample_);
    aec.block_size(block_size_);
    aec.rsi(rsi_);
    aec.n_elems(n_elems_);

    return aec.decode_offsets(in_buf);
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
template <typename SimpleValueType>
typename CcsdsDecompressor<ValueType>::Values CcsdsDecompressor<ValueType>::decode_all_ (const typename AecDecompressor<SimpleValueType>::CompressedData& in_buf, double bscale, double dscale) {
    AecDecompressor<SimpleValueType> aec{};
    auto flags = modify_aec_flags(flags_);
    aec.flags(flags);
    aec.bits_per_sample(bits_per_sample_);
    aec.block_size(block_size_);
    aec.rsi(rsi_);
    aec.n_elems(n_elems_);

    typename AecDecompressor<SimpleValueType>::Values simple_values = aec.decode(in_buf);
    
    if (aec.offsets()) {
      offsets_ = aec.offsets().value();
    }

    Values values(simple_values.size());
    std::transform(simple_values.begin(), simple_values.end(), values.begin(), [&](const auto& simple_value) {
        return (simple_value * bscale + reference_value_) * dscale;
    });

    return values;
}
// ---------------------------------------------------------------------------------------------------------------------
// Explicit instantiations of the template class
template class CcsdsDecompressor<double>;

} // namespace mc
