/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <optional>

#include "gribjump/compression/NumericCompressor.h"
#include "gribjump/compression/compressors/Aec.h"

namespace gribjump::mc {

/// @todo can we unify this with AecParams? There is a lot of overlap.
class CcsdsParams {
public:

    using Offsets = std::vector<size_t>;
    CcsdsParams() : flags_(8ul), rsi_(128ul), block_size_(32ul), bits_per_sample_(16ul) {}

    size_t flags() const { return flags_; }
    size_t rsi() const { return rsi_; }
    size_t block_size() const { return block_size_; }
    size_t bits_per_sample() const { return bits_per_sample_; }
    double reference_value() const { return reference_value_; }
    size_t decimal_scale_factor() const { return decimal_scale_factor_; }
    size_t binary_scale_factor() const { return binary_scale_factor_; }
    std::optional<Offsets> offsets() const {
        return offsets_.size() > 0 ? std::optional<Offsets>(offsets_) : std::nullopt;
    }

    CcsdsParams& flags(size_t flags) {
        flags_ = flags;
        return *this;
    }
    CcsdsParams& rsi(size_t rsi) {
        rsi_ = rsi;
        return *this;
    }
    CcsdsParams& block_size(size_t block_size) {
        block_size_ = block_size;
        return *this;
    }
    CcsdsParams& bits_per_sample(size_t bits_per_sample) {
        bits_per_sample_ = bits_per_sample;
        return *this;
    }
    CcsdsParams& reference_value(double reference_value) {
        reference_value_ = reference_value;
        return *this;
    }
    CcsdsParams& decimal_scale_factor(size_t decimal_scale_factor) {
        decimal_scale_factor_ = decimal_scale_factor;
        return *this;
    }
    CcsdsParams& binary_scale_factor(size_t binary_scale_factor) {
        binary_scale_factor_ = binary_scale_factor;
        return *this;
    }
    CcsdsParams& offsets(const Offsets& offsets) {
        ASSERT(offsets.size() > 0);
        offsets_ = offsets;
        return *this;
    }

protected:

    size_t flags_;
    size_t rsi_;
    size_t block_size_;
    size_t bits_per_sample_;
    double reference_value_;
    size_t decimal_scale_factor_;
    size_t binary_scale_factor_;
    std::vector<size_t> offsets_;
};

template <typename ValueType>
class CcsdsDecompressor : public mc::NumericDecompressor<ValueType>, public CcsdsParams {

public:  // types

    using CompressedData = typename mc::NumericDecompressor<ValueType>::CompressedData;
    using Values         = typename mc::NumericDecompressor<ValueType>::Values;

public:  // methods

    using NumericDecompressor<ValueType>::decode;

    /// Decode a compressed buffer
    Values decode(const eckit::Buffer& in_buf) override;

    /// Decode a range of values
    Values decode(const std::shared_ptr<DataAccessor> accessor, const Block& range) override;

    /// Avoid fully decoding the data, only decode the offsets
    /// @note: libaec will still do its part of the decoding
    Offsets decode_offsets(const eckit::Buffer& in_buf) override;

    size_t n_elems() const { return n_elems_; }
    CcsdsDecompressor& n_elems(size_t n_elems) {
        n_elems_ = n_elems;
        return *this;
    }

private:  // methods

    size_t sample_nbytes() const;

    // Wrappers around aec.decode

    template <typename SimpleValueType>
    Values decode_range_(const std::shared_ptr<DataAccessor> accessor, const Block& simple_range, double bscale,
                         double dscale);

    template <typename SimpleValueType>
    Offsets decode_offsets_(const typename AecDecompressor<SimpleValueType>::CompressedData& in_buf);

    template <typename SimpleValueType>
    Values decode_all_(const typename AecDecompressor<SimpleValueType>::CompressedData& in_buf, double bscale,
                       double dscale);

private:  // members

    size_t n_elems_;
};
}  // namespace gribjump::mc
