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
namespace gribjump::mc {

class AecParams {
public:

    using Offsets = std::vector<size_t>;
    AecParams() : flags_(8ul), rsi_(128ul), block_size_(32ul), bits_per_sample_(16ul) {}

    size_t flags() const { return flags_; }
    size_t rsi() const { return rsi_; }
    size_t block_size() const { return block_size_; }
    size_t bits_per_sample() const { return bits_per_sample_; }
    std::optional<Offsets> offsets() const {
        return offsets_.size() > 0 ? std::optional<Offsets>(offsets_) : std::nullopt;
    }

    AecParams& flags(size_t flags) {
        flags_ = flags;
        return *this;
    }
    AecParams& rsi(size_t rsi) {
        rsi_ = rsi;
        return *this;
    }
    AecParams& block_size(size_t block_size) {
        block_size_ = block_size;
        return *this;
    }
    AecParams& bits_per_sample(size_t bits_per_sample) {
        bits_per_sample_ = bits_per_sample;
        return *this;
    }
    AecParams& offsets(const Offsets& offsets) {
        assert(offsets.size() > 0);
        offsets_ = offsets;
        return *this;
    }

protected:

    size_t flags_;
    size_t rsi_;
    size_t block_size_;
    size_t bits_per_sample_;
    std::vector<size_t> offsets_;
};

template <typename ValueType>
class AecDecompressor;

template <typename ValueType>
class AecDecompressor : public NumericDecompressor<ValueType>, public AecParams {

public:  // types

    static_assert(std::is_integral<ValueType>::value, "AecDecompressor only supports integral types");
    using CompressedData = typename NumericCompressor<ValueType>::CompressedData;
    using Values         = typename NumericCompressor<ValueType>::Values;

public:  // methods

    Values decode(const CompressedData& encoded) override;

    Values decode(const std::shared_ptr<DataAccessor> accessor, const Block& range) override;

    Offsets decode_offsets(const CompressedData& encoded) override;

    size_t n_elems() const { return n_elems_; }
    AecDecompressor& n_elems(size_t n_elems) {
        n_elems_ = n_elems;
        return *this;
    }

private:  // methods

    void validateBitsPerSample();

private:  // members

    size_t n_elems_;
};


}  // namespace gribjump::mc
