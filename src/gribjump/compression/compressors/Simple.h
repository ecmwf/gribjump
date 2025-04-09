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

#include "gribjump/compression/NumericCompressor.h"

namespace gribjump::mc {

class SimpleParams {
public:

    using Offsets = std::vector<size_t>;
    SimpleParams() :
        bits_per_value_(16ul), reference_value_(0.0), decimal_scale_factor_(0ul), binary_scale_factor_(0ul) {}

    size_t bits_per_value() const { return bits_per_value_; }
    double reference_value() const { return reference_value_; }
    long decimal_scale_factor() const { return decimal_scale_factor_; }
    long binary_scale_factor() const { return binary_scale_factor_; }

    SimpleParams& bits_per_value(size_t bits_per_value) {
        bits_per_value_ = bits_per_value;
        return *this;
    }
    SimpleParams& reference_value(double reference_value) {
        reference_value_ = reference_value;
        return *this;
    }
    SimpleParams& decimal_scale_factor(long decimal_scale_factor) {
        decimal_scale_factor_ = decimal_scale_factor;
        return *this;
    }
    SimpleParams& binary_scale_factor(long binary_scale_factor) {
        binary_scale_factor_ = binary_scale_factor;
        return *this;
    }

protected:

    size_t bits_per_value_;
    double reference_value_;
    long decimal_scale_factor_;
    long binary_scale_factor_;
};

template <typename ValueType>
class SimpleDecompressor;

template <typename ValueType>
class SimpleDecompressor : public NumericDecompressor<ValueType>, public SimpleParams {
public:

    using CompressedData = typename NumericDecompressor<ValueType>::CompressedData;
    using Values         = typename NumericDecompressor<ValueType>::Values;
    using NumericDecompressor<ValueType>::decode;

    Values decode(const CompressedData& in_buf) override;

    Values decode(const std::shared_ptr<DataAccessor> accessor, const Block& range) override;

    // getters and setters

    size_t buffer_size() const { return buffer_size_; }

    SimpleDecompressor& buffer_size(size_t buffer_size) {
        buffer_size_ = buffer_size;
        return *this;
    }

private:

    size_t buffer_size_;
};

}  // namespace gribjump::mc
