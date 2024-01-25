// Copyright 2023 <Eugen Betke <eugen.betke@ecmwf.int>

#pragma once

#include "../NumericCompressor.h"
#include "algorithms/SimplePacking.h"

#include <eckit/io/Buffer.h>
#include <eckit/serialisation/MemoryStream.h>

#include <stdlib.h>
#include <optional>
#include <cassert>

namespace mc {


template <typename ValueType>
size_t chunk_offset(size_t value_idx, size_t bits_per_value)
{
  size_t chunk_idx = value_idx / 8;
  size_t value_idx_first_in_chunk = chunk_idx * 8;
  assert(value_idx_first_in_chunk * bits_per_value % 8 == 0); // First value in a chunk starts always at a byte boundary
  size_t chunk_offset_bytes = value_idx_first_in_chunk * bits_per_value / 8;
  return chunk_offset_bytes;
}


class SimpleParams {
public:
  using Offsets = std::vector<size_t>;
  SimpleParams() : bits_per_value_(16ul), reference_value_(0.0), decimal_scale_factor_(0ul), binary_scale_factor_(0ul) {}

  size_t bits_per_value() const { return bits_per_value_; }
  double reference_value() const { return reference_value_; }
  long decimal_scale_factor() const { return decimal_scale_factor_; }
  long binary_scale_factor() const { return binary_scale_factor_; }

  SimpleParams& bits_per_value(size_t bits_per_value) { bits_per_value_ = bits_per_value; return *this; }
  SimpleParams& reference_value(double reference_value) { reference_value_ = reference_value; return *this; }
  SimpleParams& decimal_scale_factor(long decimal_scale_factor) { decimal_scale_factor_ = decimal_scale_factor; return *this; }
  SimpleParams& binary_scale_factor(long binary_scale_factor) { binary_scale_factor_ = binary_scale_factor; return *this; }

protected:
  size_t bits_per_value_;
  double reference_value_;
  long decimal_scale_factor_;
  long binary_scale_factor_;
};

template <typename ValueType> class SimpleCompressor;
template <typename ValueType> class SimpleDecompressor;

template <typename ValueType>
class SimpleCompressor : public NumericCompressor<ValueType>, public SimpleParams {
public:
  using CompressedData = typename NumericDecompressor<ValueType>::CompressedData;
  using Values = typename NumericDecompressor<ValueType>::Values;
  std::pair<std::unique_ptr<NumericDecompressor<ValueType>>, CompressedData> encode(const Values& values) override {
    using SP = SimplePacking<ValueType>;
    SP sp{};
    DecodeParameters<ValueType> params{};
    params.bits_per_value = bits_per_value_;
    params.reference_value = reference_value_;
    params.binary_scale_factor = binary_scale_factor_;
    params.decimal_scale_factor = decimal_scale_factor_;
    params.n_vals = values.size() * 8 / bits_per_value_;

    typename SP::Data data{(ValueType*) values.data(), (ValueType*) values.data() + values.size() / sizeof(ValueType)};
    auto [decode_params, encoded] = sp.pack(params, data);

    std::unique_ptr<SimpleDecompressor<ValueType>> decompressor{};
    decompressor->bits_per_value(decode_params.bits_per_value);
    decompressor->reference_value(decode_params.reference_value);
    decompressor->binary_scale_factor(decode_params.binary_scale_factor);
    decompressor->decimal_scale_factor(decode_params.decimal_scale_factor);

    CompressedData out_buf{encoded.data(), encoded.size()};

    return std::make_pair(std::move(decompressor), std::move(out_buf));
  }
};



// Decode
template <typename ValueType>
class SimpleDecompressor : public NumericDecompressor<ValueType>, public SimpleParams {
public:
  using CompressedData = typename NumericDecompressor<ValueType>::CompressedData;
  using Values = typename NumericDecompressor<ValueType>::Values;

  size_t buffer_size() const { return buffer_size_; }
  SimpleDecompressor& buffer_size(size_t buffer_size) { buffer_size_ = buffer_size; return *this; }

  Values decode(const std::shared_ptr<DataAccessor> accessor, const Range& range) override {
    //SP sp{};
    using SP = SimplePacking<ValueType>;
    SimplePacking<double> sp{};
    DecodeParameters<ValueType> params{};

    // SimplePacking parameters
    auto [offset, size] = range;

    // Optimization:
    // Requested values can start and end in the middle of a byte.
    // Chunks of 8 values start always at a byte boundary.
    size_t new_offset = offset / 8 * 8;
    size_t end = offset + size;
    size_t new_end = (end + 7) / 8 * 8;
    size_t max_end = accessor->eof() * 8 / bits_per_value_;
    size_t new_size = std::min(new_end - new_offset, max_end - new_offset);
    Range inclusive_range{new_offset, new_size}; // Includes extra values before and after the requested range

    size_t shift_offset = offset - new_offset; // Extra values before the requested range

    params.bits_per_value = bits_per_value_;
    params.reference_value = reference_value_;
    params.binary_scale_factor = binary_scale_factor_;
    params.decimal_scale_factor = decimal_scale_factor_;
    params.n_vals = new_size; // Includes extra values before and after the requested range

    // Byte level
    size_t begin_bytes = chunk_offset<ValueType>(new_offset, bits_per_value_);
    size_t end_bytes = std::min(chunk_offset<ValueType>(new_end, bits_per_value_), accessor->eof());
    Range data_range{begin_bytes, end_bytes - begin_bytes};
    eckit::Buffer compressed = accessor->read(data_range);

    typename SP::Buffer data{(unsigned char*) compressed.data(), (unsigned char*) compressed.data() + data_range.second};
    typename SP::Values decoded = sp.unpack(params, data);

    assert(shift_offset + size <= decoded.size());

    Values out_values{decoded.data() + shift_offset, decoded.data() + shift_offset + size};

    return out_values;
  }


  Values decode(const CompressedData& in_buf) override {
    using SP = SimplePacking<ValueType>;
    SP sp{};
    DecodeParameters<ValueType> params{};
    params.bits_per_value = bits_per_value_;
    params.reference_value = reference_value_;
    params.binary_scale_factor = binary_scale_factor_;
    params.decimal_scale_factor = decimal_scale_factor_;
    params.n_vals = in_buf.size() * 8 / bits_per_value_;

    typename SP::Buffer data{(unsigned char*) in_buf.data(), (unsigned char*) in_buf.data() + params.n_vals};
    typename SP::Values decoded = sp.unpack(params, data);

    auto decoded_data = reinterpret_cast<ValueType*>(decoded.data());
    Values out_buf{decoded_data, decoded_data + decoded.size()};
    //Values out_buf{decoded.data(), decoded.size() * sizeof(ValueType)};

    return out_buf;
  }

  using NumericDecompressor<ValueType>::decode;

private:
  size_t buffer_size_;
};

} // namespace mc
