// Copyright 2023 <Eugen Betke <eugen.betke@ecmwf.int>

#pragma once

#include "../NumericCompressor.h"
#include "Aec.h"

#include <eckit/io/Buffer.h>
#include <eckit/serialisation/MemoryStream.h>

#include <stdlib.h>
#include <optional>
#include <cassert>

#pragma once


namespace mc {

/* Return n to the power of s */
template <typename T>
constexpr T codes_power(long s, long n)
{
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


bool is_big_endian();
size_t modify_aec_flags(size_t flags);


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

  CcsdsParams& flags(size_t flags) { flags_ = flags; return *this; }
  CcsdsParams& rsi(size_t rsi) { rsi_ = rsi; return *this; }
  CcsdsParams& block_size(size_t block_size) { block_size_ = block_size; return *this; }
  CcsdsParams& bits_per_sample(size_t bits_per_sample) { bits_per_sample_ = bits_per_sample; return *this; }
  CcsdsParams& reference_value(double reference_value) { reference_value_ = reference_value; return *this; }
  CcsdsParams& decimal_scale_factor(size_t decimal_scale_factor) { decimal_scale_factor_ = decimal_scale_factor; return *this; }
  CcsdsParams& binary_scale_factor(size_t binary_scale_factor) { binary_scale_factor_ = binary_scale_factor; return *this; }
  CcsdsParams& offsets(const Offsets& offsets) {
    assert(offsets.size() > 0);
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

template <typename ValueType> class CcsdsCompressor;
template <typename ValueType> class CcsdsDecompressor;

template <typename ValueType>
class CcsdsCompressor : public NumericCompressor<ValueType>, public CcsdsParams {
public:
  using CompressedData = typename NumericCompressor<ValueType>::CompressedData;
  using Values = typename NumericCompressor<ValueType>::Values;
  std::pair<std::unique_ptr<NumericDecompressor<ValueType>>, CompressedData> encode(const Values& values) override {
    size_t nbytes = (bits_per_sample_ + 7) / 8;
    if (nbytes == 3)
      nbytes = 4;

    CcsdsCompressor<ValueType> ccsds_decompressor{};
    CcsdsCompressor<ValueType>::CompressedData data{};
    switch (nbytes) {
      case 1:
        std::tie(ccsds_decompressor, data) = encode_all_<uint8_t>(values, nbytes);
        break;
      case 2:
        std::tie(ccsds_decompressor, data) = encode_all_<uint16_t>(values, nbytes);
        break;
      case 4:
        std::tie(ccsds_decompressor, data) = encode_all_<uint32_t>(values, nbytes);
        break;
      default:
        throw std::runtime_error("Unsupported number of bytes per value");
    }

    //return std::make_pair(std::move(ccsds_decompressor), std::move(data)); 
    return std::make_pair(ccsds_decompressor, data); 
  }

private:
  template <typename SimpleValueType>
  std::pair<CcsdsDecompressor<ValueType>, CompressedData> encode_all_(const Values& values, size_t nbytes) {
    ValueType divisor = codes_power<ValueType>(binary_scale_factor_, 2);
    ValueType d = codes_power<ValueType>(decimal_scale_factor_, 10);
    std::vector<SimpleValueType> simple_values(values.size());

    std::transform(values.begin(), values.end(), simple_values.begin(), [&](const auto& value) {
      return ((value * d - reference_value_) * divisor) + 0.5;
    });

    AecCompressor<ValueType> aec{};
    aec.flags(flags_);
    aec.bits_per_sample(bits_per_sample_);
    aec.block_size(block_size_);
    aec.rsi(rsi_);

    auto [aec_decompressor_base, aec_data] = aec.encode(simple_values);
    AecDecompressor<ValueType> aec_decompressor = dynamic_cast<AecDecompressor<ValueType>&>(*aec_decompressor_base);

    std::unique_ptr<CcsdsDecompressor<ValueType>> ccsds_decompressor = std::make_unique<CcsdsDecompressor<ValueType>>();
    ccsds_decompressor->flags(aec_decompressor.flags());
    ccsds_decompressor->rsi(aec_decompressor.rsi());
    ccsds_decompressor->block_size(aec_decompressor.block_size());
    ccsds_decompressor->bits_per_sample(aec_decompressor.bits_per_sample());
    ccsds_decompressor->reference_value(reference_value_);
    ccsds_decompressor->decimal_scale_factor(decimal_scale_factor_);
    ccsds_decompressor->binary_scale_factor(binary_scale_factor_);
    if (aec_decompressor.offsets()) {
      ccsds_decompressor->offsets(aec_decompressor.offsets().value());
    }
    ccsds_decompressor->n_elems(aec_decompressor.n_elems());
    //return std::make_pair(std::move(ccsds_decompressor), std::move(aec_data));
    return std::make_pair(ccsds_decompressor, aec_data);
  }

};


template <typename ValueType>
class CcsdsDecompressor : public mc::NumericDecompressor<ValueType>, public CcsdsParams {
public:
  using CompressedData = typename mc::NumericDecompressor<ValueType>::CompressedData;
  using Values = typename mc::NumericDecompressor<ValueType>::Values;

  Values decode(const CompressedData& in_buf) override
  {
    auto bscale = codes_power<double>(binary_scale_factor_, 2);
    auto dscale = codes_power<double>(decimal_scale_factor_, -10);
    auto flags = modify_aec_flags(flags_);

    size_t nbytes = (bits_per_sample_ + 7) / 8;
    if (nbytes == 3)
      nbytes = 4;

    Values values;

    size_t i;
    switch (nbytes) {
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
        throw std::runtime_error("Invalid number of bytes per sample");
    }

    return values;
  }


  Values decode(const std::shared_ptr<DataAccessor> accessor, const Range& range) override {
    if (range.second == 0)
      return Values{};

    size_t simple_nbytes = (bits_per_sample_ + 7) / 8;
    if (simple_nbytes == 3)
      simple_nbytes = 4;

    size_t value_nbytes = sizeof(double);
    auto bscale = codes_power<double>(binary_scale_factor_, 2);
    auto dscale = codes_power<double>(decimal_scale_factor_, -10);
    Values values;

    switch (simple_nbytes) {
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
        throw std::runtime_error("Invalid number of bytes per sample");
    }

    return values;
  }

  using NumericDecompressor<ValueType>::decode;

  size_t n_elems() const { return n_elems_; }
  CcsdsDecompressor& n_elems(size_t n_elems) { n_elems_ = n_elems; return *this; }

private:
  size_t n_elems_;

  template <typename SimpleValueType>
  Values decode_range_ (const std::shared_ptr<DataAccessor> accessor, const Range& simple_range, double bscale, double dscale) {
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


  template <typename SimpleValueType>
  Values decode_all_ (const typename AecDecompressor<SimpleValueType>::CompressedData& in_buf, double bscale, double dscale) {
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

};
} // namespace mc

