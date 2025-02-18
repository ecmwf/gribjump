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

#include "../NumericCompressor.h"

#include <eckit/io/Buffer.h>
#include <eckit/serialisation/MemoryStream.h>
#include "eckit/exception/Exceptions.h"

#include <stdlib.h>
#include <optional>
#include <cassert>
#include <algorithm>

#include <libaec.h>
#include <unordered_map>
#include <memory>
#include <cassert>
#include <bitset>


namespace mc {

void print_binary(const char* name, const void* data, size_t n_bytes);
void print_aec_stream_info(struct aec_stream* strm, const char* func);

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

  AecParams& flags(size_t flags) { flags_ = flags; return *this; }
  AecParams& rsi(size_t rsi) { rsi_ = rsi; return *this; }
  AecParams& block_size(size_t block_size) { block_size_ = block_size; return *this; }
  AecParams& bits_per_sample(size_t bits_per_sample) { bits_per_sample_ = bits_per_sample; return *this; }
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

template <typename ValueType> class AecCompressor;
template <typename ValueType> class AecDecompressor;

template <typename ValueType>
class AecCompressor : public NumericCompressor<ValueType>, public AecParams {
public:
  static_assert(std::is_integral<ValueType>::value, "AecDecompressor only supports integral types");
  static_assert(sizeof(ValueType) == 1 || sizeof(ValueType) == 2 || sizeof(ValueType) == 4, "AecDecompressor only supports 1, 2 or 4 byte integral types");
  using CompressedData = typename NumericCompressor<ValueType>::CompressedData;
  using Values = typename NumericCompressor<ValueType>::Values;

  std::pair<std::unique_ptr<NumericDecompressor<ValueType>>, CompressedData> encode(const Values& in_buf) override
  {
    if (sizeof(ValueType) == 1 && !(bits_per_sample_ > 0 && bits_per_sample_ <= 8))
      throw eckit::BadValue("bits_per_sample must be between 1 and 8 for 1-byte types", Here());
    if (sizeof(ValueType) == 2 && !(bits_per_sample_ > 8 && bits_per_sample_ <= 16))
      throw eckit::BadValue("bits_per_sample must be between 9 and 16 for 2-byte types", Here());
    if (sizeof(ValueType) == 4 && !(bits_per_sample_ > 16 && bits_per_sample_ <= 32))
      throw eckit::BadValue("bits_per_sample must be between 17 and 32 for 4-byte types", Here());

    const size_t n_elems{in_buf.size()};
    CompressedData out_buf{n_elems * sizeof(ValueType) * 2 + 1000};

    struct aec_stream strm;
    strm.flags           = flags_;
    strm.bits_per_sample = bits_per_sample_;
    strm.block_size      = block_size_;
    strm.rsi             = rsi_;
    strm.avail_in        = n_elems * sizeof(ValueType);
    strm.next_in         = reinterpret_cast<const unsigned char*>(in_buf.data());
    strm.avail_out       = out_buf.size();
    strm.next_out        = reinterpret_cast<unsigned char*>(out_buf.data());

    //print_aec_stream_info(&strm, "Compressing");

    if (aec_encode_init(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode_init", "Error initializing encoder", Here());

    if (aec_encode_enable_offsets(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode_enable_offsets", "Error enabling offsets", Here());

    if (aec_encode(&strm, AEC_FLUSH) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode", "Error encoding buffer", Here());

    size_t offsets_count{0};
    if (aec_encode_count_offsets(&strm, &offsets_count) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode_count_offsets", "Error counting offsets", Here());

    std::vector<size_t> offsets(offsets_count);
    if (aec_encode_get_offsets(&strm, offsets.data(), offsets.size()) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode_get_offsets", "Error getting offsets", Here());

    if (aec_encode_end(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_encode_end", "Error ending encoding", Here());

    auto decompressor = std::unique_ptr<AecDecompressor<ValueType>>(new AecDecompressor<ValueType>{});
    decompressor->flags(flags_);
    decompressor->bits_per_sample(bits_per_sample_);
    decompressor->block_size(block_size_);
    decompressor->rsi(rsi_);
    decompressor->offsets(offsets);
    decompressor->n_elems(in_buf.size());

    return std::make_pair(std::move(decompressor), CompressedData{out_buf.data(), strm.total_out});
  }
};


template <typename ValueType>
class AecDecompressor : public NumericDecompressor<ValueType>, public AecParams {
public:
  static_assert(std::is_integral<ValueType>::value, "AecDecompressor only supports integral types");


  using CompressedData = typename NumericCompressor<ValueType>::CompressedData;
  using Values = typename NumericCompressor<ValueType>::Values;

  Offsets decode_offsets(const CompressedData& encoded) override {
    decode(encoded);
    return std::move(offsets_);
  }

  Values decode(const CompressedData& encoded) override {
    if (sizeof(ValueType) == 1 && !(bits_per_sample_ > 0 && bits_per_sample_ <= 8))
      throw eckit::BadValue("bits_per_sample must be between 1 and 8 for 1-byte types", Here());
    if (sizeof(ValueType) == 2 && !(bits_per_sample_ > 8 && bits_per_sample_ <= 16))
      throw eckit::BadValue("bits_per_sample must be between 9 and 16 for 2-byte types", Here());
    if (sizeof(ValueType) == 4 && !(bits_per_sample_ > 16 && bits_per_sample_ <= 32))
      throw eckit::BadValue("bits_per_sample must be between 17 and 32 for 4-byte types", Here());

    Values decoded(n_elems_);

    //print_binary("decode_e      ", encoded.data(), encoded.size());

    struct aec_stream strm;
    strm.rsi = rsi_;
    strm.block_size = block_size_;
    strm.bits_per_sample = bits_per_sample_;
    strm.flags = flags_;
    strm.avail_in  = encoded.size();
    strm.next_in   = reinterpret_cast<const unsigned char*>(encoded.data());
    strm.avail_out = decoded.size() * sizeof(ValueType);
    strm.next_out  = reinterpret_cast<unsigned char*>(decoded.data());

    //print_aec_stream_info(&strm, "Decompressing");
    if (aec_decode_init(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_init", "Error initializing decoder", Here());

    if (aec_decode_enable_offsets(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_enable_offsets", "Error enabling offsets", Here());

    if (aec_decode(&strm, AEC_FLUSH) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode", "Error decoding buffer", Here());

    size_t offsets_count{0};
    if (aec_decode_count_offsets(&strm, &offsets_count) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_count_offsets", "Error counting offsets", Here());

    std::vector<size_t> offsets(offsets_count);
    if (aec_decode_get_offsets(&strm, offsets.data(), offsets.size()) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_get_offsets", "Error getting offsets", Here());

    //print_aec_stream_info(&strm, "Decompressing");

    if (aec_decode_end(&strm) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_end", "Error ending decoder", Here());

    offsets_ = std::move(offsets);
    return decoded;
  }


  Values decode(const std::shared_ptr<DataAccessor> accessor, const Block& range) override
  {
    if (sizeof(ValueType) == 1 && !(bits_per_sample_ > 0 && bits_per_sample_ <= 8))
      throw eckit::Exception("bits_per_sample must be between 1 and 8 for 1-byte types", Here());
    if (sizeof(ValueType) == 2 && !(bits_per_sample_ > 8 && bits_per_sample_ <= 16))
      throw eckit::Exception("bits_per_sample must be between 9 and 16 for 2-byte types", Here());
    if (sizeof(ValueType) == 4 && !(bits_per_sample_ > 16 && bits_per_sample_ <= 32))
      throw eckit::Exception("bits_per_sample must be between 17 and 32 for 4-byte types", Here());

    if (offsets_.empty())
      throw eckit::Exception("No offsets found", Here());
      

    auto [range_offset, range_size] = range;
    auto range_offset_bytes = range_offset * sizeof(ValueType);
    auto range_size_bytes   = range_size * sizeof(ValueType);

    Values decoded(range_size);

    struct aec_stream strm;
    strm.rsi = rsi_;
    strm.block_size = block_size_;
    strm.bits_per_sample = bits_per_sample_;
    strm.flags = flags_;

    size_t nbytes = (strm.bits_per_sample + 7) / 8;
    if (nbytes == 3) {
      nbytes = 4;
    }
    size_t rsi_size_bytes = strm.rsi * strm.block_size * nbytes;

    auto start_idx = range_offset_bytes / rsi_size_bytes;
    auto end_idx   = (range_offset_bytes + range_size_bytes) / rsi_size_bytes + 1;

    assert(start_idx < end_idx);
    assert(end_idx <= offsets_.size());

    size_t start_offset_bits = offsets_[start_idx];
    size_t start_offset_bytes = start_offset_bits / 8;

    size_t end_offset_bits = end_idx == offsets_.size() ? accessor->eof() * 8 : offsets_[end_idx];
    size_t end_offset_bytes = end_idx == offsets_.size() ? accessor->eof() : (offsets_[end_idx] + 7) / 8;


    // The new offset to the RSI may not be aligned to the start of the range
    // so we need to shift the offsets by the difference
    std::vector<size_t> new_offsets_tmp;
    std::vector<size_t> new_offsets;
    std::copy(offsets_.begin() + start_idx, offsets_.end(), std::back_inserter(new_offsets_tmp));
    size_t shift = start_offset_bits >> 3 << 3;
    std::transform(new_offsets_tmp.begin(), new_offsets_tmp.end(), std::back_inserter(new_offsets), [shift] (size_t offset) {
      return offset - shift;
    });

    // Read RSIs
    CompressedData encoded = accessor->read({start_offset_bytes, end_offset_bytes - start_offset_bytes});

    //print_binary("decode_range_e", encoded.data(), encoded.size());

    strm.avail_in  = encoded.size();
    strm.next_in   = reinterpret_cast<const unsigned char*>(encoded.data());
    strm.avail_out = decoded.size() * sizeof(ValueType);
    strm.next_out  = reinterpret_cast<unsigned char*>(decoded.data());

    //print_aec_stream_info(&strm, "Decompressing");

    aec_decode_init(&strm);
    int err;
    size_t new_offset_bytes = range_offset_bytes - rsi_size_bytes * start_idx;
    size_t new_size_bytes = range_size_bytes;

    if (new_offsets.empty())
      throw eckit::Exception("AEC offsets list is empty", Here());

    if ((err = aec_decode_range(&strm, new_offsets.data(), new_offsets.size(), new_offset_bytes, new_size_bytes)) != AEC_OK)
      throw eckit::FailedLibraryCall("libaec", "aec_decode_range", "Error decoding buffer", Here());

    //print_aec_stream_info(&strm, "Decompressing");
    //print_binary("decode_range_d", decoded.data(), decoded.size());

    aec_decode_end(&strm);

    return decoded;
  }

  size_t n_elems() const { return n_elems; }
  AecDecompressor& n_elems(size_t n_elems) { n_elems_ = n_elems; return *this; }
private:
  size_t n_elems_;
};


} // namespace mc

