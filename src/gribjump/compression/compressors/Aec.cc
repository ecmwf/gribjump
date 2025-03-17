/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/compression/compressors/Aec.h"

#include <bitset>
#include <algorithm>

#include <libaec.h>

#define AEC_CALL(a) aec_call(a, #a)

namespace {

void aec_call(int err, std::string func) {
    if (err != AEC_OK) {
        std::string msg = "Error code: " + std::to_string(err);
        throw eckit::FailedLibraryCall("libaec", func, msg, Here());
    }
}

// debugging
[[maybe_unused]]
void print_aec_stream_info(struct aec_stream* strm, const char* func) {
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.flags=" << strm->flags << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.bits_per_sample=" << strm->bits_per_sample << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.block_size=" << strm->block_size << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.rsi=" << strm->rsi << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.avail_out=" << strm->avail_out << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.avail_in=" << strm->avail_in << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.next_out=" << strm->next_out << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.next_in=" << strm->next_in << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.total_in=" << strm->total_in << std::endl;
    eckit::Log::info() << "DEBUG CCSDS " << func << " aec_stream.total_out=" << strm->total_out << std::endl;
    eckit::Log::info() << std::endl;
}

[[maybe_unused]]
void print_binary(const char* name, const void* data, size_t n_bytes) {
    eckit::Log::info() << name << ": ";
    for (size_t i = 0; i < n_bytes; ++i) {
        eckit::Log::info() << std::bitset<8>(static_cast<const char*>(data)[i]) << " ";
    }
    eckit::Log::info() << std::endl;
}

} // namespace

namespace gribjump::mc {

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename NumericCompressor<ValueType>::Values AecDecompressor<ValueType>::decode(const typename NumericCompressor<ValueType>::CompressedData& encoded) {
    
    validateBitsPerSample();
    
    Values decoded(n_elems_);

    struct aec_stream strm;
    strm.rsi = rsi_;
    strm.block_size = block_size_;
    strm.bits_per_sample = bits_per_sample_;
    strm.flags = flags_;
    strm.avail_in  = encoded.size();
    strm.next_in   = reinterpret_cast<const unsigned char*>(encoded.data());
    strm.avail_out = decoded.size() * sizeof(ValueType);
    strm.next_out  = reinterpret_cast<unsigned char*>(decoded.data());
    
    AEC_CALL(aec_decode_init(&strm));
    AEC_CALL(aec_decode_enable_offsets(&strm));
    AEC_CALL(aec_decode(&strm, AEC_FLUSH));
    
    size_t offsets_count = 0;
    AEC_CALL(aec_decode_count_offsets(&strm, &offsets_count));
    
    std::vector<size_t> offsets(offsets_count);
    AEC_CALL(aec_decode_get_offsets(&strm, offsets.data(), offsets.size()));
    
    AEC_CALL(aec_decode_end(&strm));

    offsets_ = std::move(offsets);
    return decoded;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename NumericCompressor<ValueType>::Values AecDecompressor<ValueType>::decode(const std::shared_ptr<DataAccessor> accessor, const Block& range) {
    
    validateBitsPerSample();

    ASSERT(!offsets_.empty());

    auto [range_offset, range_size] = range;
    auto range_offset_bytes = range_offset * sizeof(ValueType);
    auto range_size_bytes   = range_size * sizeof(ValueType);

    Values decoded(range_size);

    size_t nbytes = (bits_per_sample_ + 7) / 8;
    if (nbytes == 3) {
      nbytes = 4;
    }
    size_t rsi_size_bytes = rsi_ * block_size_ * nbytes;

    auto start_idx = range_offset_bytes / rsi_size_bytes;
    auto end_idx   = (range_offset_bytes + range_size_bytes) / rsi_size_bytes + 1;

    ASSERT(start_idx < end_idx);
    ASSERT(end_idx <= offsets_.size());

    size_t start_offset_bits = offsets_[start_idx];
    size_t start_offset_bytes = start_offset_bits / 8;
    // size_t end_offset_bits = end_idx == offsets_.size() ? accessor->eof() * 8 : offsets_[end_idx];
    size_t end_offset_bytes = end_idx == offsets_.size() ? accessor->eof() : (offsets_[end_idx] + 7) / 8;

    // The new offset to the RSI may not be aligned to the start of the range, so we need to shift the offsets by the difference
    /// @todo It seems unlikely that two copies are required here.
    std::vector<size_t> new_offsets_tmp;
    std::vector<size_t> new_offsets;
    std::copy(offsets_.begin() + start_idx, offsets_.end(), std::back_inserter(new_offsets_tmp));
    size_t shift = start_offset_bits >> 3 << 3; // align to byte boundary
    std::transform(new_offsets_tmp.begin(), new_offsets_tmp.end(), std::back_inserter(new_offsets), [shift] (size_t offset) {
      return offset - shift;
    });

    ASSERT(!new_offsets.empty());

    // Read RSIs
    CompressedData encoded = accessor->read({start_offset_bytes, end_offset_bytes - start_offset_bytes});

    struct aec_stream strm;
    strm.rsi = rsi_;
    strm.block_size = block_size_;
    strm.bits_per_sample = bits_per_sample_;
    strm.flags = flags_;
    strm.avail_in  = encoded.size();
    strm.next_in   = reinterpret_cast<const unsigned char*>(encoded.data());
    strm.avail_out = decoded.size() * sizeof(ValueType);
    strm.next_out  = reinterpret_cast<unsigned char*>(decoded.data());

    size_t new_offset_bytes = range_offset_bytes - rsi_size_bytes * start_idx;
    size_t new_size_bytes = range_size_bytes;

    AEC_CALL(aec_decode_init(&strm));
    AEC_CALL(aec_decode_range(&strm, new_offsets.data(), new_offsets.size(), new_offset_bytes, new_size_bytes));
    AEC_CALL(aec_decode_end(&strm));

    return decoded;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
AecParams::Offsets AecDecompressor<ValueType>::decode_offsets(const typename NumericCompressor<ValueType>::CompressedData& encoded) {
    decode(encoded); // sets offsets_
    return std::move(offsets_);
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
void AecDecompressor<ValueType>::validateBitsPerSample() {
    if (sizeof(ValueType) == 1 && !(bits_per_sample_ > 0 && bits_per_sample_ <= 8))
        throw eckit::Exception("bits_per_sample must be between 1 and 8 for 1-byte types", Here());
    if (sizeof(ValueType) == 2 && !(bits_per_sample_ > 8 && bits_per_sample_ <= 16))
        throw eckit::Exception("bits_per_sample must be between 9 and 16 for 2-byte types", Here());
    if (sizeof(ValueType) == 4 && !(bits_per_sample_ > 16 && bits_per_sample_ <= 32))
        throw eckit::Exception("bits_per_sample must be between 17 and 32 for 4-byte types", Here());
}

// ---------------------------------------------------------------------------------------------------------------------
// Explicit instantiations of the template class
template class AecDecompressor<uint8_t>;
template class AecDecompressor<uint16_t>;
template class AecDecompressor<uint32_t>;

} // namespace gribjump::mc
