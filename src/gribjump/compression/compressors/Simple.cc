/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "gribjump/compression/compressors/Simple.h"
#include "gribjump/compression/compressors/algorithms/SimplePacking.h"

namespace  {

size_t bin_pos(size_t value_idx, size_t bits_per_value) {
    constexpr size_t chunk_nvals = 8;
    size_t chunk_size = bits_per_value;
    size_t chunk_idx = value_idx / chunk_nvals;
    size_t value_idx_first_in_chunk = chunk_idx * chunk_nvals;
    size_t chunk_offset_bytes = value_idx_first_in_chunk * bits_per_value / chunk_nvals;

    return chunk_offset_bytes;
}
    
} // namespace

namespace mc {

template <typename ValueType>
typename SimpleDecompressor<ValueType>::Values SimpleDecompressor<ValueType>::decode(const std::shared_ptr<DataAccessor> accessor, const Block& range){
    using SP = SimplePacking<ValueType>;
    SimplePacking<double> sp{};
    DecodeParameters<ValueType> params{};
    constexpr int VSIZE = sizeof(ValueType);

    auto [offset, size] = range;
    params.bits_per_value = bits_per_value_;
    params.reference_value = reference_value_;
    params.binary_scale_factor = binary_scale_factor_;
    params.decimal_scale_factor = decimal_scale_factor_;

    constexpr size_t chunk_nvals = 8;
    size_t new_offset = offset / chunk_nvals * chunk_nvals;
    size_t end = offset + size;
    size_t new_end = (end + (chunk_nvals - 1)) / chunk_nvals * chunk_nvals;
    size_t new_size = new_end - new_offset;
    Block inclusive_range{new_offset, new_size};

    params.n_vals = new_size;

    size_t last_pos = std::min(bin_pos(new_size, bits_per_value_), accessor->eof() - bin_pos(new_offset, bits_per_value_));
    Block data_range{bin_pos(new_offset, bits_per_value_), last_pos};
    eckit::Buffer compressed = accessor->read(data_range);

    typename SP::Buffer data{(unsigned char*) compressed.data(), (unsigned char*) compressed.data() + data_range.second};
    typename SP::Values decoded = sp.unpack(params, data);

    size_t shift_offset = offset - new_offset;
    Values out_values{decoded.data() + shift_offset, decoded.data() + shift_offset + size};

    return out_values;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename SimpleDecompressor<ValueType>::Values SimpleDecompressor<ValueType>::decode(const typename NumericDecompressor<ValueType>::CompressedData& in_buf){
    
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
    
    /// @todo: this line looks very strange to me.
    Values out_buf{decoded_data, decoded_data + decoded.size()}; 
    //Values out_buf{decoded.data(), decoded.size() * sizeof(ValueType)};

    return out_buf;
}


// ---------------------------------------------------------------------------------------------------------------------
// Explicit instantiations of the template class
template class SimpleDecompressor<double>;

} // namespace mc