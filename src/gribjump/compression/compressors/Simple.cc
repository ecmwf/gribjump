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
#include "gribjump/compression/compressors/SimplePacking.h"

namespace gribjump::mc {

template <typename ValueType>
typename SimpleDecompressor<ValueType>::Values SimpleDecompressor<ValueType>::decode(
    const std::shared_ptr<DataAccessor> accessor, const Block& range) {
    using SP = SimplePacking<ValueType>;
    SimplePacking<double> sp{};
    DecodeParameters<ValueType> params{};
    constexpr int VSIZE = sizeof(ValueType);

    auto [offset, size]         = range;
    params.bits_per_value       = bits_per_value_;
    params.reference_value      = reference_value_;
    params.binary_scale_factor  = binary_scale_factor_;
    params.decimal_scale_factor = decimal_scale_factor_;

    size_t end_offset = offset + size;
    params.n_vals     = size;

    // Convert from index ranges to byte ranges
    size_t start_offset_bytes = (offset * bits_per_value_) / 8;          // Round down to the nearest byte
    size_t skipbits           = (offset * bits_per_value_) % 8;          // bits to skip to get to the first value
    size_t end_offset_bytes   = (end_offset * bits_per_value_ + 7) / 8;  // Round up to the nearest byte
    size_t size_bytes         = end_offset_bytes - start_offset_bytes;

    eckit::Buffer encoded       = accessor->read({start_offset_bytes, size_bytes});
    typename SP::Values decoded = sp.unpack(params, encoded, skipbits);

    return decoded;
}

// ---------------------------------------------------------------------------------------------------------------------
template <typename ValueType>
typename SimpleDecompressor<ValueType>::Values SimpleDecompressor<ValueType>::decode(
    const typename NumericDecompressor<ValueType>::CompressedData& in_buf) {
    NOTIMP;  //< I believe the implementation is wrong/untested
    // using SP = SimplePacking<ValueType>;
    // SP sp{};
    // DecodeParameters<ValueType> params{};
    // params.bits_per_value = bits_per_value_;
    // params.reference_value = reference_value_;
    // params.binary_scale_factor = binary_scale_factor_;
    // params.decimal_scale_factor = decimal_scale_factor_;
    // params.n_vals = in_buf.size() * 8 / bits_per_value_;

    // typename SP::Buffer data{(unsigned char*) in_buf.data(), (unsigned char*) in_buf.data() + params.n_vals}; // <--
    // I don't think the size is correct here typename SP::Values decoded = sp.unpack(params, data);

    // auto decoded_data = reinterpret_cast<ValueType*>(decoded.data());

    // /// @todo: this line looks very strange to me.
    // Values out_buf{decoded_data, decoded_data + decoded.size()};
    // //Values out_buf{decoded.data(), decoded.size() * sizeof(ValueType)};

    // return out_buf;
}

// ---------------------------------------------------------------------------------------------------------------------
// Explicit instantiations of the template class
template class SimpleDecompressor<double>;

}  // namespace gribjump::mc