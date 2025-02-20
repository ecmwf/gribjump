/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/compression/compressors/SimplePacking.h"
#include "gribjump/compression/NumericCompressor.h"
#include "eckit/exception/Exceptions.h"

namespace {

inline unsigned long bitmask(size_t x) {
    constexpr size_t maxNBits = sizeof(unsigned long) * 8;
    return (x == maxNBits) ? (unsigned long)-1UL : (1UL << x) - 1;
}

/// @note Appears to have been copied from eccodes...
template <typename T>
int decode_array(const unsigned char* p, long bitsPerValue,
                             double reference_value, double s, double d,
                             size_t n_vals, T* val) {
    unsigned long lvalue = 0;
    T x;

    if (bitsPerValue % 8 == 0) {
        /* See ECC-386 */
        int bc;
        int l    = bitsPerValue / 8;
        size_t o = 0;

        for (size_t i = 0; i < n_vals; i++) {
            lvalue = 0;
            lvalue <<= 8;
            lvalue |= p[o++];

            for (bc = 1; bc < l; bc++) {
                lvalue <<= 8;
                lvalue |= p[o++];
            }
            x      = ((lvalue * s) + reference_value) * d;
            val[i] = x;
        }
    }
    else {
        unsigned long mask = bitmask(bitsPerValue);

        /* pi: position of bitp in p[]. >>3 == /8 */
        long bitp = 0;
        long pi = 0;
        /* some bits might of the current byte at pi might be used */
        /* by the previous number usefulBitsInByte gives remaining unused bits */
        /* number of useful bits in current byte */
        int usefulBitsInByte = 8 - (bitp & 7);
        for (size_t i = 0; i < n_vals; i++) {
            /* value read as long */
            long bitsToRead = 0;
            lvalue          = 0;
            bitsToRead      = bitsPerValue;
            /* read one byte after the other to lvalue until >= bitsPerValue are read */
            while (bitsToRead > 0) {
                lvalue <<= 8;
                lvalue += p[pi];
                pi++;
                bitsToRead -= usefulBitsInByte;
                usefulBitsInByte = 8;
            }
            bitp += bitsPerValue;
            /* bitsToRead is now <= 0, remove the last bits */
            lvalue >>= -1 * bitsToRead;
            /* set leading bits to 0 - removing bits used for previous number */
            lvalue &= mask;

            usefulBitsInByte = -1 * bitsToRead; /* prepare for next round */
            if (usefulBitsInByte > 0) {
                pi--; /* reread the current byte */
            }
            else {
                usefulBitsInByte = 8; /* start with next full byte */
            }
            /* scaling and move value to output */
            x      = ((lvalue * s) + reference_value) * d;
            val[i] = x;
        }
    }
    return 0;
}

} // namespace

namespace gribjump::mc {

template <typename ValueType>
typename SimplePacking<ValueType>::Values SimplePacking<ValueType>::unpack(
    const DecodeParameters<ValueType>& params,
    const Buffer& buf) {

    if (params.bits_per_value > (sizeof(long) * 8)) {
        throw eckit::BadValue("Invalid BPV", Here());
    }

    // Empty field
    if (params.n_vals == 0) {
        return Values{};
    }
    
    /// @todo: I think there is already logic for checking for constant fields upstream of this code.
    // Constant field
    if (params.bits_per_value == 0) {
        return Values(params.n_vals, params.reference_value);
    }
    
    double s = mc::codes_power<double>(params.binary_scale_factor, 2);
    double d = mc::codes_power<double>(-params.decimal_scale_factor, 10); 

    Values values(params.n_vals);
    decode_array<ValueType>(buf.data(), params.bits_per_value, params.reference_value, s, d, params.n_vals, values.data());

    return values;
}

// ---------------------------------------------------------------------------------------------------------------------
// Explicit instantiations of the template class
template class SimplePacking<double>;

} // namespace gribjump::mc
