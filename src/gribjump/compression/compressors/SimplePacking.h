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

#include <cstddef>
#include <vector>

#include "eckit/exception/Exceptions.h"

/// @todo A lot of this file looks like it is copy-pasted from eccodes.

constexpr size_t maxNBits = sizeof(unsigned long) * 8;
#define LIB_ECCODES_BIT_MASK(x) (((x) == maxNBits) ? (unsigned long)-1UL : (1UL << (x)) - 1)

/// @todo: we're using c++, act like it.
template <typename T>
int grib_decode_array(const unsigned char* p, long bitsPerValue,
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
            /*  *bitp += bitsPerValue * n_vals; */
        }
    }
    else {
        unsigned long mask = LIB_ECCODES_BIT_MASK(bitsPerValue);

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


/* Return n to the power of s */
/// @todo: this is duplicated all over the place!
template <typename T>
T grib_power(long s, long n)
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


template <typename T>
struct DecodeParameters {
    using ValueType = T;

    ValueType reference_value;
    ValueType binary_scale_factor;
    ValueType decimal_scale_factor;
    long bits_per_value;
    size_t n_vals;
    bool is_constant_field;

    DecodeParameters() : reference_value(0), binary_scale_factor(0), decimal_scale_factor(0), bits_per_value(0) {}
    void print() {
        std::cout << "reference_value: " << reference_value << std::endl;
        std::cout << "binary_scale_factor: " << binary_scale_factor << std::endl;
        std::cout << "decimal_scale_factor: " << decimal_scale_factor << std::endl;
        std::cout << "bits_per_value: " << bits_per_value << std::endl;
        std::cout << "n_vals: " << n_vals << std::endl;
        std::cout << "is_constant_field: " << is_constant_field << std::endl;
    }
};

template <typename T>
class SimplePacking {
public:
    using ValueType = T;
    using Buffer = std::vector<unsigned char>;
    using Values = std::vector<ValueType>;

    SimplePacking() = default;
    ~SimplePacking() = default;

    Values unpack(
        const DecodeParameters<ValueType>& params,
        const Buffer& buffer
    );
};


template <typename ValueType>
typename SimplePacking<ValueType>::Values SimplePacking<ValueType>::unpack(
    const DecodeParameters<ValueType>& params,
    const Buffer& buf) {

    if (params.bits_per_value > (sizeof(long) * 8)) {
        throw eckit::BadValue("Invalid BPV", Here());
    }

    if (params.n_vals == 0) {
        return Values{};
    }

    // Special case: constant field
    if (params.bits_per_value == 0) {
        return Values(params.n_vals, params.reference_value);
    }
    
    /// @todo: these are precalculated in the jump info!
    double s = grib_power<double>(params.binary_scale_factor, 2);
    double d = grib_power<double>(-params.decimal_scale_factor, 10); 

    Values values(params.n_vals);
    grib_decode_array<ValueType>(buf.data(), params.bits_per_value, params.reference_value, s, d, params.n_vals, values.data());

    return values;
}

