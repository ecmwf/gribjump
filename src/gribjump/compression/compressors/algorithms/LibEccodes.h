#pragma once

#include <iostream>
#include <limits>
#include <cassert>
#include <cmath>
#include <vector>
#include <random>
#include <iomanip>

#define LIB_ECCODES_SUCCESS 0
#define LIB_ECCODES_NO_VALUES 1
#define LIB_ECCODES_INVALID_BPV 2
#define LIB_ECCODES_INTERNAL_ERROR 3
#define LIB_ECCODES_LOG_DEBUG 4
#define LIB_ECCODES_LOG_ERROR 5
#define LIB_ECCODES_ENCODING_ERROR 6
#define LIB_ECCODES_OUT_OF_RANGE 7
#define LIB_ECCODES_UNDERFLOW 8
#define LIB_ECCODES_ARRAY_TOO_SMALL 9

constexpr size_t maxNBits = sizeof(unsigned long) * 8;
#define LIB_ECCODES_BIT_MASK(x) (((x) == maxNBits) ? (unsigned long)-1UL : (1UL << (x)) - 1)

typedef std::numeric_limits<double> dbl;
typedef std::numeric_limits<float> flt;
typedef struct ieee_table_t ieee_table_t;
//static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

struct ieee_table_t
{
    int inited;
    double e[255];
    double v[255];
    double vmin;
    double vmax;
};


int grib_get_nearest_smaller_value(double val, double* nearest);
void grib_set_bit_off(unsigned char* p, long* bitp);
void grib_set_bit_on(unsigned char* p, long* bitp);
int grib_encode_unsigned_longb(unsigned char* p, unsigned long val, long* bitp, long nb);
int grib_encode_double_array(size_t n_vals, const double* val, long bits_per_value, double reference_value, double d, double divisor, unsigned char* p, long* off);
long grib_get_binary_scale_fact(double max, double min, long bpval, int* ret);
void factec(int* krep, const double pa, const int knbit, const long kdec, const int range, long* ke, int* knutil);
double epsilon();
int vrange();
int grib_optimize_decimal_factor(
                                 const double pmax, const double pmin, const int knbit,
                                 const int compat_gribex, const int compat_32bit,
                                 long* kdec, long* kbin, double* ref);

int number_of_bits(unsigned long x, long* result);
void binary_search(const double xx[], const unsigned long n, double x, unsigned long* j);



void init_ieee_table();
unsigned long grib_decode_unsigned_long(const unsigned char* p, long* bitp, long nbits);
int grib_encode_unsigned_long(unsigned char* p, unsigned long val, long* bitp, long nbits);
void init_table_if_needed();
int grib_nearest_smaller_ieee_float(double a, double* ret);
int nearest_smaller_value(double val, double* nearest);
int grib_nearest_smaller_value(double val, double* nearest);
int grib_check_data_values_range(const double min_val, const double max_val);
unsigned long grib_ieee_to_long(double x);
double grib_long_to_ieee(unsigned long x);
unsigned long grib_ieee_nearest_smaller_to_long(double x);


template <typename T>
int grib_decode_array(const unsigned char* p, long* bitp, long bitsPerValue,
                             double reference_value, double s, double d,
                             size_t n_vals, T* val)
{
    size_t i               = 0;
    unsigned long lvalue = 0;
    T x;

    if (bitsPerValue % 8 == 0) {
        /* See ECC-386 */
        int bc;
        int l    = bitsPerValue / 8;
        size_t o = 0;

        for (i = 0; i < n_vals; i++) {
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
        long pi = *bitp / 8;
        /* some bits might of the current byte at pi might be used */
        /* by the previous number usefulBitsInByte gives remaining unused bits */
        /* number of useful bits in current byte */
        int usefulBitsInByte = 8 - (*bitp & 7);
        for (i = 0; i < n_vals; i++) {
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
            *bitp += bitsPerValue;
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

