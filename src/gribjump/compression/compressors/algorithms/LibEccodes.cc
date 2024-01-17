#include <iostream>
#include <limits>
#include <cassert>
#include <cmath>
#include <vector>
#include <random>
#include <iomanip>
//#include <pthread.h>
//
#include "LibEccodes.h"

//#define GRIB_SUCCESS 0
//#define GRIB_NO_VALUES 1
//#define GRIB_INVALID_BPV 2
//#define GRIB_INTERNAL_ERROR 3
//#define GRIB_LOG_DEBUG 4
//#define GRIB_LOG_ERROR 5
//#define GRIB_ENCODING_ERROR 6
//#define GRIB_OUT_OF_RANGE 7
//#define GRIB_UNDERFLOW 8
//#define GRIB_ARRAY_TOO_SMALL 9

#define DebugAssert(a) assert(a)
#define mask1(i) (1UL << i)
#define test(n, i) !!((n)&mask1(i))

constexpr double dbl_min = std::numeric_limits<double>::min();
constexpr double dbl_max = std::numeric_limits<double>::max();
int grib_get_nearest_smaller_value(double val, double* nearest);
unsigned long grib_ieee_nearest_smaller_to_long(double x);
unsigned long grib_ieee_to_long(double x);
double grib_long_to_ieee(unsigned long x);

static const unsigned long nbits[32] = {
    0x1, 0x2, 0x4, 0x8, 0x10, 0x20,
    0x40, 0x80, 0x100, 0x200, 0x400, 0x800,
    0x1000, 0x2000, 0x4000, 0x8000, 0x10000, 0x20000,
    0x40000, 0x80000, 0x100000, 0x200000, 0x400000, 0x800000,
    0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000, 0x20000000,
    0x40000000, 0x80000000
};

static const unsigned long dmasks[] = {
    0xFF,
    0xFE,
    0xFC,
    0xF8,
    0xF0,
    0xE0,
    0xC0,
    0x80,
    0x00,
};

static ieee_table_t ieee_table = { 0, {0, }, {0, }, 0, 0 };

void grib_set_bit_off(unsigned char* p, long* bitp)
{
    p += *bitp / 8;
    *p &= ~(1u << (7 - ((*bitp) % 8)));
    (*bitp)++;
}

void grib_set_bit_on(unsigned char* p, long* bitp)
{
    unsigned char o = 1;
    p += (*bitp >> 3);
    o <<= 7 - ((*bitp) % 8);
    *p |= o;
    (*bitp) += 1;
}

int grib_encode_unsigned_longb(unsigned char* p, unsigned long val, long* bitp, long nb)
{
    long i = 0;

    if (nb > max_nbits) {
        fprintf(stderr, "Number of bits (%ld) exceeds maximum number of bits (%d)\n", nb, max_nbits);
        assert(0);
        return GRIB_INTERNAL_ERROR;
    }
#ifdef DEBUG
    {
        unsigned long maxV = grib_power(nb, 2);
        if (val > maxV) {
            fprintf(stderr, "grib_encode_unsigned_longb: Value=%lu, but number of bits=%ld!\n", val, nb);
            Assert(0);
        }
    }
#endif
    for (i = nb - 1; i >= 0; i--) {
        if (test(val, i))
            grib_set_bit_on(p, bitp);
        else
            grib_set_bit_off(p, bitp);
    }
    return GRIB_SUCCESS;
}

int grib_encode_double_array(size_t n_vals, const double* val, long bits_per_value, double reference_value, double d, double divisor, unsigned char* p, long* off)
{
    size_t i                   = 0;
    unsigned long unsigned_val = 0;
    unsigned char* encoded     = p;
    double x;
    if (bits_per_value % 8) {
        for (i = 0; i < n_vals; i++) {
            x            = (((val[i] * d) - reference_value) * divisor) + 0.5;
            unsigned_val = (unsigned long)x;
            grib_encode_unsigned_longb(encoded, unsigned_val, off, bits_per_value);
        }
    }
    else {
        for (i = 0; i < n_vals; i++) {
            int blen     = 0;
            blen         = bits_per_value;
            x            = ((((val[i] * d) - reference_value) * divisor) + 0.5);
            unsigned_val = (unsigned long)x;
            while (blen >= 8) {
                blen -= 8;
                *encoded = (unsigned_val >> blen);
                encoded++;
                *off += 8;
            }
        }
    }
    return GRIB_SUCCESS;
}




long grib_get_binary_scale_fact(double max, double min, long bpval, int* ret)
{
    double range         = max - min;
    double zs            = 1;
    long scale           = 0;
    const long last      = 127; /* Depends on edition, should be parameter */
    unsigned long maxint = 0;
    const size_t ulong_size = sizeof(maxint) * 8;

    /* See ECC-246
      unsigned long maxint = grib_power<double>(bpval,2) - 1;
      double dmaxint=(double)maxint;
    */
    if (bpval >= ulong_size) {
        *ret = GRIB_OUT_OF_RANGE; /*overflow*/
        return 0;
    }
    const double dmaxint = grib_power<double>(bpval, 2) - 1;
    maxint = (unsigned long)dmaxint; /* Now it's safe to cast */

    *ret = 0;
    if (bpval < 1) {
        *ret = GRIB_ENCODING_ERROR; /* constant field */
        return 0;
    }

    assert(bpval >= 1);
    /*   printf("---- Maxint %ld range=%g\n",maxint,range);    */
    if (range == 0)
        return 0;

    /* range -= 1e-10; */
    while ((range * zs) <= dmaxint) {
        scale--;
        zs *= 2;
    }

    while ((range * zs) > dmaxint) {
        scale++;
        zs /= 2;
    }

    while ((unsigned long)(range * zs + 0.5) <= maxint) {
        scale--;
        zs *= 2;
    }

    while ((unsigned long)(range * zs + 0.5) > maxint) {
        scale++;
        zs /= 2;
    }

    if (scale < -last) {
        *ret = GRIB_UNDERFLOW;
        /*printf("grib_get_binary_scale_fact: max=%g min=%g\n",max,min);*/
        scale = -last;
    }
    assert(scale <= last);
    return scale;
}


void factec(int* krep, const double pa, const int knbit, const long kdec, const int range, long* ke, int* knutil)
{
    *krep = 0;

    *ke     = 0;
    *knutil = 0;

    if (pa < dbl_min) {
        *knutil = 1;
        goto end;
    }

    if ((fabs(log10(fabs(pa)) + (double)kdec) >= range)) {
        *krep = 1;
        goto end;
    }

    /* Binary scale factor associated to kdec */
    *ke = floor(log2((pa * grib_power<double>(kdec, 10)) / (grib_power<double>(knbit, 2) - 0.5))) + 1;
    /* Encoded value for pa = max - min       */
    *knutil = floor(0.5 + pa * grib_power<double>(kdec, 10) * grib_power<double>(-*ke, 2));

end:
    return;
}

double epsilon()
{
    double e = 1.;
    while (1. != (1. + e)) {
        e /= 2;
    }
    return e;
}

int vrange()
{
    return (int)(log(dbl_max) / log(10)) - 1;
}


int grib_optimize_decimal_factor(
                                 const double pmax, const double pmin, const int knbit,
                                 const int compat_gribex, const int compat_32bit,
                                 long* kdec, long* kbin, double* ref)
{
    constexpr double flt_min = std::numeric_limits<float>::min();
    constexpr double flt_max = std::numeric_limits<float>::max();

    //grib_handle* gh = grib_handle_of_accessor(a);
    int idecmin     = -15;
    int idecmax     = 5;
    long inbint;
    double xtinyr4, xhuger4, xnbint;
    int inumax, inutil;
    long jdec, ie;
    int irep;
    int RANGE      = vrange();
    double EPSILON = epsilon();
    double pa      = pmax - pmin;

    if (pa == 0) {
        *kdec = 0;
        *kbin = 0;
        *ref  = 0.;
        return GRIB_SUCCESS;
    }

    inumax = 0;

    if (fabs(pa) <= EPSILON) {
        *kdec   = 0;
        idecmin = 1;
        idecmax = 0;
    }
    else if (pmin != 0. && fabs(pmin) < EPSILON) {
        *kdec   = 0;
        idecmin = 1;
        idecmax = 0;
    }

    xtinyr4 = flt_min;
    xhuger4 = flt_max;

    inbint = grib_power<double>(knbit, 2) - 1;
    xnbint = (double)inbint;

    /* Test decimal scale factors; keep the most suitable */
    for (jdec = idecmin; jdec <= idecmax; jdec++) {
        /* Fix a problem in GRIBEX */
        if (compat_gribex)
            if (pa * grib_power<double>(jdec, 10) <= 1.E-12)
                continue;

        /* Check it will be possible to decode reference value with 32bit floats */
        if (compat_32bit)
            if (fabs(pmin) > dbl_min)
                if (log10(fabs(pmin)) + (double)jdec <= log10(xtinyr4))
                    continue;

        /* Check if encoding will not cause an overflow */
        if (fabs(log10(fabs(pa)) + (double)jdec) >= (double)RANGE)
            continue;

        factec(&irep, pa, knbit, jdec, RANGE, &ie, &inutil);

        if (irep != 0)
            continue;

        /* Check it will be possible to decode the maximum value of the fields using 32bit floats */
        if (compat_32bit)
            if (pmin * grib_power<double>(jdec, 10) + xnbint * grib_power<double>(ie, 2) >= xhuger4)
                continue;

        /* GRIB1 demands that the binary scale factor be encoded in a single byte */
        if (compat_gribex)
            if ((ie < -126) || (ie > 127))
                continue;

        if (inutil > inumax) {
            inumax = inutil;
            *kdec  = jdec;
            *kbin  = ie;
        }
    }

    if (inumax > 0) {
        double decimal = grib_power<double>(+*kdec, 10);
        double divisor = grib_power<double>(-*kbin, 2);
        double min     = pmin * decimal;
        long vmin, vmax;
        if (grib_get_nearest_smaller_value(min, ref) != GRIB_SUCCESS) {
            //grib_context_log(gh->context, GRIB_LOG_ERROR,
            //                 "unable to find nearest_smaller_value of %g for %s", min, reference_value);
            return GRIB_INTERNAL_ERROR;
        }

        vmax = (((pmax * decimal) - *ref) * divisor) + 0.5;
        vmin = (((pmin * decimal) - *ref) * divisor) + 0.5;

        /* This may happen if pmin*decimal-*ref is too large */
        if ((vmin != 0) || (vmax > inbint))
            inumax = 0;
    }

    /* If seeking for an optimal decimal scale factor fails, fall back to a basic method */
    if (inumax == 0) {
        int last   = compat_gribex ? 99 : 127;
        double min = pmin, max = pmax;
        double range    = max - min;
        double f        = grib_power<double>(knbit, 2) - 1;
        double minrange = grib_power<double>(-last, 2) * f;
        double maxrange = grib_power<double>(+last, 2) * f;
        double decimal  = 1;
        int err;

        *kdec = 0;

        while (range < minrange) {
            *kdec += 1;
            decimal *= 10;
            min   = pmin * decimal;
            max   = pmax * decimal;
            range = max - min;
        }

        while (range > maxrange) {
            *kdec -= 1;
            decimal /= 10;
            min   = pmin * decimal;
            max   = pmax * decimal;
            range = max - min;
        }

        if (grib_get_nearest_smaller_value(min, ref) != GRIB_SUCCESS) {
            //grib_context_log(gh->context, GRIB_LOG_ERROR,
                             //"unable to find nearest_smaller_value of %g for %s", min, reference_value);
            return GRIB_INTERNAL_ERROR;
        }

        *kbin = grib_get_binary_scale_fact(max, *ref, knbit, &err);

        if (err == GRIB_UNDERFLOW) {
            *kbin = 0;
            *kdec = 0;
            *ref  = 0;
        }
    }

    return GRIB_SUCCESS;
}


int number_of_bits(unsigned long x, long* result)
{
    const int count        = sizeof(nbits) / sizeof(nbits[0]);
    const unsigned long* n = nbits;
    *result                = 0;
    while (x >= *n) {
        n++;
        (*result)++;
        if (*result >= count) {
            return GRIB_ENCODING_ERROR;
        }
    }
    return GRIB_SUCCESS;
}


void binary_search(const double xx[], const unsigned long n, double x, unsigned long* j)
{
    /*These routine works only on ascending ordered arrays*/
    unsigned long ju, jm, jl;
    jl = 0;
    ju = n;
    while (ju - jl > 1) {
        jm = (ju + jl) >> 1;
        /* printf("jl=%lu jm=%lu ju=%lu\n",jl,jm,ju); */
        /* printf("xx[jl]=%.10e xx[jm]=%.10e xx[ju]=%.10e\n",xx[jl],xx[jm],xx[ju]); */
        if (x >= xx[jm])
            jl = jm;
        else
            ju = jm;
    }
    *j = jl;
}




void init_ieee_table()
{
    if (!ieee_table.inited) {
        unsigned long i;
        unsigned long mmin = 0x800000;
        unsigned long mmax = 0xffffff;
        double e           = 1;
        for (i = 1; i <= 104; i++) {
            e *= 2;
            ieee_table.e[i + 150] = e;
            ieee_table.v[i + 150] = e * mmin;
        }
        ieee_table.e[150] = 1;
        ieee_table.v[150] = mmin;
        e                 = 1;
        for (i = 1; i < 150; i++) {
            e /= 2;
            ieee_table.e[150 - i] = e;
            ieee_table.v[150 - i] = e * mmin;
        }
        ieee_table.vmin   = ieee_table.v[1];
        ieee_table.vmax   = ieee_table.e[254] * mmax;
        ieee_table.inited = 1;
        /*for (i=0;i<128;i++) printf("++++ ieee_table.v[%d]=%g\n",i,ieee_table.v[i]);*/
    }
}




/**
 * decode a value consisting of nbits from an octet-bitstream to long-representation
 *
 * @param p input bitstream, for technical reasons put into octets
 * @param bitp current start position in the bitstream
 * @param nbits number of bits needed to build a number (e.g. 8=byte, 16=short, 32=int, but also other sizes allowed)
 * @return value encoded as 32/64bit numbers
 */
unsigned long grib_decode_unsigned_long(const unsigned char* p, long* bitp, long nbits)
{
    unsigned long ret    = 0;
    long oc              = *bitp / 8;
    unsigned long mask   = 0;
    long pi              = 0;
    int usefulBitsInByte = 0;
    long bitsToRead      = 0;

    if (nbits == 0)
        return 0;

    if (nbits > max_nbits) {
        int bits = nbits;
        int mod  = bits % max_nbits;

        if (mod != 0) {
            int e = grib_decode_unsigned_long(p, bitp, mod);
            assert(e == 0);
            bits -= mod;
        }

        while (bits > max_nbits) {
            int e = grib_decode_unsigned_long(p, bitp, max_nbits);
            assert(e == 0);
            bits -= max_nbits;
        }

        return grib_decode_unsigned_long(p, bitp, bits);
    }
    mask = BIT_MASK(nbits);
    /* pi: position of bitp in p[]. >>3 == /8 */
    pi = oc;
    /* number of useful bits in current byte */
    usefulBitsInByte = 8 - (*bitp & 7);
    /* read at least enough bits (byte by byte) from input */
    bitsToRead = nbits;
    while (bitsToRead > 0) {
        ret <<= 8;
        /*   ret += p[pi];     */
        DebugAssert((ret & p[pi]) == 0);
        ret = ret | p[pi];
        pi++;
        bitsToRead -= usefulBitsInByte;
        usefulBitsInByte = 8;
    }
    *bitp += nbits;
    /* printf("%d %d %d\n", pi, ret, offset); */
    /* bitsToRead might now be negative (too many bits read) */
    /* remove those which are too much */
    ret >>= -1 * bitsToRead;
    /* remove leading bits (from previous value) */
    ret &= mask;
    /* printf("%d %d\n", ret2, ret);*/
    return ret;
}

int grib_encode_unsigned_long(unsigned char* p, unsigned long val, long* bitp, long nbits)
{
    long len          = nbits;
    int s             = *bitp % 8;
    int n             = 8 - s;
    unsigned char tmp = 0; /*for temporary results*/

    if (nbits > max_nbits) {
        /* TODO: Do some real code here, to support long longs */
        int bits  = nbits;
        int mod   = bits % max_nbits;
        long zero = 0;

        if (mod != 0) {
            int e = grib_encode_unsigned_long(p, zero, bitp, mod);
            /* printf(" -> : encoding %ld bits=%ld %ld\n",zero,(long)mod,*bitp); */
            assert(e == 0);
            bits -= mod;
        }

        while (bits > max_nbits) {
            int e = grib_encode_unsigned_long(p, zero, bitp, max_nbits);
            /* printf(" -> : encoding %ld bits=%ld %ld\n",zero,(long)max_nbits,*bitp); */
            assert(e == 0);
            bits -= max_nbits;
        }

        /* printf(" -> : encoding %ld bits=%ld %ld\n",val,(long)bits,*bitp); */
        return grib_encode_unsigned_long(p, val, bitp, bits);
    }

    p += (*bitp >> 3); /* skip the bytes */

    /* head */
    if (s) {
        len -= n;
        if (len < 0) {
            tmp = ((val << -len) | ((*p) & dmasks[n]));
        }
        else {
            tmp = ((val >> len) | ((*p) & dmasks[n]));
        }
        *p++ = tmp;
    }

    /*  write the middle words */
    while (len >= 8) {
        len -= 8;
        *p++ = (val >> len);
    }

    /*  write the end bits */
    if (len)
        *p = (val << (8 - len));

    *bitp += nbits;
    return GRIB_SUCCESS;
}

void init_table_if_needed()
{
    //GRIB_MUTEX_INIT_ONCE(&once, &init)
    //GRIB_MUTEX_LOCK(&mutex)

    if (!ieee_table.inited)
        init_ieee_table();

    //GRIB_MUTEX_UNLOCK(&mutex)
}

int grib_nearest_smaller_ieee_float(double a, double* ret)
{
    unsigned long l = 0;

    init_table_if_needed();

    if (a > ieee_table.vmax) {
        std::cout << "Number is too large: x=" << a << " > xmax=" << ieee_table.vmax << " (IEEE float)";
        return GRIB_INTERNAL_ERROR;
    }

    l    = grib_ieee_nearest_smaller_to_long(a);
    *ret = grib_long_to_ieee(l);
    return GRIB_SUCCESS;
}

int nearest_smaller_value(double val, double* nearest)
{
    return grib_nearest_smaller_ieee_float(val, nearest);
}

int grib_nearest_smaller_value(double val, double* nearest)
{
    return nearest_smaller_value(val, nearest);
}

int grib_get_nearest_smaller_value(double val, double* nearest)
{
    return grib_nearest_smaller_value(val, nearest);
}


int grib_check_data_values_range(const double min_val, const double max_val)
{
    int result        = GRIB_SUCCESS;

    if (!(min_val < dbl_max && min_val > -dbl_max)) {
        std::cout << "Minimum value out of range: " << min_val << std::endl;
        return GRIB_ENCODING_ERROR;
    }
    if (!(max_val < dbl_max && max_val > -dbl_max)) {
        std::cout << "Maximum value out of range: " <<  max_val << std::endl;
        return GRIB_ENCODING_ERROR;
    }

    //// Data Quality checks
    //if (ctx->grib_data_quality_checks) {
    //    result = grib_util_grib_data_quality_check(h, min_val, max_val);
    //}

    return result;
}

unsigned long grib_ieee_to_long(double x)
{
    unsigned long s    = 0;
    unsigned long mmax = 0xffffff;
    unsigned long mmin = 0x800000;
    unsigned long m    = 0;
    unsigned long e    = 0;
    double rmmax       = mmax + 0.5;

    init_table_if_needed();

    /* printf("\ngrib_ieee_to_long: x=%.20e\n",x); */
    if (x < 0) {
        s = 1;
        x = -x;
    }

    /* Underflow */
    if (x < ieee_table.vmin) {
        /*printf("grib_ieee_to_long: (x < ieee_table.vmin) x=%.20e vmin=%.20e v=0x%lX\n",x,ieee_table.vmin,(s<<31));*/
        return (s << 31);
    }

    /* Overflow */
    if (x > ieee_table.vmax) {
        fprintf(stderr, "grib_ieee_to_long: Number is too large: x=%.20e > xmax=%.20e\n", x, ieee_table.vmax);
        assert(0);
        return 0;
    }

    binary_search(ieee_table.v, 254, x, &e);

    /* printf("grib_ieee_to_long: e=%ld\n",e); */

    x /= ieee_table.e[e];

    /* printf("grib_ieee_to_long: x=%.20e\n",x); */

    while (x < mmin) {
        x *= 2;
        e--;
        /* printf("grib_ieee_to_long (e--): x=%.20e e=%ld \n",x,e); */
    }

    while (x > rmmax) {
        x /= 2;
        e++;
        /* printf("grib_ieee_to_long (e++): x=%.20e e=%ld \n",x,e); */
    }

    m = x + 0.5;
    /* printf("grib_ieee_to_long: m=0x%lX (%lu) x=%.10e \n",m,m,x ); */
    if (m > mmax) {
        e++;
        m = 0x800000;
        /* printf("grib_ieee_to_long: ( m > mmax ) m=0x%lX (%lu) x=%.10e \n",m,m,x ); */
    }

    /* printf("grib_ieee_to_long: s=%lu c=%lu (0x%lX) m=%lu (0x%lX)\n",s,e,e,m,m ); */

    return (s << 31) | (e << 23) | (m & 0x7fffff);
}


double grib_long_to_ieee(unsigned long x)
{
    unsigned long s = x & 0x80000000;
    unsigned long c = (x & 0x7f800000) >> 23;
    unsigned long m = (x & 0x007fffff);

    double val;

#ifdef DEBUG
    if (x > 0 && x < 0x800000) {
        fprintf(stderr, "grib_long_to_ieee: Invalid input %lu\n", x);
        Assert(0);
    }
#endif
    init_table_if_needed();

    if (c == 0 && m == 0)
        return 0;

    if (c == 0) {
        m |= 0x800000;
        c = 1;
    }
    else
        m |= 0x800000;

    val = m * ieee_table.e[c];
    if (s)
        val = -val;

    return val;
}


unsigned long grib_ieee_nearest_smaller_to_long(double x)
{
    unsigned long l;
    unsigned long e;
    unsigned long m;
    unsigned long s;
    unsigned long mmin = 0x800000;
    double y, eps;

    if (x == 0)
        return 0;

    init_table_if_needed();

    l = grib_ieee_to_long(x);
    y = grib_long_to_ieee(l);

    if (x < y) {
        if (x < 0 && -x < ieee_table.vmin) {
            l = 0x80800000;
        }
        else {
            e = (l & 0x7f800000) >> 23;
            m = (l & 0x007fffff) | 0x800000;
            s = l & 0x80000000;

            if (m == mmin) {
                /* printf("grib_ieee_nearest_smaller_to_long: m == mmin (0x%lX) e=%lu\n",m,e);  */
                e = s ? e : e - 1;
                if (e < 1)
                    e = 1;
                if (e > 254)
                    e = 254;
                /* printf("grib_ieee_nearest_smaller_to_long: e=%lu \n",e);  */
            }

            eps = ieee_table.e[e];

            /* printf("grib_ieee_nearest_smaller_to_long: x<y\n"); */
            l = grib_ieee_to_long(y - eps);
            /* printf("grib_ieee_nearest_smaller_to_long: grib_ieee_to_long(y-eps)=0x%lX y=%.10e eps=%.10e x=%.10e\n",l,y,eps,x); */
        }
    }
    else
        return l;

    if (x < grib_long_to_ieee(l)) {
        printf("grib_ieee_nearest_smaller_to_long: x=%.20e grib_long_to_ieee(0x%lX)=%.20e\n", x, l, grib_long_to_ieee(l));
        assert(x >= grib_long_to_ieee(l));
    }

    return l;
}


//// Return true(1) if large constant fields are to be created, otherwise false(0)
//int grib_producing_large_constant_fields(grib_handle* h, int edition)
//{
//    // First check if the transient key is set
//    grib_context* c                 = h->context;
//    long produceLargeConstantFields = 0;
//    if (grib_get_long(h, "produceLargeConstantFields", &produceLargeConstantFields) == GRIB_SUCCESS &&
//        produceLargeConstantFields != 0) {
//        return 1;
//    }

//    if (c->gribex_mode_on == 1 && edition == 1) {
//        return 1;
//    }

//    // Finally check the environment variable via the context
//    return c->large_constant_fields;
//}


