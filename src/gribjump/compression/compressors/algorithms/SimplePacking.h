#pragma once

#include <cstddef>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include "LibEccodes.h"


enum class FloatType {
    IEEE32,
    IBM32,
    UNKNOWN
};



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

    std::pair<DecodeParameters<ValueType>, Buffer> pack(
        struct DecodeParameters<ValueType> params,
        const Values& values,
        double units_factor = 1.0,
        double units_bias = 0.0);

    Values unpack(
        const DecodeParameters<ValueType>& params,
        const Buffer& buffer,
        double units_factor = 1.0,
        double units_bias = 0.0,
        long offsetBeforeData = 0,
        long offsetAfterData = 0);

    // Setter
    void decimal_scale_factor(size_t decimal_scale_factor_get) { decimal_scale_factor_get_ = decimal_scale_factor_get; }
    void changing_precision(size_t changing_precision) { changing_precision_ = changing_precision; }
    void optimize_scaling_factor(bool optimize_scaling_factor) { optimize_scaling_factor_ = optimize_scaling_factor; }
    void large_constant_fields(long large_constant_fields) { large_constant_fields_ = large_constant_fields; }
    void dirty(bool dirty) { dirty_ = dirty; }
    void compat_gribex_set(bool compat_gribex) { compat_gribex_ = compat_gribex; }

    // Getter
    size_t decimal_scale_factor() const { return decimal_scale_factor_get_; }
    size_t changing_precision() const { return changing_precision_; }
    bool optimize_scaling_factor() const { return optimize_scaling_factor_; }
    long large_constant_fields() const { return large_constant_fields_; }
    bool dirty() const { return dirty_; }
    bool compat_gribex() const { return compat_gribex_; }

private:
    bool optimize_scaling_factor_ = false;
    size_t decimal_scale_factor_get_ = 0;
    size_t changing_precision_ = 0;
    long large_constant_fields_ = 0;
    bool dirty_ = false;
    bool compat_gribex_ = false;
    FloatType float_type_ = FloatType::IEEE32;
};



template <typename ValueType>
std::pair<DecodeParameters<ValueType>, typename SimplePacking<ValueType>::Buffer> SimplePacking<ValueType>::pack(
    struct DecodeParameters<ValueType> params,
    const Values& values,
    double units_factor,
    double units_bias
)
{

    size_t i = 0;
    int err       = 0;
    //long decimal_scale_factor_get = 0;
    //long optimize_scaling_factor  = 0;
    ValueType decimal                = 1;
    ValueType unscaled_max           = 0;
    ValueType unscaled_min           = 0;
    ValueType f                      = 0;
    ValueType range                  = 0;
    ValueType minrange = 0, maxrange = 0;
    //long changing_precision = 0;


    size_t n_vals = values.size();

    if (n_vals == 0)
        throw std::runtime_error("No grib values");


    /* check we don't encode bpv > max(ulong)-1 as it is not currently supported by the algorithm */
    if (params.bits_per_value > (sizeof(long) * 8 - 1))
        throw std::runtime_error("Invalid bits_per_value" + std::to_string(params.bits_per_value));

    dirty_ = 1;

    const ValueType* val = values.data();
    auto [min_ptr, max_ptr] = std::minmax_element(values.begin(), values.end());
    auto min = *min_ptr;
    auto max = *max_ptr;

    if ((err = grib_check_data_values_range(min, max)) != GRIB_SUCCESS)
        throw std::runtime_error("grib_check_data_values_range");

    if (max == min) {
        if (grib_get_nearest_smaller_value(val[0], &params.reference_value) != GRIB_SUCCESS) {
            throw std::runtime_error("unable to find nearest_smaller_value");
        }

        if (large_constant_fields_) {
            params.binary_scale_factor = 0;
            params.decimal_scale_factor = 0;
            if (params.bits_per_value == 0) {
                params.bits_per_value = 16;
            }

            params.is_constant_field = true;
            params.reference_value = min;
            return std::make_pair(params, Buffer());
        }
        else {
            params.bits_per_value = 0;
            params.is_constant_field = true;
            params.reference_value = min;

            return std::make_pair(params, Buffer());
        }
    }

    /* the packing parameters are not properly defined, this is a safe way of fixing the problem */
    if (changing_precision_ == 0 && params.bits_per_value == 0 && decimal_scale_factor_get_ == 0) {
        params.bits_per_value = 24;
    }

    long binary_scale_factor = 0;
    long decimal_scale_factor = 0;
    ValueType reference_value = 0;

    if (params.bits_per_value == 0 || (binary_scale_factor == 0 && decimal_scale_factor_get_ != 0)) {
        /* decimal_scale_factor is given, binary_scale_factor=0 and params.bits_per_value is computed */
        binary_scale_factor  = 0;
        decimal_scale_factor = decimal_scale_factor_get_;
        decimal              = grib_power<double>(decimal_scale_factor, 10);
        min *= decimal;
        max *= decimal;

        /* params.bits_per_value=(long)ceil(log((double)(imax-imin+1))/log(2.0)); */
        /* See GRIB-540 for why we use ceil */
        if (number_of_bits((unsigned long)ceil(fabs(max - min)), &params.bits_per_value) != GRIB_SUCCESS)
            throw std::runtime_error("Range of values too large. Try a smaller value for decimal precision (less than %ld)");

        if (grib_get_nearest_smaller_value(min, &reference_value) != GRIB_SUCCESS)
            throw std::runtime_error("unable to find nearest_smaller_value");
        /* divisor=1; */
    }
    else {
        int last = 127;  /* 'last' should be a parameter coming from a definitions file */
        //if (c->gribex_mode_on && self->edition == 1)
        if (compat_gribex_)
            last = 99;
        /* params.bits_per_value is given and decimal_scale_factor and binary_scale_factor are calcualated */
        if (max == min) {
            binary_scale_factor = 0;
            /* divisor=1; */
            if (grib_get_nearest_smaller_value(min, &reference_value) != GRIB_SUCCESS)
                throw std::runtime_error("unable to find nearest_smaller_value of %g for %s");
        }
        else if (optimize_scaling_factor_) {
            if ((err = grib_optimize_decimal_factor(
                                                    max, min, params.bits_per_value,
                                                    compat_gribex_, 1,
                                                    &decimal_scale_factor, &binary_scale_factor, &reference_value)) != GRIB_SUCCESS)
                throw std::runtime_error("grib_optimize_decimal_factor failed");
        }
        else {
            /* printf("max=%g reference_value=%g grib_power<double>(-last,2)=%g decimal_scale_factor=%ld params.bits_per_value=%ld\n",
               max,reference_value,grib_power<double>(-last,2),decimal_scale_factor,params.bits_per_value);*/
            range        = (max - min);
            unscaled_min = min;
            unscaled_max = max;
            f            = (grib_power<double>(params.bits_per_value, 2) - 1);
            minrange     = grib_power<double>(-last, 2) * f;
            maxrange     = grib_power<double>(last, 2) * f;

            while (range < minrange) {
                decimal_scale_factor += 1;
                decimal *= 10;
                min   = unscaled_min * decimal;
                max   = unscaled_max * decimal;
                range = (max - min);
            }
            while (range > maxrange) {
                decimal_scale_factor -= 1;
                decimal /= 10;
                min   = unscaled_min * decimal;
                max   = unscaled_max * decimal;
                range = (max - min);
            }

            if (grib_get_nearest_smaller_value(min, &reference_value) != GRIB_SUCCESS) {
                throw std::runtime_error(std::string{"unable to find nearest_smaller_value of "} + std::to_string(min) + " for " + std::to_string(reference_value));
            }

            binary_scale_factor = grib_get_binary_scale_fact(max, reference_value, params.bits_per_value, &err);
            if (err) {
                throw std::runtime_error("unable to compute binary_scale_factor");
            }
        }
    }

    changing_precision_ = 0;

    //return GRIB_SUCCESS;


//static int pack_double(grib_accessor* a, const double* cval, size_t* len)
    
    //double decimal                            = 1;
    size_t buflen                             = 0;
    //unsigned char* buf                        = NULL;
    unsigned char* encoded                    = NULL;
    double divisor                            = 1;
    long off                                  = 0;
    int ret                                   = 0;
    //double* val                               = (double*)cval;
    //int i;
    
    //if (*len == 0) {
    if (n_vals == 0) {
        //grib_buffer_replace(a, NULL, 0, 1, 1);
        return {params, Buffer(0)};
        //return GRIB_SUCCESS;
    }

    //if (ret == GRIB_SUCCESS)
    //    ret = grib_set_long_internal(grib_handle_of_accessor(a), self->number_of_values, *len);

    //if (ret != GRIB_SUCCESS)
    //    return ret;

    //if (self->units_factor &&
    //    (grib_get_double_internal(grib_handle_of_accessor(a), self->units_factor, &units_factor) == GRIB_SUCCESS)) {
    //    grib_set_double_internal(grib_handle_of_accessor(a), self->units_factor, 1.0);
    //}

    //if (self->units_bias &&
    //    (grib_get_double_internal(grib_handle_of_accessor(a), self->units_bias, &units_bias) == GRIB_SUCCESS)) {
    //    grib_set_double_internal(grib_handle_of_accessor(a), self->units_bias, 0.0);
    //}

    //if (units_factor != 1.0) {
    //    if (units_bias != 0.0)
    //        for (i = 0; i < n_vals; i++)
    //            val[i] = val[i] * units_factor + units_bias;
    //    else
    //        for (i = 0; i < n_vals; i++)
    //            val[i] *= units_factor;
    //}
    //else if (units_bias != 0.0)
    //    for (i = 0; i < n_vals; i++)
    //        val[i] += units_bias;

    /* IEEE packing */
    //if (c->ieee_packing) {
    //    grib_handle* h = grib_handle_of_accessor(a);
    //    long precision = 0; [> Either 1(=32 bits) or 2(=64 bits) <]
    //    size_t lenstr  = 10;
    //    if ((ret = codes_check_grib_ieee_packing_value(c->ieee_packing)) != GRIB_SUCCESS)
    //        return ret;
    //    precision = c->ieee_packing == 32 ? 1 : 2;
    //    if ((ret = grib_set_string(h, "packingType", "grid_ieee", &lenstr)) != GRIB_SUCCESS)
    //        return ret;
    //    if ((ret = grib_set_long(h, "precision", precision)) != GRIB_SUCCESS)
    //        return ret;

    //    return grib_set_double_array(h, "values", val, *len);
    //}

    //if (super != grib_accessor_class_data_g2simple_packing) {
    //    [> Normal case: parent not same as me! <]
    //    ret = super->pack_double(a, val, len);
    //}
    //else {
    //    [> GRIB-364: simple packing with logarithm pre-processing <]
    //    grib_accessor_class* super2 = NULL;
    //    Assert(super->super);
    //    super2 = *(super->super);
    //    ret    = super2->pack_double(a, val, len);
    //}
    //switch (ret) {
    //    case GRIB_CONSTANT_FIELD:
    //        grib_buffer_replace(a, NULL, 0, 1, 1);
    //        return GRIB_SUCCESS;
    //    case GRIB_SUCCESS:
    //        break;
    //    default:
    //        grib_context_log(a->context, GRIB_LOG_ERROR, "GRIB2 simple packing: unable to set values (%s)", grib_get_error_message(ret));
    //        return ret;
    //}


    decimal = grib_power<double>(decimal_scale_factor, 10);
    divisor = grib_power<double>(-binary_scale_factor, 2);

    buflen  = (((params.bits_per_value * n_vals) + 7) / 8) * sizeof(unsigned char);
    unsigned char *buf    = (unsigned char*)malloc(buflen);
    encoded = buf;

    grib_encode_double_array(n_vals, val, params.bits_per_value, reference_value, decimal, divisor, encoded, &off);

    Buffer buffer = Buffer(encoded, encoded + buflen);

    DecodeParameters<ValueType> decode_params;
    decode_params.reference_value = reference_value;
    decode_params.binary_scale_factor = binary_scale_factor;
    decode_params.decimal_scale_factor = decimal_scale_factor;
    decode_params.bits_per_value = params.bits_per_value;
    decode_params.n_vals = n_vals;

    //grib_context_log(a->context, GRIB_LOG_DEBUG,
                     //"grib_accessor_data_g2simple_packing : pack_double : packing %s, %d values", a->name, n_vals);

    //grib_buffer_replace(a, buf, buflen, 1, 1);

    //grib_context_buffer_free(a->context, buf);

    //return decode_params;
//}


  return {decode_params, buffer};
}


template <typename ValueType>
typename SimplePacking<ValueType>::Values SimplePacking<ValueType>::unpack(
    const DecodeParameters<ValueType>& params,
    const Buffer& buf,
    double units_factor,
    double units_bias,
    long offsetBeforeData,
    long offsetAfterData
)
{

    ////unsigned char* buf, size_t buf_len, T* val, size_t val_len,
    ////size_t n_vals,
    ////const DecodeParameters& decode_params)
    //static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");


    //double *val = (unsigned char*)buf.data();
    Values values(params.n_vals);

    double* val = values.data();
    size_t val_len = values.size() * sizeof(ValueType);
    size_t n_vals = params.n_vals;

    //unsigned char* buf                      = (unsigned char*)grib_handle_of_accessor(a)->buffer->data;

    size_t i      = 0;
    int err       = 0;
    long pos      = 0;
    //long count    = 0;

    double s            = 0;
    double d            = 0;
    size_t count = 0;

    //err = grib_value_count(a, &count);
    //if (err)
    //    return err;
    //n_vals = count;
    size_t len = val_len / sizeof(ValueType);

    if (len < n_vals) {
        len = (long)n_vals;
        //return GRIB_ARRAY_TOO_SMALL;
        throw std::runtime_error("Array too small");
    }

    //if ((err = grib_get_long_internal(gh, self->params.bits_per_value, &params.bits_per_value)) != GRIB_SUCCESS)
    //    return err;

    /*
     * check we don't decode bpv > max(ulong) as it is
     * not currently supported by the algorithm
     */
    if (params.bits_per_value > (sizeof(long) * 8)) {
        throw std::runtime_error("Invalid BPV");
    }

    if (n_vals == 0) {
        len = 0;
        return Values{};
    }

    dirty_ = 0;

    /* Special case */

    if (params.bits_per_value == 0) {
        for (i = 0; i < n_vals; i++)
            val[i] = params.reference_value;
        //len = n_vals;
        //return GRIB_SUCCESS;
        return Values(n_vals, params.reference_value);
    }

    s = grib_power<double>(params.binary_scale_factor, 2);
    d = grib_power<double>(-params.decimal_scale_factor, 10);

    //offsetBeforeData = grib_byte_offset(a);
    //buf += offsetBeforeData;

    /*Assert(((params.bits_per_value*n_vals)/8) < (1<<29));*/ /* See GRIB-787 */

    /* ECC-941 */
    if (float_type_ == FloatType::IEEE32) {
        /* Must turn off this check when the environment variable ECCODES_GRIB_IEEE_PACKING is on */
        if (offsetAfterData > offsetBeforeData) {
            const long valuesSize = (params.bits_per_value * n_vals) / 8; /*in bytes*/
            if (offsetBeforeData + valuesSize > offsetAfterData) {
                throw std::runtime_error("Values section size mismatch");
            }
        }
    }

    grib_decode_array<ValueType>(buf.data(), &pos, params.bits_per_value, params.reference_value, s, d, n_vals, val);

    //len = (long)n_vals;

    if (units_factor != 1.0) {
        if (units_bias != 0.0)
            for (i = 0; i < n_vals; i++)
                val[i] = val[i] * units_factor + units_bias;
        else
            for (i = 0; i < n_vals; i++)
                val[i] *= units_factor;
    }
    else if (units_bias != 0.0)
        for (i = 0; i < n_vals; i++)
            val[i] += units_bias;

    return values;
}

