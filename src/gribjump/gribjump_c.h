/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum gribjump_error_values_t {
    GRIBJUMP_SUCCESS = 0, /* Operation succeded. */
    GRIBJUMP_ERROR   = 1, /* Operation failed. */
} gribjump_error_t;

typedef enum gribjump_iterator_status_t {
    GRIBJUMP_ITERATOR_SUCCESS  = 0, /* Operation succeded. */
    GRIBJUMP_ITERATOR_COMPLETE = 1, /* All elements have been returned */
    GRIBJUMP_ITERATOR_ERROR    = 2  /* Operation failed. */
} gribjump_iterator_status_t;

struct gribjump_handle_t;
typedef struct gribjump_handle_t gribjump_handle_t;

struct gribjump_extraction_result_t;
typedef struct gribjump_extraction_result_t gribjump_extraction_result_t;

struct gribjump_extraction_request_t;
typedef struct gribjump_extraction_request_t gribjump_extraction_request_t;

struct gribjump_extractioniterator_t;
typedef struct gribjump_extractioniterator_t gribjump_extractioniterator_t;

struct gribjump_axes_t;
typedef struct gribjump_axes_t gribjump_axes_t;

gribjump_error_t gribjump_new_handle(gribjump_handle_t** gj);
gribjump_error_t gribjump_delete_handle(gribjump_handle_t* gj);

gribjump_error_t gribjump_extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests,
                                  unsigned long nrequests, const char* ctx, gribjump_extractioniterator_t** iterator);

gribjump_error_t gribjump_extract_single(gribjump_handle_t* handle, const char* request, const size_t* range_arr,
                                         size_t range_arr_size, const char* gridhash, const char* ctx,
                                         gribjump_extractioniterator_t** iterator);

gribjump_error_t gribjump_new_request(gribjump_extraction_request_t** request, const char* reqstr, const size_t* ranges,
                                      size_t n_ranges, const char* gridhash);

gribjump_error_t gribjump_delete_request(gribjump_extraction_request_t* request);

gribjump_error_t gribjump_new_result(gribjump_extraction_result_t** result);

gribjump_error_t gribjump_result_values(gribjump_extraction_result_t* result, double** values, size_t nvalues);

// Note: mask is encoded as 64-bit unsigned integers.
// So if N values were extracted in a range, the mask array will contain N/64 elements.
gribjump_error_t gribjump_result_mask(gribjump_extraction_result_t* result, unsigned long long** masks, size_t nmasks);

gribjump_error_t gribjump_delete_result(gribjump_extraction_result_t* result);

gribjump_error_t gribjump_new_axes(gribjump_handle_t* gj, const char* reqstr, int level, const char* ctx,
                                   gribjump_axes_t** axes);

gribjump_error_t gribjump_axes_keys(gribjump_axes_t* axes, const char** keys, size_t size);
gribjump_error_t gribjump_axes_keys_size(gribjump_axes_t* axes, size_t* size);
gribjump_error_t gribjump_axes_values_size(gribjump_axes_t* axes, const char* key, size_t* size);
gribjump_error_t gribjump_axes_values(gribjump_axes_t* axes, const char* key, const char** values, size_t size);
gribjump_error_t gribjump_delete_axes(gribjump_axes_t* axes);

gribjump_error_t gribjump_initialise();
const char* gribjump_version();
const char* gribjump_git_sha1();

gribjump_error_t gribjump_extractioniterator_delete(const gribjump_extractioniterator_t* it);
gribjump_iterator_status_t gribjump_extractioniterator_next(gribjump_extractioniterator_t* it,
                                                            gribjump_extraction_result_t** result);

const char* gribjump_error_string();

#ifdef __cplusplus
}  // extern "C"
#endif
