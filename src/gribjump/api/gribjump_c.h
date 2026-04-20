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

/// @file gribjump_c.h
/// @brief C API for GribJump language interoperability.
///
/// This header exposes a C-compatible interface to GribJump for foreign-function
/// bindings (for example Python integration via CFFI).

#pragma once
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/// @brief Error codes returned by GribJump C API calls.
typedef enum gribjump_error_values_t {
    GRIBJUMP_SUCCESS = 0, /* Operation succeded. */
    GRIBJUMP_ERROR   = 1, /* Operation failed. */
} gribjump_error_t;

/// @brief Status codes returned when advancing an extraction iterator.
typedef enum gribjump_iterator_status_t {
    GRIBJUMP_ITERATOR_SUCCESS  = 0, /* Operation succeded. */
    GRIBJUMP_ITERATOR_COMPLETE = 1, /* All elements have been returned */
    GRIBJUMP_ITERATOR_ERROR    = 2  /* Operation failed. */
} gribjump_iterator_status_t;

/// @brief Opaque handle owning a GribJump C++ facade instance.
struct gribjump_handle_t;
typedef struct gribjump_handle_t gribjump_handle_t;

/// @brief Opaque extraction result object.
struct gribjump_extraction_result_t;
typedef struct gribjump_extraction_result_t gribjump_extraction_result_t;

/// @brief Opaque request object for MARS-based extraction.
struct gribjump_extraction_request_t;
typedef struct gribjump_extraction_request_t gribjump_extraction_request_t;

/// @brief Opaque request object for path-and-offset-based extraction.
struct gribjump_path_extraction_request_t;
typedef struct gribjump_path_extraction_request_t gribjump_path_extraction_request_t;

/// @brief Opaque iterator over extraction results.
struct gribjump_extractioniterator_t;
typedef struct gribjump_extractioniterator_t gribjump_extractioniterator_t;

/// @brief Opaque object containing axis-query keys and values.
struct gribjump_axes_t;
typedef struct gribjump_axes_t gribjump_axes_t;

/// @brief Create a new GribJump handle.
/// @param[out] gj Pointer receiving the allocated handle.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_new_handle(gribjump_handle_t** gj);

/// @brief Delete a GribJump handle.
/// @param[in] gj Handle to destroy. May be null.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_delete_handle(gribjump_handle_t* gj);

/// @brief Start extraction from MARS-based extraction request objects.
/// @param[in] handle GribJump handle.
/// @param[in] requests Array of request pointers.
/// @param[in] nrequests Number of elements in @p requests.
/// @param[in] ctx Optional context string for logging/metrics.
/// @param[out] iterator Pointer receiving a newly allocated result iterator.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests,
                                  unsigned long nrequests, const char* ctx, gribjump_extractioniterator_t** iterator);

/// @brief Start extraction from path-based extraction request objects.
/// @param[in] handle GribJump handle.
/// @param[in] requests Array of path request pointers.
/// @param[in] nrequests Number of elements in @p requests.
/// @param[in] ctx Optional context string for logging/metrics.
/// @param[out] iterator Pointer receiving a newly allocated result iterator.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_extract_from_paths(gribjump_handle_t* handle, gribjump_path_extraction_request_t** requests,
                                             unsigned long nrequests, const char* ctx,
                                             gribjump_extractioniterator_t** iterator);

/// @brief Convenience extraction for a single MARS request and range array.
/// @param[in] handle GribJump handle.
/// @param[in] request MARS request string.
/// @param[in] range_arr Flat array of range bounds encoded as [start0,end0,start1,end1,...].
/// @param[in] range_arr_size Number of size_t entries in @p range_arr.
/// @param[in] gridhash Optional grid hash filter/verification string.
/// @param[in] ctx Optional context string for logging/metrics.
/// @param[out] iterator Pointer receiving a newly allocated result iterator.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_extract_single(gribjump_handle_t* handle, const char* request, const size_t* range_arr,
                                         size_t range_arr_size, const char* gridhash, const char* ctx,
                                         gribjump_extractioniterator_t** iterator);

/// @brief Allocate a MARS-based extraction request object.
/// @param[out] request Pointer receiving the allocated request.
/// @param[in] reqstr MARS request string.
/// @param[in] ranges Flat array of bounds encoded as [start0,end0,start1,end1,...].
/// @param[in] n_ranges Number of size_t entries in @p ranges.
/// @param[in] gridhash Optional grid hash string.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_new_request(gribjump_extraction_request_t** request, const char* reqstr, const size_t* ranges,
                                      size_t n_ranges, const char* gridhash);

/// @brief Allocate a path-based extraction request object.
/// @param[out] request Pointer receiving the allocated request.
/// @param[in] filename Path to the GRIB file.
/// @param[in] scheme URI scheme (for example "file").
/// @param[in] offset GRIB message offset in bytes.
/// @param[in] host Optional host for remote access.
/// @param[in] port Optional port for remote access.
/// @param[in] range_arr Flat array of bounds encoded as [start0,end0,start1,end1,...].
/// @param[in] range_arr_size Number of size_t entries in @p range_arr.
/// @param[in] gridhash Optional grid hash string.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_new_request_from_path(gribjump_path_extraction_request_t** request, const char* filename,
                                                const char* scheme, size_t offset, const char* host, int port,
                                                const size_t* range_arr, size_t range_arr_size, const char* gridhash);

/// @brief Delete a MARS-based extraction request object.
/// @param[in] request Request object to destroy.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_delete_request(gribjump_extraction_request_t* request);

/// @brief Delete a path-based extraction request object.
/// @param[in] request Path request object to destroy.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_delete_path_request(gribjump_path_extraction_request_t* request);

/// @brief Allocate an empty extraction result object.
/// @param[out] result Pointer receiving the allocated result.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_new_result(gribjump_extraction_result_t** result);

/// @brief Copy extracted values from a result into caller-provided storage.
/// @param[in] result Extraction result object.
/// @param[out] values Output pointer to contiguous value storage.
/// @param[in] nvalues Number of values expected by the caller.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_result_values(gribjump_extraction_result_t* result, double** values, size_t nvalues);

// Note: mask is encoded as 64-bit unsigned integers.
// So if N values were extracted in a range, the mask array will contain N/64 elements.
/// @brief Copy missing-value masks from a result into caller-provided storage.
/// @param[in] result Extraction result object.
/// @param[out] masks Output pointer to contiguous 64-bit masks.
/// @param[in] nmasks Number of mask elements expected by the caller.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_result_mask(gribjump_extraction_result_t* result, unsigned long long** masks, size_t nmasks);

/// @brief Delete an extraction result object.
/// @param[in] result Result object to destroy.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_delete_result(gribjump_extraction_result_t* result);

/// @brief Query available axes for a request and return an opaque axes object.
/// @param[in] gj GribJump handle.
/// @param[in] reqstr MARS request string.
/// @param[in] level Axis enumeration depth.
/// @param[in] ctx Optional context string for logging/metrics.
/// @param[out] axes Pointer receiving the allocated axes object.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_new_axes(gribjump_handle_t* gj, const char* reqstr, int level, const char* ctx,
                                   gribjump_axes_t** axes);

/// @brief Copy axis key names into caller-provided array.
/// @param[in] axes Axes object.
/// @param[out] keys Caller-allocated array receiving key strings.
/// @param[in] size Number of elements available in @p keys.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_axes_keys(gribjump_axes_t* axes, const char** keys, size_t size);

/// @brief Get number of axis keys available in an axes object.
/// @param[in] axes Axes object.
/// @param[out] size Number of keys.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_axes_keys_size(gribjump_axes_t* axes, size_t* size);

/// @brief Get number of values for a specific axis key.
/// @param[in] axes Axes object.
/// @param[in] key Axis key.
/// @param[out] size Number of values associated with @p key.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_axes_values_size(gribjump_axes_t* axes, const char* key, size_t* size);

/// @brief Copy values for a specific axis key into caller-provided array.
/// @param[in] axes Axes object.
/// @param[in] key Axis key.
/// @param[out] values Caller-allocated array receiving value strings.
/// @param[in] size Number of elements available in @p values.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_axes_values(gribjump_axes_t* axes, const char* key, const char** values, size_t size);

/// @brief Delete an axes object.
/// @param[in] axes Axes object to destroy.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_delete_axes(gribjump_axes_t* axes);

/// @brief Perform one-time GribJump library initialisation.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_initialise();

/// @brief Get the GribJump version string.
/// @return Null-terminated version string.
const char* gribjump_version();

/// @brief Get the GribJump git SHA1 string for this build.
/// @return Null-terminated git SHA1 string.
const char* gribjump_git_sha1();

/// @brief Delete an extraction iterator.
/// @param[in] it Iterator to destroy.
/// @return GRIBJUMP_SUCCESS on success, GRIBJUMP_ERROR otherwise.
gribjump_error_t gribjump_extractioniterator_delete(const gribjump_extractioniterator_t* it);

/// @brief Advance an extraction iterator and retrieve the next result.
/// @param[in,out] it Iterator to advance.
/// @param[out] result Pointer receiving the next result object.
/// @return GRIBJUMP_ITERATOR_SUCCESS when a result is returned,
///         GRIBJUMP_ITERATOR_COMPLETE when no more results remain,
///         or GRIBJUMP_ITERATOR_ERROR on failure.
gribjump_iterator_status_t gribjump_extractioniterator_next(gribjump_extractioniterator_t* it,
                                                            gribjump_extraction_result_t** result);

/// @brief Get the most recent thread-local GribJump error message.
/// @return Null-terminated human-readable error string.
const char* gribjump_error_string();

#ifdef __cplusplus
}  // extern "C"
#endif
