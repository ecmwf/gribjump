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

#include "gribjump/gribjump_c.h"
#include <functional>
#include "eckit/runtime/Main.h"
#include "eckit/utils/StringTools.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/api/ExtractionIterator.h"

using namespace gribjump;


// --------------------------------------------------------------------------------------------
// Error handling
static std::string LAST_ERROR_STR;

const char* gribjump_error_string(gribjump_error_t err) {
    switch (err) {
        case 1:
            return LAST_ERROR_STR.c_str();
        default:
            return "Unknown error";
    };
}
namespace {

// gribjump_error_t innerWrapFn(std::function<gribjump_error_t()> f) {
//     return f();
// }

gribjump_error_t innerWrapFn(std::function<void()> f) {
    f();
    return GRIBJUMP_SUCCESS;
}

template <typename FN>
[[nodiscard]] gribjump_error_t tryCatch(FN&& fn) {
    try {
        return innerWrapFn(std::forward<FN>(fn));
    }
    catch (const std::exception& e) {
        eckit::Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        LAST_ERROR_STR = e.what();
        return GRIBJUMP_ERROR;
    }
    catch (...) {
        eckit::Log::error() << "Caught unknown on C-C++ API boundary" << std::endl;
        LAST_ERROR_STR = "Unknown exception";
        return GRIBJUMP_ERROR;
    }
}

}  // namespace

// --------------------------------------------------------------------------------------------

struct gribjump_handle_t : public GribJump {
    using GribJump::GribJump;
};

struct gribjump_extraction_result_t : public ExtractionResult {
    using ExtractionResult::ExtractionResult;

    explicit gribjump_extraction_result_t(std::unique_ptr<ExtractionResult> result) :
        ExtractionResult(std::move(*result)) {}
};

struct gribjump_extraction_request_t : public ExtractionRequest {
    using ExtractionRequest::ExtractionRequest;

    gribjump_extraction_request_t(const ExtractionRequest& request) : ExtractionRequest(request) {}
};

// Wrapper around ExtractionIterator
struct gribjump_extractioniterator_t : public ExtractionIterator {
    using ExtractionIterator::ExtractionIterator;

    gribjump_extractioniterator_t(ExtractionIterator&& it) : ExtractionIterator(std::move(it)) {}
};

struct gribjump_axes_t {
public:

    gribjump_axes_t(std::map<std::string, std::unordered_set<std::string>> values) : values_(values) {}

    void print() {
        for (const auto& kv : values_) {
            std::cout << kv.first << ": ";
            for (const auto& v : kv.second) {
                std::cout << v << ", ";
            }
            std::cout << std::endl;
        }
    }

    void keys_old(const char*** keys_out, unsigned long* size) {
        const char** keys = new const char*[values_.size()];
        int i             = 0;
        for (const auto& v : values_) {
            const auto& key = v.first;
            keys[i++]       = key.c_str();
        }
        *size     = values_.size();
        *keys_out = keys;
    }

    void values_old(const char* key, const char*** values_out, unsigned long* size) {
        ASSERT(values_.find(key) != values_.end());
        // Note its up to the caller to free the memory
        const char** values = new const char*[values_[key].size()];
        int i               = 0;
        for (const auto& value : values_[key]) {
            values[i++] = value.c_str();
        }
        *size       = values_[key].size();
        *values_out = values;
    }


    size_t size() const { return values_.size(); }

    size_t size(const std::string& key) const {
        auto it = values_.find(key);
        if (it != values_.end()) {
            return it->second.size();
        }
        return 0;
    }

    void values(const char* key, const char** values_out, size_t size) {
        ASSERT(values_.find(key) != values_.end());
        ASSERT(size == values_[key].size());
        int i = 0;
        for (const auto& value : values_[key]) {
            values_out[i++] = value.c_str();
        }
    }

    void keys(const char** keys_out, size_t size) {
        ASSERT(size == values_.size());
        int i = 0;
        for (const auto& v : values_) {
            const auto& key = v.first;
            keys_out[i++]   = key.c_str();
        }
    }

private:

    std::map<std::string, std::unordered_set<std::string>> values_;
};


gribjump_error_t gribjump_new_handle(gribjump_handle_t** handle) {
    return tryCatch([=] { *handle = new gribjump_handle_t(); });
}

gribjump_error_t gribjump_delete_handle(gribjump_handle_t* handle) {
    return tryCatch([=] { delete handle; });
}

gribjump_error_t gribjump_new_request_old(gribjump_extraction_request_t** request, const char* reqstr,
                                          const char* rangesstr, const char* gridhash) {
    return tryCatch([=] {
        // reqstr is a request string, we *ASSUME* that it resembles a valid mars request for a SINGLE field.
        // rangesstr is a comma-separated list of ranges, e.g. "0-10,20-30"

        // Parse the ranges string
        std::vector<std::string> ranges = eckit::StringTools::split(",", rangesstr);
        std::vector<Range> rangevec;
        for (const auto& range : ranges) {
            std::vector<std::string> kv =
                eckit::StringTools::split("-", range);  // this is silly, we should just pass the values as integers
            ASSERT(kv.size() == 2);
            rangevec.push_back(std::make_pair(std::stoi(kv[0]), std::stoi(kv[1])));
        }

        std::string gridhash_str = gridhash ? std::string(gridhash) : "";
        *request                 = new gribjump_extraction_request_t(reqstr, rangevec, gridhash_str);
    });
}

gribjump_error_t gribjump_new_request(gribjump_extraction_request_t** request, const char* reqstr,
                                      const size_t* range_arr, size_t range_arr_size, const char* gridhash) {
    return tryCatch([=] {
        ASSERT(request);
        ASSERT(reqstr);
        ASSERT(range_arr);
        ASSERT(range_arr_size % 2 == 0);
        std::vector<Range> ranges;
        for (size_t i = 0; i < range_arr_size; i += 2) {
            ranges.push_back(std::make_pair(range_arr[i], range_arr[i + 1]));
        }

        std::string gridhash_str = gridhash ? std::string(gridhash) : "";
        *request                 = new gribjump_extraction_request_t(reqstr, ranges, gridhash_str);
    });
}

gribjump_error_t gribjump_delete_request(gribjump_extraction_request_t* request) {
    return tryCatch([=] {
        ASSERT(request);
        delete request;
    });
}

///@todo not sure if this is needed
gribjump_error_t gribjump_new_result(gribjump_extraction_result_t** result) {
    return tryCatch([=] { *result = nullptr; });
}

// makes a copy of the values
gribjump_error_t gribjump_result_values_old(gribjump_extraction_result_t* result, double*** values,
                                            unsigned long* nrange, unsigned long** nvalues) {
    return tryCatch([=] {
        ASSERT(result);
        std::vector<std::vector<double>> vals = result->values();
        *nrange                               = vals.size();
        *values                               = new double*[*nrange];
        *nvalues                              = new unsigned long[*nrange];
        for (size_t i = 0; i < *nrange; i++) {
            (*nvalues)[i] = vals[i].size();
            (*values)[i]  = new double[(*nvalues)[i]];
            for (size_t j = 0; j < (*nvalues)[i]; j++) {
                (*values)[i][j] = vals[i][j];
            }
        }
    });
}

// Copy results from ExtractionResult into externally allocated array.
gribjump_error_t gribjump_result_values(gribjump_extraction_result_t* result, double** values, size_t nvalues) {
    return tryCatch([=] {
        ASSERT(result);
        ASSERT(values);
        size_t count = 0;
        ASSERT(result->total_values() == nvalues);
        for (auto& vals : result->values()) {
            for (size_t j = 0; j < vals.size(); j++) {
                (*values)[count++] = vals[j];
            }
        }
        ASSERT(count == nvalues);
    });
}

// Note: mask is encoded as 64-bit unsigned integers.
// So if N values were extracted in a range, the mask array will contain 1 + floor(N/64) elements.
gribjump_error_t gribjump_result_mask(gribjump_extraction_result_t* result, unsigned long long** masks, size_t nmasks) {
    return tryCatch([=] {
        ASSERT(result);
        ASSERT(masks);
        size_t count = 0;
        for (auto& msk : result->mask()) {
            for (size_t j = 0; j < msk.size(); j++) {
                (*masks)[count++] = msk[j].to_ullong();
            }
        }
        ASSERT(count == nmasks);
    });
}

// makes a copy of the mask, converting from bitset to uint64_t
// TODO(Chris): Why does my py code handle uint64_t instead of unsigned long long, when pyfdb handles it fine?
gribjump_error_t gribjump_result_mask_old(gribjump_extraction_result_t* result, unsigned long long*** masks,
                                          unsigned long* nrange, unsigned long** nmasks) {
    return tryCatch([=] {
        ASSERT(result);
        std::vector<std::vector<std::bitset<64>>> msk = result->mask();
        *nrange                                       = msk.size();
        *masks                                        = new unsigned long long*[*nrange];
        *nmasks                                       = new unsigned long[*nrange];
        for (size_t i = 0; i < *nrange; i++) {
            (*nmasks)[i] = msk[i].size();
            (*masks)[i]  = new unsigned long long[(*nmasks)[i]];
            for (size_t j = 0; j < (*nmasks)[i]; j++) {
                (*masks)[i][j] = msk[i][j].to_ullong();
            }
        }
    });
}

gribjump_error_t gribjump_result_values_nocopy_old(gribjump_extraction_result_t* result, double*** values,
                                                   unsigned long* nrange, unsigned long** nvalues) {
    return tryCatch([=] {
        ASSERT(result);
        result->values_ptr(values, nrange, nvalues);
    });
}

gribjump_error_t gribjump_delete_result(gribjump_extraction_result_t* result) {
    return tryCatch([=] {
        ASSERT(result);
        delete result;
    });
}

/// @todo review why this extract_single exists.
gribjump_error_t extract_single(gribjump_handle_t* handle, gribjump_extraction_request_t* request,
                                gribjump_extraction_result_t*** results_array, unsigned long* nfields) {
    return tryCatch([=] {
        ExtractionRequest req              = *request;
        std::vector<ExtractionRequest> vec = {req};
        std::vector<std::unique_ptr<ExtractionResult>> results;

        ExtractionIterator it = handle->extract(vec);
        while (it.hasNext()) {
            results.push_back(it.next());
        }


        *nfields       = results.size();
        *results_array = new gribjump_extraction_result_t*[*nfields];

        for (size_t i = 0; i < *nfields; i++) {
            (*results_array)[i] =
                new gribjump_extraction_result_t(std::move(results[i]));  // not convinced this is safe
        }
    });
}

gribjump_error_t extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests, unsigned long nrequests,
                         gribjump_extraction_result_t**** results_array, unsigned long** nfields, const char* ctx) {
    return tryCatch([=] {
        std::vector<ExtractionRequest> reqs;
        for (size_t i = 0; i < nrequests; i++) {
            reqs.push_back(*requests[i]);
        }
        LogContext logctx;
        if (ctx) {
            logctx = LogContext(ctx);
        }

        ///@todo: the new way
        // std::vector<std::unique_ptr<ExtractionResult>> results = handle->extract(reqs, logctx);
        // NOTIMP;
        std::vector<std::unique_ptr<ExtractionResult>> results = handle->extract(reqs, logctx).dumpVector();

        *nfields       = new unsigned long[nrequests];
        *results_array = new gribjump_extraction_result_t**[nrequests];
        // todo: update c api to reflect dimensionality changes // or, return the iterator directly.
        size_t nfields_per_request = 1;
        for (size_t i = 0; i < nrequests; i++) {
            (*nfields)[i]       = nfields_per_request;
            (*results_array)[i] = new gribjump_extraction_result_t*[(*nfields)[i]];
            (*results_array)[i][0] =
                new gribjump_extraction_result_t(std::move(results[i]));  // todo: this has one useless dimension now.
        }
    });
}

gribjump_error_t gribjump_extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests,
                                  unsigned long nrequests, const char* ctx, gribjump_extractioniterator_t** iterator) {
    return tryCatch([=] {
        std::vector<ExtractionRequest> reqs;
        for (size_t i = 0; i < nrequests; i++) {
            reqs.push_back(*requests[i]);
        }

        LogContext logctx;
        if (ctx)
            logctx = LogContext(ctx);

        *iterator = new gribjump_extractioniterator_t(handle->extract(reqs, logctx));
    });
}

// --------------------------------------------------------------------------------------------
// gribjump_axes_t
// --------------------------------------------------------------------------------------------

gribjump_error_t gribjump_new_axes(gribjump_handle_t* gj, const char* reqstr, int level, const char* ctx,
                                   gribjump_axes_t** axes) {
    return tryCatch([=] {
        ASSERT(gj);
        LogContext logctx;
        if (ctx) {
            logctx = LogContext(ctx);
        }
        std::string reqstr_str(reqstr);
        std::map<std::string, std::unordered_set<std::string>> values;
        values = gj->axes(reqstr_str, level, logctx);
        *axes  = new gribjump_axes_t(values);
    });
}

gribjump_error_t gribjump_axes_keys_old(gribjump_axes_t* axes, const char*** keys_out, unsigned long* size) {
    return tryCatch([=] {
        ASSERT(axes);
        axes->keys_old(keys_out, size);
    });
}

gribjump_error_t gribjump_axes_keys(gribjump_axes_t* axes, const char** keys, size_t size) {
    return tryCatch([=] {
        ASSERT(axes);
        axes->keys(keys, size);
    });
}

gribjump_error_t gribjump_axes_values_old(gribjump_axes_t* axes, const char* key, const char*** values_out,
                                          unsigned long* size) {
    return tryCatch([=] {
        ASSERT(axes);
        axes->values_old(key, values_out, size);
    });
}


gribjump_error_t gribjump_axes_keys_size(gribjump_axes_t* axes, size_t* size) {
    return tryCatch([=] {
        ASSERT(axes);
        *size = axes->size();
    });
}

gribjump_error_t gribjump_axes_values_size(gribjump_axes_t* axes, const char* key, size_t* size) {
    return tryCatch([=] {
        ASSERT(axes);
        ASSERT(key);
        *size = axes->size(key);
    });
}

gribjump_error_t gribjump_axes_values(gribjump_axes_t* axes, const char* key, const char** values, size_t size) {
    return tryCatch([=] {
        ASSERT(axes);
        ASSERT(key);
        ASSERT(values);
        axes->values(key, values, size);
    });
}

gribjump_error_t gribjump_delete_axes(gribjump_axes_t* axes) {
    return tryCatch([=] {
        ASSERT(axes);
        delete axes;
    });
}

// -----------------------------------------------------------------------------
// gribjump_extractioniterator_t
// -----------------------------------------------------------------------------

gribjump_error_t gribjump_extractioniterator_delete(const gribjump_extractioniterator_t* it) {
    return tryCatch([it] { delete it; });
}

gribjump_iterator_status_t gribjump_extractioniterator_next(gribjump_extractioniterator_t* it,
                                                            gribjump_extraction_result_t** result) {
    if (!it) {
        return GRIBJUMP_ITERATOR_ERROR;
    }

    std::unique_ptr<ExtractionResult> res = it->next();
    if (res) {
        *result = new gribjump_extraction_result_t(std::move(res));
        return GRIBJUMP_ITERATOR_SUCCESS;
    }
    else {
        return GRIBJUMP_ITERATOR_COMPLETE;
    }
}

/*
 * Initialise API
 * @note This is only required if being used from a context where Main()
 *       is not otherwise initialised
 */
gribjump_error_t gribjump_initialise() {
    return tryCatch([] {
        static bool initialised = false;

        if (initialised) {
            eckit::Log::warning() << "Initialising gribjump library twice" << std::endl;
        }

        if (!initialised) {
            const char* argv[2] = {"gribjump-api", 0};
            eckit::Main::initialise(1, const_cast<char**>(argv));
            initialised = true;
        }
    });
}
