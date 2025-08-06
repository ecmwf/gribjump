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
#include <sstream>
#include "eckit/runtime/Main.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/api/ExtractionIterator.h"
#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"

using namespace gribjump;


// --------------------------------------------------------------------------------------------
// Error handling
static std::string LAST_ERROR_STR = "Unknown error";
const char* gribjump_error_string() {
    return LAST_ERROR_STR.c_str();
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

struct gribjump_path_extraction_request_t : public PathExtractionRequest {
    using PathExtractionRequest::PathExtractionRequest;

    gribjump_path_extraction_request_t(const PathExtractionRequest& request) : PathExtractionRequest(request) {}
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

gribjump_error_t gribjump_new_request_from_path(gribjump_path_extraction_request_t** request, const char* filename,
                                                const char* scheme, size_t offset, const char* host, int port,
                                                const size_t* range_arr, size_t range_arr_size, const char* gridhash) {
    return tryCatch([=] {
        ASSERT(request);
        ASSERT(filename);
        ASSERT(scheme);
        ASSERT(host);
        ASSERT(range_arr);
        ASSERT(range_arr_size % 2 == 0);
        std::vector<Range> ranges;
        for (size_t i = 0; i < range_arr_size; i += 2) {
            ranges.push_back(std::make_pair(range_arr[i], range_arr[i + 1]));
        }

        std::string gridhash_str = gridhash ? std::string(gridhash) : "";
        *request = new gribjump_path_extraction_request_t(filename, scheme, offset, host, port, ranges, gridhash_str);
    });
}

gribjump_error_t gribjump_delete_request(gribjump_extraction_request_t* request) {
    return tryCatch([=] {
        ASSERT(request);
        delete request;
    });
}

gribjump_error_t gribjump_delete_path_request(gribjump_path_extraction_request_t* request) {
    return tryCatch([=] {
        ASSERT(request);
        delete request;
    });
}

///@todo not sure if this is needed
gribjump_error_t gribjump_new_result(gribjump_extraction_result_t** result) {
    return tryCatch([=] { *result = nullptr; });
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

gribjump_error_t gribjump_delete_result(gribjump_extraction_result_t* result) {
    return tryCatch([=] {
        ASSERT(result);
        delete result;
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

gribjump_error_t gribjump_extract_from_paths(gribjump_handle_t* handle, gribjump_path_extraction_request_t** requests,
                                             unsigned long nrequests, const char* ctx,
                                             gribjump_extractioniterator_t** iterator) {
    return tryCatch([=] {
        std::vector<PathExtractionRequest> reqs;
        reqs.reserve(nrequests);
        for (size_t i = 0; i < nrequests; i++) {
            reqs.push_back(*requests[i]);
        }

        LogContext logctx;
        if (ctx)
            logctx = LogContext(ctx);

        *iterator = new gribjump_extractioniterator_t(handle->extract(reqs, logctx));
    });
}

gribjump_error_t gribjump_extract_single(gribjump_handle_t* handle, const char* request, const size_t* range_arr,
                                         size_t range_arr_size, const char* gridhash, const char* ctx,
                                         gribjump_extractioniterator_t** iterator) {
    return tryCatch([=] {
        ASSERT(handle);
        ASSERT(request);
        ASSERT(range_arr);
        ASSERT(range_arr_size % 2 == 0);

        std::vector<Range> ranges;
        for (size_t i = 0; i < range_arr_size; i += 2) {
            ranges.push_back(std::make_pair(range_arr[i], range_arr[i + 1]));
        }

        LogContext logctx;
        if (ctx)
            logctx = LogContext(ctx);

        std::string gridhash_str = gridhash ? std::string(gridhash) : "";

        // Parse the mars request
        std::istringstream in(request);
        metkit::mars::MarsParser parser(in);
        metkit::mars::MarsExpension expand(false, true);
        auto v = expand.expand(parser.parse());
        ASSERT(v.size() == 1);
        metkit::mars::MarsRequest req = v[0];

        *iterator = new gribjump_extractioniterator_t(handle->extract(req, ranges, gridhash_str, logctx));
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

gribjump_error_t gribjump_axes_keys(gribjump_axes_t* axes, const char** keys, size_t size) {
    return tryCatch([=] {
        ASSERT(axes);
        axes->keys(keys, size);
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
        LAST_ERROR_STR = "gribjump_extractioniterator_next: iterator is null";
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
