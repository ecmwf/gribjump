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
#include "eckit/runtime/Main.h"
#include "eckit/utils/StringTools.h"
#include "gribjump/GribJump.h"
#include "gribjump/gribjump_version.h"
#include "metkit/mars/MarsParser.h"

using namespace gribjump;

extern "C" {

// --------------------------------------------------------------------------------------------
// Error handling
static std::string LAST_ERROR_STR;

const char* gribjump_error_string(int err) {
    switch (err) {
        case 1:
            return LAST_ERROR_STR.c_str();
        default:
            return "Unknown error";
    };
}
}  // extern "C"

namespace {

template <typename FN>
int wrapApiFunction(FN f) {
    try {
        f();
        return 0;
    }
    catch (std::exception& e) {
        eckit::Log::error() << "Caught exception on C-C++ API boundary: " << e.what() << std::endl;
        LAST_ERROR_STR = e.what();
        return 1;
    }
    catch (...) {
        eckit::Log::error() << "Caught unknown on C-C++ API boundary" << std::endl;
        LAST_ERROR_STR = "Unrecognised and unknown exception";
        return 1;
    }
}

}  // namespace

// --------------------------------------------------------------------------------------------

extern "C" {
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

struct gj_axes_t {
public:

    gj_axes_t(std::map<std::string, std::unordered_set<std::string>> values) : values_(values) {}

    void print() {
        for (const auto& kv : values_) {
            std::cout << kv.first << ": ";
            for (const auto& v : kv.second) {
                std::cout << v << ", ";
            }
            std::cout << std::endl;
        }
    }

    void keys(const char*** keys_out, unsigned long* size) {
        const char** keys = new const char*[values_.size()];
        int i             = 0;
        for (const auto& v : values_) {
            const auto& key = v.first;
            keys[i++]       = key.c_str();
        }
        *size     = values_.size();
        *keys_out = keys;
    }

    void values(const char* key, const char*** values_out, unsigned long* size) {
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

private:

    std::map<std::string, std::unordered_set<std::string>> values_;
};


int gribjump_new_handle(gribjump_handle_t** handle) {
    return wrapApiFunction([=] { *handle = new gribjump_handle_t(); });
}

int gribjump_delete_handle(gribjump_handle_t* handle) {
    return wrapApiFunction([=] {
        ASSERT(handle);
        delete handle;
    });
}

int gribjump_new_request(gribjump_extraction_request_t** request, const char* reqstr, const char* rangesstr,
                         const char* gridhash) {
    return wrapApiFunction([=] {
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

int gribjump_delete_request(gribjump_extraction_request_t* request) {
    return wrapApiFunction([=] {
        ASSERT(request);
        delete request;
    });
}

///@todo not sure if this is needed
int gribjump_new_result(gribjump_extraction_result_t** result) {
    return wrapApiFunction([=] { *result = nullptr; });
}

// makes a copy of the values
int gribjump_result_values(gribjump_extraction_result_t* result, double*** values, unsigned long* nrange,
                           unsigned long** nvalues) {
    return wrapApiFunction([=] {
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

// makes a copy of the mask, converting from bitset to uint64_t
// TODO(Chris): Why does my py code handle uint64_t instead of unsigned long long, when pyfdb handles it fine?
int gribjump_result_mask(gribjump_extraction_result_t* result, unsigned long long*** masks, unsigned long* nrange,
                         unsigned long** nmasks) {
    return wrapApiFunction([=] {
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

int gribjump_result_values_nocopy(gribjump_extraction_result_t* result, double*** values, unsigned long* nrange,
                                  unsigned long** nvalues) {
    return wrapApiFunction([=] {
        ASSERT(result);
        result->values_ptr(values, nrange, nvalues);
    });
}

int gribjump_delete_result(gribjump_extraction_result_t* result) {
    return wrapApiFunction([=] {
        ASSERT(result);
        delete result;
    });
}

/// @todo review why this extract_single exists.
int extract_single(gribjump_handle_t* handle, gribjump_extraction_request_t* request,
                   gribjump_extraction_result_t*** results_array, unsigned long* nfields) {
    return wrapApiFunction([=] {
        ExtractionRequest req                                                = *request;
        std::vector<ExtractionRequest> vec                                   = {req};
        std::vector<std::vector<std::unique_ptr<ExtractionResult>>> resultsv = handle->extract(vec);
        ASSERT(resultsv.size() == 1);

        std::vector<std::unique_ptr<ExtractionResult>> results = std::move(resultsv[0]);

        *nfields       = results.size();
        *results_array = new gribjump_extraction_result_t*[*nfields];

        for (size_t i = 0; i < *nfields; i++) {
            (*results_array)[i] =
                new gribjump_extraction_result_t(std::move(results[i]));  // not convinced this is safe
        }
    });
}
int extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests, unsigned long nrequests,
            gribjump_extraction_result_t**** results_array, unsigned long** nfields, const char* ctx) {
    return wrapApiFunction([=] {
        std::vector<ExtractionRequest> reqs;
        for (size_t i = 0; i < nrequests; i++) {
            reqs.push_back(*requests[i]);
        }
        LogContext logctx;
        if (ctx) {
            logctx = LogContext(ctx);
        }

        std::vector<std::vector<std::unique_ptr<ExtractionResult>>> results;
        results = handle->extract(reqs, logctx);

        *nfields       = new unsigned long[nrequests];
        *results_array = new gribjump_extraction_result_t**[nrequests];

        for (size_t i = 0; i < nrequests; i++) {
            (*nfields)[i]       = results[i].size();
            (*results_array)[i] = new gribjump_extraction_result_t*[(*nfields)[i]];
            for (size_t j = 0; j < (*nfields)[i]; j++) {
                (*results_array)[i][j] = new gribjump_extraction_result_t(std::move(results[i][j]));
            }
        }
    });
}


int gribjump_new_axes(gj_axes_t** axes, const char* reqstr, int* level, const char* ctx, gribjump_handle_t* gj) {
    return wrapApiFunction([=] {
        ASSERT(gj);
        LogContext logctx;
        if (ctx) {
            logctx = LogContext(ctx);
        }
        std::string reqstr_str(reqstr);
        std::map<std::string, std::unordered_set<std::string>> values;
        values = gj->axes(reqstr_str, *level, logctx);
        *axes  = new gj_axes_t(values);
    });
}

int gribjump_axes_keys(gj_axes_t* axes, const char*** keys_out, unsigned long* size) {
    return wrapApiFunction([=] {
        ASSERT(axes);
        axes->keys(keys_out, size);
    });
}

int gribjump_axes_values(gj_axes_t* axes, const char* key, const char*** values_out, unsigned long* size) {
    return wrapApiFunction([=] {
        ASSERT(axes);
        axes->values(key, values_out, size);
    });
}

int gribjump_delete_axes(gj_axes_t* axes) {
    return wrapApiFunction([=] {
        ASSERT(axes);
        delete axes;
    });
}

/*
 * Initialise API
 * @note This is only required if being used from a context where Main()
 *       is not otherwise initialised
 */
int gribjump_initialise() {
    return wrapApiFunction([] {
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

int gribjump_version_c(const char** version) {
    return wrapApiFunction([version] { (*version) = gribjump_version_str(); });
}

int gribjump_git_sha1_c(const char** sha1) {
    return wrapApiFunction([sha1] { (*sha1) = gribjump_git_sha1(); });
}

}  // extern "C"
