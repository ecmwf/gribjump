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
#include "gribjump/GribJump.h"
#include "eckit/runtime/Main.h"
#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsParser.h"

using namespace gribjump;

// Note: We don't do much in terms of error handling here.

extern "C" {

struct gribjump_handle_t : public GribJump {
    using GribJump::GribJump;
};

// todo deleter
struct gribjump_extraction_result_t: public ExtractionResult {
    using ExtractionResult::ExtractionResult;

    gribjump_extraction_result_t(const ExtractionResult& result): ExtractionResult(result) {}
};

// todo deleter
struct gribjump_extraction_request_t: public ExtractionRequest {
    using ExtractionRequest::ExtractionRequest;

    gribjump_extraction_request_t(const ExtractionRequest& request): ExtractionRequest(request) {}
};

int gribjump_new_handle(gribjump_handle_t** handle) {
    *handle = new gribjump_handle_t();
    return 0;
}

int gribjump_delete_handle(gribjump_handle_t* handle) {
    ASSERT(handle);
    delete handle;
    return 0;
}

int gribjump_new_request(gribjump_extraction_request_t** request, const char* reqstr, const char* rangesstr) {
    // reqstr is a string representation of a metkit::mars::MarsRequest
    // rangesstr is a comma-separated list of ranges, e.g. "0-10,20-30"
    
    // NB: Treat the requests as raw requests.
    std::istringstream iss(reqstr);
    metkit::mars::MarsParser parser(iss);
    std::vector<metkit::mars::MarsParsedRequest> requests = parser.parse();
    ASSERT(requests.size() == 1);
    metkit::mars::MarsRequest mreq(requests[0]);


    // Parse the ranges string
    std::vector<std::string> ranges = eckit::StringTools::split(",", rangesstr);
    std::vector<Range> rangevec;
    for (const auto& range : ranges) {
        std::vector<std::string> kv = eckit::StringTools::split("-", range);
        ASSERT(kv.size() == 2);
        rangevec.push_back(std::make_pair(std::stoi(kv[0]), std::stoi(kv[1])));
    }

    *request = new gribjump_extraction_request_t(mreq, rangevec);

    return 0;
}

int gribjump_delete_request(gribjump_extraction_request_t* request) {
    ASSERT(request);
    delete request;
    return 0;
}

int gribjump_new_result(gribjump_extraction_result_t** result) { // not sure if this is needed
    // set to null
    *result = nullptr;
    return 0;
}

int gribjump_result_values(gribjump_extraction_result_t* result, double*** values, unsigned long* nrange, unsigned long** nvalues){
    // makes a copy of the values
    ASSERT(result);
    std::vector<std::vector<double>> vals = result->values();
    *nrange = vals.size();
    *values = new double*[*nrange];
    *nvalues = new unsigned long[*nrange];
    for (size_t i = 0; i < *nrange; i++) {
        (*nvalues)[i] = vals[i].size();
        (*values)[i] = new double[(*nvalues)[i]];
        for (size_t j = 0; j < (*nvalues)[i]; j++) {
            (*values)[i][j] = vals[i][j];
        }
    }
    return 0;
}

int gribjump_result_mask(gribjump_extraction_result_t* result, unsigned long long*** masks, unsigned long* nrange, unsigned long** nmasks){
    // makes a copy of the mask, converting from bitset to uint64_t
    // TODO(Chris): Why does my py code handle uint64_t instead of unsigned long long, when pyfdb handles it fine?
    ASSERT(result);
    std::vector<std::vector<std::bitset<64>>> msk = result->mask();
    *nrange = msk.size();
    *masks = new unsigned long long*[*nrange];
    *nmasks = new unsigned long[*nrange];
    for (size_t i = 0; i < *nrange; i++) {
        (*nmasks)[i] = msk[i].size();
        (*masks)[i] = new unsigned long long[(*nmasks)[i]];
        for (size_t j = 0; j < (*nmasks)[i]; j++) {
            (*masks)[i][j] = msk[i][j].to_ullong();
        }
    }
    return 0;
}

int gribjump_result_values_nocopy(gribjump_extraction_result_t* result, double*** values, unsigned long* nrange, unsigned long** nvalues){
    ASSERT(result);
    result->values_ptr(values, nrange, nvalues);
    return 0;
}

int gribjump_delete_result(gribjump_extraction_result_t* result) {
    ASSERT(result);
    delete result;
    return 0;
}

int extract_single(gribjump_handle_t* handle, gribjump_extraction_request_t* request, gribjump_extraction_result_t*** results_array, unsigned short* nfields) {
    ExtractionRequest req = *request;
    std::vector<ExtractionResult*> results = handle->extract(std::vector<ExtractionRequest>{req})[0];

    *nfields = results.size();
    *results_array = new gribjump_extraction_result_t*[*nfields];

    for (size_t i = 0; i < *nfields; i++) {
        (*results_array)[i] = new gribjump_extraction_result_t(*results[i]);
    }

    return 0;
}
int extract(gribjump_handle_t* handle, gribjump_extraction_request_t** requests, unsigned short nrequests, gribjump_extraction_result_t**** results_array, unsigned short** nfields) {
    std::vector<ExtractionRequest> reqs;
    for (size_t i = 0; i < nrequests; i++) {
        reqs.push_back(*requests[i]);
    }
    std::vector<std::vector<ExtractionResult*>> results = handle->extract(reqs);

    *nfields = new unsigned short[nrequests];
    *results_array = new gribjump_extraction_result_t**[nrequests];

    for (size_t i = 0; i < nrequests; i++) {
        (*nfields)[i] = results[i].size();
        (*results_array)[i] = new gribjump_extraction_result_t*[(*nfields)[i]];
        for (size_t j = 0; j < (*nfields)[i]; j++) {
            (*results_array)[i][j] = new gribjump_extraction_result_t(*results[i][j]);
        }
    }
    return 0;
}

struct gj_axes_t {
public:
    gj_axes_t(std::map<std::string, std::unordered_set<std::string>> values): values_(values) {}

    void print() {
        for (const auto& kv : values_) {
            std::cout << kv.first << ": ";
            for (const auto& v : kv.second) {
                std::cout << v << ", ";
            }
            std::cout << std::endl;
        }
    }

    void keys(const char ***keys_out, unsigned long* size) {
        const char** keys = new const char*[values_.size()];
        int i = 0;
        for (const auto& v : values_) {
            const auto& key = v.first;
            keys[i++] = key.c_str();
        }
        *size = values_.size();
        *keys_out = keys;
    }

    void values(const char* key, const char*** values_out, unsigned long* size) {
        ASSERT(values_.find(key) != values_.end());
        // Note its up to the caller to free the memory
        const char** values = new const char*[values_[key].size()];
        int i = 0;
        for (const auto& value : values_[key]) {
            values[i++] = value.c_str();
        }
        *size = values_[key].size();
        *values_out = values;
    }
private:
    std::map<std::string, std::unordered_set<std::string>> values_;
};

int gribjump_new_axes(gj_axes_t** axes, const char* reqstr, gribjump_handle_t* gj) {
    ASSERT(gj);
    std::string reqstr_str(reqstr);
    std::map<std::string, std::unordered_set<std::string>> values = gj->axes(reqstr_str);
    *axes = new gj_axes_t(values);
    return 0;
}

int gribjump_axes_keys(gj_axes_t* axes, const char*** keys_out, unsigned long* size) {
    ASSERT(axes);
    axes->keys(keys_out, size);
    return 0;
}

int gribjump_axes_values(gj_axes_t* axes, const char* key, const char*** values_out, unsigned long* size) {
    ASSERT(axes);
    axes->values(key, values_out, size);
    return 0;
}

int gribjump_delete_axes(gj_axes_t* axes) {
    ASSERT(axes);
    delete axes;
    return 0;
}

/*
 * Initialise API
 * @note This is only required if being used from a context where Main()
 *       is not otherwise initialised
*/
int gribjump_initialise() {
    static bool initialised = false;

    if (initialised) {
        eckit::Log::warning() << "Initialising gribjump library twice" << std::endl;
    }

    if (!initialised) {
        const char* argv[2] = {"gribjump-api", 0};
        eckit::Main::initialise(1, const_cast<char**>(argv));
        initialised = true;
    }
    return 0;
}

} // extern "C"
