#include "gribjump/gribjump_c.h"
#include "gribjump/GribJump.h"
#include "eckit/runtime/Main.h"
#include "eckit/utils/StringTools.h"

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

    metkit::mars::MarsRequest mreq(metkit::mars::MarsRequest::parse(reqstr));
    std::cout << mreq << std::endl;

    // Parse the ranges string
    std::vector<std::string> ranges = eckit::StringTools::split(",", rangesstr);
    std::vector<Range> rangevec;
    for (const auto& range : ranges) {
        std::vector<std::string> kv = eckit::StringTools::split("-", range);
        ASSERT(kv.size() == 2);
        rangevec.push_back(std::make_tuple(std::stoi(kv[0]), std::stoi(kv[1])));
    }

    *request = new gribjump_extraction_request_t(mreq, rangevec);

    std::cout << **request << std::endl;

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
    eckit::Log::info() << "Creating result " << result << std::endl;
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
    // TODO: Why does my py code handle uint64_t instead of unsigned long long, when pyfdb handles it fine?
    ASSERT(result);
    std::vector<std::vector<std::bitset<64>>> msk = result->mask();
    *nrange = msk.size();
    *masks = new uint64_t*[*nrange];
    *nmasks = new unsigned long[*nrange];
    for (size_t i = 0; i < *nrange; i++) {
        (*nmasks)[i] = msk[i].size();
        (*masks)[i] = new uint64_t[(*nmasks)[i]];
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
    eckit::Log::info() << "Deleting result " << result << std::endl;
    delete result;
    return 0;
}

int extract(gribjump_handle_t* handle, gribjump_extraction_request_t* request, gribjump_extraction_result_t*** results_array, unsigned short* nfields) {
    std::vector<ExtractionResult> results = handle->extract(request->getRequest(), request->getRanges());

    *nfields = results.size();
    *results_array = new gribjump_extraction_result_t*[*nfields];

    for (size_t i = 0; i < *nfields; i++) {
        (*results_array)[i] = new gribjump_extraction_result_t(results[i]);
        eckit::Log::info() << "Created result " << i << results[i] << std::endl;
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

private:
    std::map<std::string, std::unordered_set<std::string>> values_;
};

int gribjump_new_axes(gj_axes_t** axes, const char* reqstr, gribjump_handle_t* gj) {
    ASSERT(gj);
    std::string reqstr_str(reqstr);
    std::map<std::string, std::unordered_set<std::string>> values = gj->axes(reqstr_str);
    *axes = new gj_axes_t(values);
    (*axes)->print();
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
