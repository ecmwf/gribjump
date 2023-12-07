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

int gj_new_request(gribjump_extraction_request_t** request, const char* reqstr, const char* rangesstr) {
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

int extract(gribjump_handle_t* handle, gribjump_extraction_request_t* request, gribjump_extraction_result_t** results_array) {

    // For now, let's create a request here.

    std::vector<ExtractionResult> results = handle->extract(request->getRequest(), request->getRanges());

    size_t nfields = results.size();

    results_array = new gribjump_extraction_result_t*[nfields];
    for (size_t i = 0; i < nfields; i++) {
        results_array[i] = new gribjump_extraction_result_t(results[i]);
        eckit::Log::info() << "result " << i << ": " << results[i] << std::endl;
    }

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
}

} // extern "C"
