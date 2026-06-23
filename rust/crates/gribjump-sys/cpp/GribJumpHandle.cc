// gribjump GribJumpHandle bridge — implementation.

#include "gribjump_exceptions.h"

#include "ExtractionIteratorHandle.h"
#include "GribJumpHandle.h"
#include "gribjump-sys/src/lib.rs.h"

#include "gribjump/ExtractionData.h"

#include "eckit/filesystem/PathName.h"
#include "metkit/mars/MarsRequest.h"

#include <vector>

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

namespace {

std::vector<gribjump::Range> to_cpp_ranges(const rust::Vec<Range>& ranges) {
    std::vector<gribjump::Range> result;
    result.reserve(ranges.size());
    for (const auto& r : ranges) {
        result.emplace_back(r.start, r.end);
    }
    return result;
}

std::vector<gribjump::ExtractionRequest> to_cpp_requests(const rust::Vec<ExtractionRequestData>& requests) {
    std::vector<gribjump::ExtractionRequest> result;
    result.reserve(requests.size());
    for (const auto& req : requests) {
        result.emplace_back(std::string(req.request_str), to_cpp_ranges(req.ranges), std::string(req.grid_hash));
    }
    return result;
}

std::vector<gribjump::PathExtractionRequest> to_cpp_path_requests(
    const rust::Vec<PathExtractionRequestData>& requests) {
    std::vector<gribjump::PathExtractionRequest> result;
    result.reserve(requests.size());
    for (const auto& req : requests) {
        result.emplace_back(std::string(req.filename), std::string(req.scheme), req.offset, std::string(req.host),
                            req.port, to_cpp_ranges(req.ranges), std::string(req.grid_hash));
    }
    return result;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

GribJumpHandle::GribJumpHandle() : impl_() {}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<ExtractionIteratorHandle> GribJumpHandle::extract(const rust::Vec<ExtractionRequestData>& requests) {
    auto cpp_requests = to_cpp_requests(requests);
    auto it           = impl_.extract(cpp_requests);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> GribJumpHandle::extract_from_paths(
    const rust::Vec<PathExtractionRequestData>& requests) {
    auto cpp_requests = to_cpp_path_requests(requests);
    auto it           = impl_.extract(cpp_requests);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> GribJumpHandle::extract_mars(rust::Str request,
                                                                       const rust::Vec<Range>& ranges,
                                                                       rust::Str grid_hash) {
    std::string request_str{request};
    auto mars_request = metkit::mars::MarsRequest::parse(request_str);
    auto cpp_ranges   = to_cpp_ranges(ranges);
    std::string hash{grid_hash};
    auto it = impl_.extract(mars_request, cpp_ranges, hash);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> GribJumpHandle::extract_from_file(const FileExtractionData& data) {
    std::string path_str{data.path};
    eckit::PathName path{path_str};

    std::vector<eckit::Offset> offsets;
    offsets.reserve(data.offsets.size());
    for (auto o : data.offsets) {
        offsets.push_back(eckit::Offset(o));
    }

    // Unflatten ranges: ranges_offsets tells us where each message's ranges start
    std::vector<std::vector<gribjump::Range>> ranges_per_message;
    ranges_per_message.reserve(data.ranges_offsets.size());

    for (size_t i = 0; i < data.ranges_offsets.size(); ++i) {
        size_t start = data.ranges_offsets[i];
        size_t end   = (i + 1 < data.ranges_offsets.size()) ? data.ranges_offsets[i + 1] : data.ranges.size();

        std::vector<gribjump::Range> msg_ranges;
        msg_ranges.reserve(end - start);
        for (size_t j = start; j < end; ++j) {
            msg_ranges.emplace_back(data.ranges[j].start, data.ranges[j].end);
        }
        ranges_per_message.push_back(std::move(msg_ranges));
    }

    auto it = impl_.extract(path, offsets, ranges_per_message);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

//----------------------------------------------------------------------------------------------------------------------

rust::Vec<AxisEntry> GribJumpHandle::axes(rust::Str request, int32_t level) {
    std::string request_str(request);
    auto cpp_axes = impl_.axes(request_str, level);

    rust::Vec<AxisEntry> result;
    result.reserve(cpp_axes.size());

    for (const auto& [key, values] : cpp_axes) {
        AxisEntry entry;
        entry.key = rust::String(key);
        entry.values.reserve(values.size());
        for (const auto& v : values) {
            entry.values.push_back(rust::String(v));
        }
        result.push_back(std::move(entry));
    }

    return result;
}

//----------------------------------------------------------------------------------------------------------------------

size_t GribJumpHandle::scan_paths(const rust::Vec<rust::String>& paths) {
    std::vector<eckit::PathName> cpp_paths;
    cpp_paths.reserve(paths.size());
    for (const auto& p : paths) {
        cpp_paths.emplace_back(std::string(p));
    }
    return impl_.scan(cpp_paths);
}

size_t GribJumpHandle::scan_requests(const rust::Vec<rust::String>& requests, bool by_files) {
    std::vector<metkit::mars::MarsRequest> cpp_requests;
    cpp_requests.reserve(requests.size());
    for (const auto& r : requests) {
        cpp_requests.emplace_back(std::string(r));
    }
    return impl_.scan(cpp_requests, by_files);
}

void GribJumpHandle::stats() {
    impl_.stats();
}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<GribJumpHandle> GribJumpHandle::create() {
    return std::make_unique<GribJumpHandle>();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
