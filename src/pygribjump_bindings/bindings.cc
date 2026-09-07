/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/runtime/Main.h"
#include "eckit/system/Library.h"
#include "eckit/system/LibraryManager.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/Config.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/Types.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/gribjump_version.h"

namespace py = pybind11;
namespace gj = gribjump;

namespace {

//--------------------------------------------------------------------------------------------------
// @brief Helpers
//--------------------------------------------------------------------------------------------------

metkit::mars::MarsRequest mars_request_from_string(const std::string& request) {
    std::istringstream in(request);
    metkit::mars::MarsParser parser(in);

    const bool inherit = false;
    const bool strict  = true;
    metkit::mars::MarsExpansion expand(inherit, strict);

    auto expanded = expand.expand(parser.parse());
    ASSERT(expanded.size() == 1);
    return expanded[0];
}

/// The request string handed over by the python layer is only parsed and expanded if the
/// library has been configured to do so. Parsing is a bottleneck for large numbers of
/// requests, hence it stays configurable (see gribjump::ConfigOptions::requestParsing()).
gj::ExtractionRequest make_extraction_request(const std::string& request, const gj::Ranges& ranges,
                                              const std::string& grid_hash) {
    if (gj::ConfigOptions::instance().requestParsing()) {
        const auto mars_request = mars_request_from_string(request);
        return gj::ExtractionRequest(mars_request.asString(), ranges, grid_hash);
    }
    return gj::ExtractionRequest(request, ranges, grid_hash);
}

gj::LogContext log_context(const std::string& context) {
    if (context.empty()) {
        return gj::LogContext();
    }
    return gj::LogContext(context);
}

py::array_t<double> to_array(const std::vector<double>& values) {
    return py::array_t<double>(static_cast<py::ssize_t>(values.size()), values.data());
}

py::array_t<std::uint64_t> to_array(const std::vector<std::bitset<64>>& mask) {
    std::vector<std::uint64_t> buffer;
    buffer.reserve(mask.size());
    for (const auto& bits : mask) {
        buffer.emplace_back(static_cast<std::uint64_t>(bits.to_ullong()));
    }
    return py::array_t<std::uint64_t>(static_cast<py::ssize_t>(buffer.size()), buffer.data());
}

}  // namespace

PYBIND11_MODULE(pygribjump_bindings, m) {
    // Errors raised by the gribjump library (and its dependencies) are eckit exceptions.
    // They are translated into a dedicated python exception, which derives from RuntimeError
    // so that code catching either keeps working.
    py::register_local_exception<eckit::Exception>(m, "GribJumpException", PyExc_RuntimeError);

    m.def("init_bindings", []() {
        const char* args[] = {"pygribjump", ""};
        eckit::Main::initialise(1, const_cast<char**>(args));
    });

    m.def("version_info", []() {
        std::vector<std::tuple<std::string, std::string, std::string, std::string>> dependencyInformation;

        for (const std::string& libname : eckit::system::LibraryManager::list()) {
            const eckit::system::Library& lib = eckit::system::LibraryManager::lookup(libname);
            dependencyInformation.emplace_back(lib.name(), lib.version(), lib.gitsha1(), lib.libraryPath());
        }

        return dependencyInformation;
    });

    // Compile-time gribjump version
    m.attr("__gribjump_build_version__") = gribjump_VERSION_STR;

    //--------------------------------------------------
    // @brief Request classes
    //--------------------------------------------------

    py::class_<gj::ExtractionRequest>(m, "ExtractionRequest")
        .def(py::init())
        .def(py::init([](const std::string& request, const gj::Ranges& ranges, const std::string& grid_hash) {
                 return make_extraction_request(request, ranges, grid_hash);
             }),
             py::arg("request"), py::arg("ranges"), py::arg("grid_hash") = std::string{})
        .def("ranges", &gj::ExtractionRequest::ranges)
        .def("request_string",
             [](const gj::ExtractionRequest& request) { return request.requestString(); })
        .def("grid_hash", &gj::ExtractionRequest::gridHash)
        .def("__repr__", [](const gj::ExtractionRequest& request) {
            std::stringstream buf;
            buf << request;
            return buf.str();
        });

    py::class_<gj::PathExtractionRequest, gj::ExtractionRequest>(m, "PathExtractionRequest")
        .def(py::init([](const std::string& path, const std::string& scheme, std::size_t offset,
                         const std::string& host, int port, const gj::Ranges& ranges, const std::string& grid_hash) {
                 return gj::PathExtractionRequest(path, scheme, offset, host, port, ranges, grid_hash);
             }),
             py::arg("path"), py::arg("scheme"), py::arg("offset"), py::arg("host"), py::arg("port"),
             py::arg("ranges"), py::arg("grid_hash") = std::string{})
        .def("path", &gj::PathExtractionRequest::path)
        .def("scheme", &gj::PathExtractionRequest::scheme)
        .def("offset", [](const gj::PathExtractionRequest& request) { return request.offset(); })
        .def("host", &gj::PathExtractionRequest::host)
        .def("port", &gj::PathExtractionRequest::port)
        .def("__repr__", [](const gj::PathExtractionRequest& request) {
            std::stringstream buf;
            buf << request;
            return buf.str();
        });

    //--------------------------------------------------
    // @brief Result class
    //--------------------------------------------------

    py::class_<gj::ExtractionResult, py::smart_holder>(m, "ExtractionResult",
                                                       py::release_gil_before_calling_cpp_dtor())
        .def("nrange", &gj::ExtractionResult::nrange)
        .def("nvalues", &gj::ExtractionResult::nvalues)
        .def("total_values", &gj::ExtractionResult::total_values)
        .def("values",
             [](const gj::ExtractionResult& result) {
                 std::vector<py::array_t<double>> arrays;
                 arrays.reserve(result.values().size());
                 for (const auto& values : result.values()) {
                     arrays.emplace_back(to_array(values));
                 }
                 return arrays;
             })
        .def("values_flat",
             [](const gj::ExtractionResult& result) {
                 std::vector<double> flat;
                 flat.reserve(result.total_values());
                 for (const auto& values : result.values()) {
                     flat.insert(flat.end(), values.begin(), values.end());
                 }
                 return to_array(flat);
             })
        .def("mask",
             [](const gj::ExtractionResult& result) {
                 std::vector<py::array_t<std::uint64_t>> arrays;
                 arrays.reserve(result.mask().size());
                 for (const auto& mask : result.mask()) {
                     arrays.emplace_back(to_array(mask));
                 }
                 return arrays;
             })
        .def("mask_flat",
             [](const gj::ExtractionResult& result) {
                 std::vector<std::bitset<64>> flat;
                 for (const auto& mask : result.mask()) {
                     flat.insert(flat.end(), mask.begin(), mask.end());
                 }
                 return to_array(flat);
             })
        .def("__repr__", [](const gj::ExtractionResult& result) {
            std::stringstream buf;
            buf << result;
            return buf.str();
        });

    //--------------------------------------------------
    // @brief Iterator class
    //--------------------------------------------------

    py::class_<gj::ExtractionIterator, py::smart_holder>(m, "ExtractionIterator",
                                                         py::release_gil_before_calling_cpp_dtor())
        .def("__iter__", [](gj::ExtractionIterator& self) -> gj::ExtractionIterator& { return self; })
        .def(
            "__next__",
            [](gj::ExtractionIterator& iterator) -> std::unique_ptr<gj::ExtractionResult> {
                std::unique_ptr<gj::ExtractionResult> result = iterator.next();
                if (result) {
                    return result;
                }
                py::gil_scoped_acquire gil;
                throw py::stop_iteration();
            },
            py::call_guard<py::gil_scoped_release>())
        .def("has_next", &gj::ExtractionIterator::hasNext, py::call_guard<py::gil_scoped_release>());

    //--------------------------------------------------
    // @brief GribJump class
    //--------------------------------------------------

    py::class_<gj::GribJump, py::smart_holder>(m, "GribJump", py::release_gil_before_calling_cpp_dtor())
        .def(py::init(), py::call_guard<py::gil_scoped_release>())
        .def(
            "extract",
            [](gj::GribJump& gribjump, std::vector<gj::ExtractionRequest> requests, const std::string& ctx) {
                return gribjump.extract(requests, log_context(ctx));
            },
            py::arg("requests"), py::arg("ctx") = std::string{}, py::call_guard<py::gil_scoped_release>())
        .def(
            "extract_from_paths",
            [](gj::GribJump& gribjump, std::vector<gj::PathExtractionRequest> requests, const std::string& ctx) {
                return gribjump.extract(requests, log_context(ctx));
            },
            py::arg("requests"), py::arg("ctx") = std::string{}, py::call_guard<py::gil_scoped_release>())
        .def(
            "extract_single",
            [](gj::GribJump& gribjump, const std::string& request, const gj::Ranges& ranges,
               const std::string& grid_hash, const std::string& ctx) {
                const auto mars_request = mars_request_from_string(request);
                py::gil_scoped_release gil;
                return gribjump.extract(mars_request, ranges, grid_hash, log_context(ctx));
            },
            py::arg("request"), py::arg("ranges"), py::arg("grid_hash") = std::string{},
            py::arg("ctx") = std::string{})
        .def(
            "axes",
            [](gj::GribJump& gribjump, const std::string& request, int level, const std::string& ctx) {
                const auto axes = gribjump.axes(request, level, log_context(ctx));

                std::map<std::string, std::vector<std::string>> result;
                for (const auto& [key, values] : axes) {
                    result.emplace(key, std::vector<std::string>{values.begin(), values.end()});
                }
                return result;
            },
            py::arg("request"), py::arg("level") = 3, py::arg("ctx") = std::string{},
            py::call_guard<py::gil_scoped_release>())
        .def(
            "scan",
            [](gj::GribJump& gribjump, const std::vector<std::string>& paths, const std::string& ctx) {
                std::vector<eckit::PathName> path_names;
                path_names.reserve(paths.size());
                for (const auto& path : paths) {
                    path_names.emplace_back(path);
                }
                return gribjump.scan(path_names, log_context(ctx));
            },
            py::arg("paths"), py::arg("ctx") = std::string{}, py::call_guard<py::gil_scoped_release>())
        .def(
            "scan_request",
            [](gj::GribJump& gribjump, const std::string& request, bool byfiles, const std::string& ctx) {
                const auto mars_request = mars_request_from_string(request);
                py::gil_scoped_release gil;
                return gribjump.scan({mars_request}, byfiles, log_context(ctx));
            },
            py::arg("request"), py::arg("byfiles") = false, py::arg("ctx") = std::string{})
        .def("__repr__", [](const gj::GribJump&) { return std::string("GribJump()"); });
}
