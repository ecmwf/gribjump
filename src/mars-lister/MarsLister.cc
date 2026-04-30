/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "MarsLister.h"

#include <sstream>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Log.h"
#include "metkit/mars/MarsRequest.h"

namespace marslister {

//----------------------------------------------------------------------------------------------------------------------

MarsListerUser::MarsListerUser(eckit::net::TCPSocket& protocol) : NetUser(protocol) {}

MarsListerUser::~MarsListerUser() {}

void MarsListerUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out) {
    eckit::Log::info() << "MarsLister: serving new connection" << std::endl;

    try {
        handle(s);
    }
    catch (std::exception& e) {
        eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
        eckit::Log::error() << "** Exception is handled" << std::endl;
        try {
            s << e;
        }
        catch (...) {
            eckit::Log::error() << "** Exception is ignored" << std::endl;
        }
    }
    catch (...) {
        eckit::Log::error() << "** Unknown exception caught in " << Here() << std::endl;
        eckit::Log::error() << "** Exception is ignored" << std::endl;
    }
}

void MarsListerUser::handle(eckit::Stream& s) {
    uint16_t version;
    s >> version;

    if (version != protocolVersion) {
        throw eckit::SeriousBug(
            "MarsLister protocol mismatch: server version " + std::to_string(protocolVersion) +
            ", client version " + std::to_string(version));
    }

    uint16_t reqType;
    s >> reqType;
    RequestType requestType = static_cast<RequestType>(reqType);

    switch (requestType) {
        case RequestType::LIST:
            handleList(s);
            break;
        default:
            throw eckit::SeriousBug("MarsLister: unknown request type " + std::to_string(reqType));
    }
}

void MarsListerUser::handleList(eckit::Stream& s) {
    metkit::mars::MarsRequest request(s);

    eckit::Log::info() << "MarsLister: received request: " << request << std::endl;

    // Build JSON response with dummy URIs, aggregated by path
    // TODO: replace with real listing logic
    std::ostringstream oss;
    {
        eckit::JSON j(oss);
        j.startList();

        j.startObject();
        j << "path" << "/dummy/data/fc_20250101_00.grib";
        j << "offsets";
        j.startList(); j << 0; j << 2000; j.endList();
        j << "lengths";
        j.startList(); j << 2000; j << 1500; j.endList();
        j.endObject();

        j.startObject();
        j << "path" << "/dummy/data/fc_20250101_12.grib";
        j << "offsets";
        j.startList(); j << 0; j.endList();
        j << "lengths";
        j.startList(); j << 3000; j.endList();
        j.endObject();

        j.endList();
    }

    std::string json = oss.str();
    eckit::Log::info() << "MarsLister: sending response: " << json << std::endl;

    // Send errors count (none)
    size_t nErrors = 0;
    s << nErrors;

    // Send JSON response
    s << json;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace marslister
