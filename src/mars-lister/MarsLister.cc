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

#include "eckit/exception/Exceptions.h"
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
    size_t numRequests;
    s >> numRequests;

    eckit::Log::info() << "MarsLister: received " << numRequests << " request(s)" << std::endl;

    std::vector<metkit::mars::MarsRequest> requests;
    requests.reserve(numRequests);
    for (size_t i = 0; i < numRequests; i++) {
        requests.emplace_back(metkit::mars::MarsRequest(s));
    }

    // Send errors count (none)
    size_t nErrors = 0;
    s << nErrors;

    // Echo the requests back
    s << numRequests;
    for (const auto& req : requests) {
        s << req;
    }

    eckit::Log::info() << "MarsLister: echoed " << numRequests << " request(s) back to client" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace marslister
