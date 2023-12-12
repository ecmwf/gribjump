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

# include "gribjump/remote/GribJumpUser.h"

namespace gribjump {

GribJumpUser::GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){}

GribJumpUser::~GribJumpUser() {}

void GribJumpUser::serve(eckit::Stream& s, std::istream& in, std::ostream& out){
    // NB: Use EITHER s OR in/out, not both. We use s.
    // TODO: Monitor?

    eckit::Log::info() << "Serving new connection" << std::endl;

    try {
        std::string cmd;
        s >> cmd;
        eckit::Log::info() << "Received cmd: " << cmd << std::endl;
        GribJump gj;

        if (cmd == "EXTRACT"){
            // create a vector of ExtractionRequests from the stream    
            std::vector<ExtractionRequest> requests;
            size_t numRequests;
            s >> numRequests;
            for (size_t i = 0; i < numRequests; i++) {
                ExtractionRequest exrequest = ExtractionRequest(s);
                requests.push_back(exrequest);
            }

            eckit::Log::info() << "Received " << numRequests << " requests" << std::endl;

            std::vector<std::vector<ExtractionResult>> output = gj.extract(requests);

            eckit::Log::info() << "Extract finished. Sending results" << std::endl;
            for (auto& vec : output) {
                size_t nfields = vec.size();
                s << nfields;
                for (auto& o : vec) {
                    s << o;
                }
            }
            eckit::Log::info() << "Sent results" << std::endl;

        }

        else if (cmd == "AXES"){
            throw eckit::SeriousBug("AXES still to do...", Here());
        }

        else {
            throw eckit::SeriousBug("Unknown command " + cmd, Here());
        }
    }
    
    catch (std::exception& e) {
        eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
        eckit::Log::error() << "** Exception is handled" << std::endl;
        try {
            s << e;
        }
        catch (std::exception& a) {
            eckit::Log::error() << "** " << a.what() << " Caught in " << Here() << std::endl;
            eckit::Log::error() << "** Exception is ignored" << std::endl;
        }
    }

    eckit::Log::info() << "Closing connection" << std::endl;
}

}  // namespace gribjump
