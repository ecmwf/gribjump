/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include <memory>

#include "eckit/net/NetUser.h"
#include "eckit/serialisation/Stream.h"

#include "gribjump/GribJump.h"
namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class GJRequest {
public:
    GJRequest() {}
    ~GJRequest() {}
};

// helper to stream polyoutput
// TODO make this a method of PolyOutput
// TODO Make PolyOutput inherit from Streamable?
void polyOutputToStream(eckit::Stream& s, const PolyOutput& output){
    s << std::get<0>(output);
    // bitset is not streamable, convert to string
    std::vector<std::vector<std::string>> bitsetStrings;
    for (auto& v : std::get<1>(output)) {
        std::vector<std::string> bitsetString;
        for (auto& b : v) {
            bitsetString.push_back(b.to_string());
        }
        bitsetStrings.push_back(bitsetString);
    }
    s << bitsetStrings;
}

class GribJumpUser : public eckit::net::NetUser {
public:
    GribJumpUser(eckit::net::TCPSocket& protocol):  NetUser(protocol){
    }

    ~GribJumpUser() {}

private:  // methods
    // From net::NetUser

    virtual void serve(eckit::Stream& s, std::istream& in, std::ostream& out){
        // NB: Use EITHER s OR in/out, not both. We use s.
        // TODO: Monitor?

        eckit::Log::info() << "Serving new connection" << std::endl;

        try {
            std::string cmd;
            s >> cmd;
            eckit::Log::info() << "Received cmd: " << cmd << std::endl;
            GribJump gj;

            if (cmd == "EXTRACT"){
                size_t numRequests;
                s >> numRequests;
                eckit::Log::info() << "Expecting " << numRequests << " requests" << std::endl;
                for (size_t i = 0; i < numRequests; i++) {
                    metkit::mars::MarsRequest request(s);
                    eckit::Log::info() << "Received request: " << request << std::endl;
                    size_t numRanges;
                    s >> numRanges;
                    std::vector<Range> ranges;
                    eckit::Log::info() << "Ranges: ";
                    for (size_t j = 0; j < numRanges; j++) {
                        size_t start, end;
                        s >> start >> end;
                        ranges.push_back(std::make_tuple(start, end));
                        eckit::Log::info() << " " << start << "-" << end << ",";
                    }
                    eckit::Log::info() << std::endl;

                    std::vector<PolyOutput> output = gj.extract(request, ranges);
                    eckit::Log::info() << "Sending output" << std::endl;
                    
                    s << output.size();
                    for (auto& o : output) {
                        polyOutputToStream(s, o);
                    }

                }

                eckit::Log::info() << "Finished." << std::endl;
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

private:  // members
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
