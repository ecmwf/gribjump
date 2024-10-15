/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include "eckit/serialisation/Stream.h"
#include "eckit/log/Timer.h"
#include "eckit/log/JSON.h"
#include "eckit/log/TimeStamp.h"

namespace gribjump {

class LogContext {
public:
    LogContext(std::string s="none") : context_(s) {}

    explicit LogContext(eckit::Stream& s) {
        s >> context_;
    }
    
    ~LogContext() {}
private:
    void encode(eckit::Stream& s) const {
        s << context_;
    }
    friend eckit::Stream& operator<<(eckit::Stream& s, const LogContext& o){
        o.encode(s);
        return s;
    }
    void print(std::ostream& s) const {
        s << context_;
    }
    friend std::ostream& operator<<(std::ostream& s, const LogContext& o){
        o.print(s);
        return s;
    }

private:
    std::string context_;
};


// --------------------------------------------------------------------------------------------------------------------------------

class Metrics {

public: // methods
    
    Metrics(LogContext ctx) : context_(ctx) {
    }
    
    ~Metrics() {}

    void report() {
        eckit::Log::metrics() << "{"
        << "action:" << action
        << ",start_time:" << start_
        << ",end_time:" << eckit::TimeStamp()
        << ",count_requests" << nRequests
        << ",count_tasks:" << nTasks
        << ",count_failed_tasks:" << nFailedTasks
        << ",elapsed_receive:" << timeReceive
        << ",elapsed_execute:" << timeExecute
        << ",elapsed_reply:" << timeReply
        << ",elapsed_total:" << timer_.elapsed()
        << ",Context:" << context_
        << "}" << std::endl;
    }

public: // members

    std::string action;
    int nRequests = -1;
    int nTasks = -1;
    int nFailedTasks = -1;
    double timeReceive = 0;
    double timeExecute = 0;
    double timeReply = 0;

private: // members

    LogContext context_;
    eckit::Timer timer_;
    eckit::TimeStamp start_;
};

} // namespace gribjump
