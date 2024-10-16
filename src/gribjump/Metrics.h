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

    void json(eckit::JSON& s) const {
        s << context_;
    }

    friend eckit::JSON& operator<<(eckit::JSON& s, const LogContext& o){
        o.json(s);
        return s;
    }

private:
    std::string context_;
};


// --------------------------------------------------------------------------------------------------------------------------------

class Metrics {

public: // methods
    
    Metrics(LogContext ctx) : context_(ctx) {
        start_ = std::string(eckit::TimeStamp("%Y-%m-%dT%H:%M:%SZ"));
    }
    
    ~Metrics() {}

    void report() {
        eckit::JSON j(eckit::Log::metrics(), false);
        j.startObject();
        j << "action" << action;
        j << "start_time" << start_;
        j << "end_time" << eckit::TimeStamp("%Y-%m-%dT%H:%M:%SZ");
        j << "count_requests" << nRequests;
        j << "count_tasks" << nTasks;
        j << "count_failed_tasks" << nFailedTasks;
        j << "elapsed_receive" << timeReceive;
        j << "elapsed_execute" << timeExecute;
        j << "elapsed_reply" << timeReply;
        j << "elapsed_total" << timer_.elapsed();
        j << "context" << context_;
        j.endObject();
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
    std::string start_;
};

} // namespace gribjump
