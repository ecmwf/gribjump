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

#include "eckit/exception/Exceptions.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Timer.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/runtime/Metrics.h"
#include "eckit/serialisation/Stream.h"

namespace gribjump {

// This should be a dumb object, that must be representable as a json
class LogContext {
public:

    LogContext(std::string s = "{}") : context_(s) {
        // ensure we can parse the context as a json.
        try {
            auto val = eckit::JSONParser::decodeString(context_);
        }
        catch (eckit::Exception& e) {
            throw eckit::BadValue("Could not parse context string as json. Context: " + context_);
        }
    }

    explicit LogContext(eckit::Stream& s) { s >> context_; }

    ~LogContext() {}

private:

    void encode(eckit::Stream& s) const { s << context_; }

    friend eckit::Stream& operator<<(eckit::Stream& s, const LogContext& o) {
        o.encode(s);
        return s;
    }

    void json(eckit::JSON& s) const { s << context_; }

    friend eckit::JSON& operator<<(eckit::JSON& s, const LogContext& o) {
        o.json(s);
        return s;
    }


    friend std::ostream& operator<<(std::ostream& os, const LogContext& o) {
        os << o.context_;
        return os;
    }

private:

    std::string context_;
};

// --------------------------------------------------------------------------------------------------------------------------------
///@todo: Investigate using eckit::metrics and MetricsCollector
//        it does not look usable in a multi-threaded context.
class Metrics {

public:  // methods

    Metrics();
    ~Metrics();

    void add(const std::string& name, const eckit::Value& value);

    void addContext(const LogContext& context) { context_ = context; }

    void report();

public:  // members

    eckit::ValueMap values_;

private:  // members

    LogContext context_;
    time_t created_;

    eckit::Timer timer_;
};

// --------------------------------------------------------------------------------------------------------------------------------
// Wrapper around Metrics to treat them as thread-local
class MetricsManager {

public:  // methods

    static MetricsManager& instance();

    void set(const std::string& name, const eckit::Value& value);
    // void setContext(const LogContext& context);
    void report();

private:

    MetricsManager();
    ~MetricsManager();

    Metrics& metrics();
};

// --------------------------------------------------------------------------------------------------------------------------------

// Context also needs to be thread-local
class ContextManager {
public:

    static ContextManager& instance();

    LogContext& context();

    void set(const LogContext& context);

private:

    ContextManager();
    ~ContextManager();

private:

    static thread_local LogContext context_;
};
}  // namespace gribjump
