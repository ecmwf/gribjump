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

#pragma once

#include "eckit/log/Statistics.h"

namespace gribjump {
class Stats : public eckit::Statistics {
public:

    Stats();

    void addExtract(eckit::Timer& timer);
    void addInfo(eckit::Timer& timer);
    void addInspect(eckit::Timer& timer);

    void report(std::ostream& out, const char* prefix) const;

private:

    void reportTimeStats(std::ostream& out, const std::string& title, size_t count, double sum_times,
                         double sum_times_squared, double min, double max, const char* indent) const;

private:

    size_t countExtract_;
    size_t countInfos_;
    size_t countInspects_;

    double minExtract_;
    double maxExtract_;
    double minInfos_;
    double maxInfos_;
    double minInspects_;
    double maxInspects_;

    double elapsedExtract_;
    double elapsedInfos_;
    double elaspedInspects_;

    double elapsedExtractSquared_;
    double elapsedInfosSquared_;
    double elaspedInspectsSquared_;
};
}  // namespace gribjump
