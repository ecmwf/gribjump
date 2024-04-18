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

#include "gribjump/Stats.h"

#include <cmath>
#include <cstring>
#include <iomanip>

namespace gribjump {

Stats::Stats() :
    countExtract_(0),
    countInfos_(0),
    countInspects_(0),
    minExtract_(0),
    maxExtract_(0),
    minInfos_(0),
    maxInfos_(0),
    minInspects_(0),
    maxInspects_(0),
    elapsedExtract_(0),
    elapsedInfos_(0),
    elaspedInspects_(0),
    elapsedExtractSquared_(0),
    elapsedInfosSquared_(0),
    elaspedInspectsSquared_(0) {}

void Stats::addExtract(eckit::Timer& timer) {
    countExtract_++;
    double elapsed = timer.elapsed();

    elapsedExtract_ += elapsed;
    elapsedExtractSquared_ += elapsed * elapsed;

    if (countExtract_ == 1) {
        minExtract_ = elapsed;
    }

    minExtract_ = std::min(minExtract_, elapsed);
    maxExtract_ = std::max(maxExtract_, elapsed);
}

void Stats::addInfo(eckit::Timer& timer) {
    countInfos_++;
    double elapsed = timer.elapsed();

    elapsedInfos_ += elapsed;
    elapsedInfosSquared_ += elapsed * elapsed;

    if (countInfos_ == 1) {
        minInfos_ = elapsed;
    }

    minInfos_ = std::min(minInfos_, elapsed);
    maxInfos_ = std::max(maxInfos_, elapsed);
}

void Stats::addInspect(eckit::Timer& timer) {
    countInspects_++;
    double elapsed = timer.elapsed();

    elaspedInspects_ += elapsed;
    elaspedInspectsSquared_ += elapsed * elapsed;

    if (countInspects_ == 1) {
        minInspects_ = elapsed;
    }

    minInspects_ = std::min(minInspects_, elapsed);
    maxInspects_ = std::max(maxInspects_, elapsed);
}


void Stats::reportTimeStats(std::ostream& out, const std::string& title, size_t count, double sum_times,
                                 double sum_times_squared, double min, double max, const char* indent) const {
    if (count) {
        const size_t WIDTH = 34;

        double average      = 0;
        double stdDeviation = 0;
        if (count != 0) {
            average      = sum_times / count;
            stdDeviation = std::sqrt(std::max((count * sum_times_squared) - (sum_times * sum_times), 0.0)) / count;
        }

        out << indent << title << std::setw(WIDTH - title.length()) 
            << std::scientific << std::setprecision(3)
            << " total: " << std::setw(10) << sum_times << " s"
            << ", mean: " << std::setw(10) << average << " s"
            << ", std: " << std::setw(10) << stdDeviation << " s"
            << ", min: " << std::setw(10) << min << " s"
            << ", max: " << std::setw(10) << max << " s"
            << ". (count: " << std::setw(10) << count << ")" << std::endl;
    }
}

void Stats::report(std::ostream& out, const char* prefix) const {
    reportTimeStats(out, "Inspect time ", countInspects_, elaspedInspects_, elaspedInspectsSquared_, minInspects_, maxInspects_, prefix);
    reportTimeStats(out, "Info time", countInfos_, elapsedInfos_, elapsedInfosSquared_, minInfos_, maxInfos_, prefix);
    reportTimeStats(out, "Extraction time", countExtract_, elapsedExtract_, elapsedExtractSquared_, minExtract_, maxExtract_, prefix);
}

} // namespace gribjump