/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/filesystem/PathName.h"

namespace gribjump
{

std::vector<std::vector<std::pair<size_t, size_t>>> parseRangesFile(eckit::PathName fname){
    
    // plain text file with the following format:
    //      10-20, 30-40
    //      10-20, 60-70, 80-90
    //      ...etc
    // Each line contains a list of ranges, separated by commas.
    // One line per request.

    std::vector<std::vector<std::pair<size_t, size_t>>> allRanges;

    std::ifstream in(fname);
    if (in.bad()) {
        throw eckit::ReadError(fname);
    }

    std::string line;
    while (std::getline(in, line)) {
        std::vector<std::pair<size_t, size_t>> ranges;
        std::vector<std::string> rangeStrings = eckit::StringTools::split(",", line);
        for (const auto& rangeString : rangeStrings) {
            std::vector<std::string> range = eckit::StringTools::split("-", rangeString);
            ASSERT(range.size() == 2);
            ranges.push_back(std::make_pair(std::stoi(range[0]), std::stoi(range[1])));
        }
        allRanges.push_back(ranges);
    }

    return allRanges;
}

} // namespace gribjump
