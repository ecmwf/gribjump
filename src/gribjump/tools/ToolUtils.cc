/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>

#include "eckit/utils/StringTools.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/codes/GribHandle.h"

#include "gribjump/tools/ToolUtils.h"
#include "gribjump/FDBService.h"

namespace gribjump
{

std::vector<std::vector<Range>> parseRangesFile(eckit::PathName fname){
    
    // plain text file with the following format:
    //      10-20, 30-40
    //      10-20, 60-70, 80-90
    //      ...etc
    // Each line contains a list of ranges, separated by commas.
    // One line per request.

    std::vector<std::vector<Range>> allRanges;

    std::ifstream in(fname);
    if (in.bad()) {
        throw eckit::ReadError(fname);
    }

    std::string line;
    while (std::getline(in, line)) {
        std::vector<Range> ranges;
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

std::vector<std::vector<std::vector<double>>> eccodesExtract(metkit::mars::MarsRequest request, std::vector<Range> ranges){  

    std::map< eckit::PathName, eckit::OffsetList > map = FDBService::instance().filesOffsets({request});
   
    std::vector<std::vector<std::vector<double>>> results;
    for (const auto& entry : map) {
        const eckit::PathName path = entry.first;
        const eckit::OffsetList offsets = entry.second;
        std::unique_ptr<eckit::DataHandle> dh(path.fileHandle());
        dh->openForRead();

        std::vector<std::vector<std::vector<double>>> resultsFile = eccodesExtract(path, offsets, ranges);
        results.insert(results.end(), resultsFile.begin(), resultsFile.end());

    }

    return results;
}

std::vector<std::vector<std::vector<double>>> eccodesExtract(eckit::PathName path, eckit::OffsetList offsets, std::vector<Range> ranges){ 

    std::vector<std::vector<std::vector<double>>> results;
    
    std::unique_ptr<eckit::DataHandle> dh(path.fileHandle());
    dh->openForRead();

    for (int j = 0; j < offsets.size(); j++) {

        const eckit::Offset offset = offsets[j];
        const metkit::grib::GribHandle handle(*dh, offset);

        size_t count;
        std::unique_ptr<double[]> data(handle.getDataValues(count));

        std::vector<std::vector<double>> ecvalues;
        for (const auto& range : ranges) {
            std::vector<double> rangeValues;
            for (size_t k = range.first; k < range.second; k++) {
                rangeValues.push_back(data[k]);
            }
            ecvalues.push_back(rangeValues);
        }

        results.push_back(ecvalues);
    }

    return results;
}

} // namespace gribjump
