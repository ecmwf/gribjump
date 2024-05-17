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

#include <fstream>
#include <memory>

#include "eckit/utils/StringTools.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"

#include "metkit/codes/GribHandle.h"

// #include "gribjump/FDBService.h"
#include "gribjump/Lister.h"
#include "gribjump/tools/EccodesExtract.h"


namespace gribjump {

std::vector<std::vector<std::vector<double>>> eccodesExtract(metkit::mars::MarsRequest request, std::vector<Range> ranges){  

    std::map< eckit::PathName, eckit::OffsetList > map = FDBLister::instance().filesOffsets({request});
   
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

std::vector<double> eccodesExtract(eckit::PathName path) {
    
        std::unique_ptr<eckit::DataHandle> dh(path.fileHandle());
        dh->openForRead();
    
        const metkit::grib::GribHandle handle(*dh, 0);
    
        size_t count;
        std::unique_ptr<double[]> data(handle.getDataValues(count));
    
        std::vector<double> ecvalues;
        for (size_t k = 0; k < count; k++) {
            ecvalues.push_back(data[k]);
        }
    
        return ecvalues;
}

} // namespace gribjump