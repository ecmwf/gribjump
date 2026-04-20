/**
 * @file simple_extract.cc
 * @brief Minimal example: extract data subsets from GRIB fields using GribJump.
 *
 * Build:
 *   g++ -std=c++17 -I/path/to/gribjump/src -I/path/to/eckit/include \
 *       -I/path/to/metkit/include simple_extract.cc \
 *       -lgribjump -leckit -lmetkit -o simple_extract
 *
 * Usage:
 *   export GRIBJUMP_CONFIG_FILE=/path/to/config.yaml
 *   ./simple_extract
 */
#include <iostream>
#include <vector>

#include "gribjump/api/GribJump.h"

int main() {
    gribjump::GribJump gj;

    // Define extraction requests: MARS request string + ranges
    std::string marsReq =
        "class=od,type=fc,stream=oper,expver=0001,"
        "levtype=sfc,param=151130,date=20230710,"
        "time=1200,step=1,domain=g";

    std::vector<gribjump::Range> ranges = {{0, 10}, {100, 110}};

    std::vector<gribjump::ExtractionRequest> requests;
    requests.emplace_back(marsReq, ranges);

    // Extract
    auto it = gj.extract(requests);

    // Iterate over results
    while (it.hasNext()) {
        auto result = it.next();
        std::cout << "Extracted " << result->nrange() << " range(s)\n";
        for (size_t r = 0; r < result->nrange(); ++r) {
            std::cout << "  Range " << r << ": " << result->nvalues(r) << " values\n";
            for (double v : result->values()[r]) {
                std::cout << "    " << v << "\n";
            }
        }
    }

    return 0;
}
