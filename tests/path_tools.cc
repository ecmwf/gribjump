#include "fdb5/api/FDB.h"
#include "gribjump/Lister.cc"
#include "metkit/mars/MarsParser.h"

std::string get_path_name_from_mars_req(std::string mars_str, fdb5::FDB& fdb) {
    std::istringstream istream(mars_str);
    metkit::mars::MarsParser parser(istream);
    std::vector<metkit::mars::MarsParsedRequest> unionRequests = parser.parse();
    fdb5::FDBToolRequest fdbreq(unionRequests[0]);

    // std::cout << "WHAT's THE mars req HERE AT TOP" << std::endl;
    // std::cout << unionRequests[0] << std::endl;

    // fdb5::FDB fdb;
    auto listIter = fdb.list(fdbreq, true);

    std::cout << "fdb_req" << fdbreq << std::endl;
    // std::cout << "what's the fdb iter" << listIter << std::endl;

    fdb5::ListElement elem;
    // std::cout << "AND HERE??" << listIter.next(elem) << std::endl;
    std::vector<eckit::URI> uris = {};
    while (listIter.next(elem)) {
        std::cout << "EVER HERE??" << std::endl;
        std::string key = gribjump::fdbkeyToStr(elem.combinedKey());

        // If key not in map, not related to the request
        // if (reqToExtractionItem.find(key) == reqToExtractionItem.end())
        //     continue;

        // Set the URI in the ExtractionItem
        eckit::URI uri = elem.location().fullUri();
        std::cout << "WHAT's THE URI HERE AT TOP" << uri << std::endl;
        uris.push_back(uri);
    }
    // return uris[0].path();
    return uris.at(0).path();
}