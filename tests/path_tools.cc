#include "fdb5/api/FDB.h"
#include "gribjump/Lister.cc"
#include "metkit/mars/MarsParser.h"

std::string get_path_name_from_mars_req(std::string mars_str, fdb5::FDB& fdb) {
    std::istringstream istream(mars_str);
    metkit::mars::MarsParser parser(istream);
    std::vector<metkit::mars::MarsParsedRequest> unionRequests = parser.parse();
    fdb5::FDBToolRequest fdbreq(unionRequests[0]);

    auto listIter = fdb.list(fdbreq, true);

    fdb5::ListElement elem;
    std::vector<eckit::URI> uris = {};
    while (listIter.next(elem)) {
        eckit::URI uri = elem.location().fullUri();
        uris.push_back(uri);
    }
    return uris.at(0).path();
}