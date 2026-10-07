/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "gribjump/MarsListerClient.h"
#include <algorithm>
#include <sstream>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/log/Log.h"

#include "gribjump/gribjump_config.h"
#ifdef GRIBJUMP_HAVE_DHSKIT
#include "dhskit/ListAggregation.h"
#endif

#include "gribjump/Types.h"
#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"

namespace gribjump {

namespace {

/// Build the lookup key used to match a listed field against an ExtractionItem.
/// The key must have its mars keys sorted alphabetically (to match the canonical key used in
/// reqToExtractionItem).

/// @todo: It ought to be metkit or dhskit's job to be able to tell me if different
/// representations of the same request are equivalent. (e.g. date=2025-11-30 vs year=2025, month=202511, day=30).
/// I'll put some hacky logic here for now, please don't let it stay.......
std::map<std::string, std::string> keyMagic(const std::map<std::string, std::string>& request) {

    std::map<std::string, std::string> newRequest = request;

    // Magic 1: date.
    // if date is present, ignore year and month as they are aliases.
    if (request.find("date") != request.end()) {
        newRequest.erase("year");
        newRequest.erase("month");
        newRequest.erase("day");
    }
    else if (request.find("year") != request.end() && request.find("month") != request.end() &&
             request.find("day") != request.end()) {
        // if date not present, but  year, month and day are present, construct a single date=YYYYMMDD from them.

        const std::string& month = request.at("month");

        std::string mm = month.substr(month.size() - 2);  // last two characters of month string

        const std::string& yyyy = request.at("year");
        const std::string& dd   = request.at("day");

        std::string date = yyyy + mm + dd;
        newRequest.erase("year");
        newRequest.erase("month");
        newRequest.erase("day");
        newRequest["date"] = date;
    }

    return newRequest;  // return the modified request if no changes were made
}

/// @todo: incredibly hacky and quite expensive.
std::string marsRequestToKey(const std::map<std::string, std::string>& request_in,
                             metkit::mars::MarsExpansion& expand) {
    std::map<std::string, std::string> request = keyMagic(request_in);
    std::string key                            = "retrieve,";
    std::string separator;
    for (const auto& [k, v] : request) {
        key += separator + k + "=" + v;
        separator = ",";
    }
    // return key;
    std::istringstream in(key);
    metkit::mars::MarsParser parser(in);
    auto v = expand.expand(parser.parse());
    ASSERT(v.size() == 1);

    // return v[0].asString().substr(9); // drop the "retrieve," prefix
    // to string, our own way.
    std::string out = "";
    // iterate over keys
    std::vector<std::string> keys;
    v[0].getParams(keys);
    std::sort(keys.begin(), keys.end());
    for (const auto& k : keys) {
        if (out != "") {
            out += ",";
        }
        out += k + "=" + v[0].values(k)[0];
    }
    return out;
}

}  // namespace


MarsListerClient::MarsListerClient(const std::string& host, int port) : host_(host), port_(port) {
    eckit::Log::info() << "MarsListerClient targeting " << host_ << ":" << port_ << std::endl;
}

MarsListerClient::~MarsListerClient() {}

std::vector<ListResult> MarsListerClient::list(const metkit::mars::MarsRequest& request) {
#ifdef GRIBJUMP_HAVE_DHSKIT
    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));
    stream << protocolVersion_;
    stream << static_cast<uint16_t>(RequestType::LIST);
    stream << request;

    size_t nErrors;
    stream >> nErrors;
    if (nErrors > 0) {
        std::stringstream ss;
        ss << "MarsListerClient received " << nErrors << " server-side error(s):" << std::endl;
        for (size_t i = 0; i < nErrors; ++i) {
            std::string error;
            stream >> error;
            ss << error << std::endl;
        }
        throw eckit::RemoteException(ss.str(), Here());
    }

    // Protocol v1 sends a binary aggregation, not JSON. dhskit reconstructs
    // inherited metadata across fields and resets it at each shape boundary.
    dhskit::ListAggregation aggregation(stream);
    std::vector<ListResult> results;
    for (const auto& field : aggregation) {
        eckit::PathName path(field.file);
        eckit::URI uri("file", path.path());
        uri.host(path.node());
        uri.port(0);
        uri.fragment(std::to_string(static_cast<long long>(field.offset)));
        uri.query("length", std::to_string(static_cast<long long>(field.length)));
        results.emplace_back(std::move(uri), field.request, field.length);
    }
    return results;
#else
    throw eckit::UserError("MARS listing requires gribjump to be built with dhskit", Here());
#endif
}

std::map<std::string, std::unordered_set<std::string>> MarsListerClient::axes(const std::string& request, int level) {
    NOTIMP;
}

filemap_t MarsListerClient::fileMap(const metkit::mars::MarsRequest& marsRequest,
                                    const ExItemMap& reqToExtractionItem) {
    filemap_t filemap;
    const auto fields = list(marsRequest);
    // Normalisation is needed only for matching extraction requests. Public
    // listing preserves the metadata returned by MARS.
    metkit::mars::MarsExpansion expand(false, true);
    for (const auto& field : fields) {
        const std::string key = marsRequestToKey(field.metadata(), expand);
        LOG_DEBUG_LIB(LibGribJump) << "Searching for key: " << key << std::endl;

        auto it = reqToExtractionItem.find(key);
        if (it == reqToExtractionItem.end()) {
            // Field is not one we requested; skip it.
            continue;
        }

        const eckit::URI& uri = field.uri();

        ExtractionItem* extractionItem = it->second.get();
        extractionItem->URI(uri);
        insertFileMap(filemap, uri.path(), extractionItem);
    }

    logFileMap(filemap);

    return filemap;
}

}  // namespace gribjump
