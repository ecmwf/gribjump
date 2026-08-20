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
#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/value/Value.h"

#include "dhskit/ListAggregation.h"

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
    else if (request.find("year") != request.end() && request.find("month") != request.end() && request.find("day") != request.end()) {
        // if date not present, but  year, month and day are present, construct a single date=YYYYMMDD from them.

        const std::string& month = request.at("month");
        
        std::string mm = month.substr(month.size() - 2); // last two characters of month string

        const std::string& yyyy = request.at("year");
        const std::string& dd = request.at("day");

        std::string date = yyyy + mm + dd;
        newRequest.erase("year");
        newRequest.erase("month");
        newRequest.erase("day");
        newRequest["date"] = date;
    }

    return newRequest; // return the modified request if no changes were made
}

/// @todo: incredibly hacky and quite expensive.
std::string marsRequestToKey(const std::map<std::string, std::string>& request_in) {
    std::map<std::string, std::string> request = keyMagic(request_in);
    std::string key = "retrieve,";
    std::string separator;
    for (const auto& [k, v] : request) {
        key += separator + k + "=" + v;
        separator = ",";
    }
    // return key;
    std::istringstream in(key);
    metkit::mars::MarsParser parser(in);
    metkit::mars::MarsExpansion expand(false, true);
    auto v = expand.expand(parser.parse());
    ASSERT(v.size() == 1);

    // return v[0].asString().substr(9); // drop the "retrieve," prefix
    // to string, our own way.
    std::string out="";
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

std::vector<eckit::URI> MarsListerClient::list(const std::vector<metkit::mars::MarsRequest> requests) {
    std::vector<eckit::URI> allURIs;

    for (const auto& request : requests) {

        eckit::net::TCPClient client;
        eckit::net::InstantTCPStream stream(client.connect(host_, port_));

        // Send header
        stream << protocolVersion_;
        stream << static_cast<uint16_t>(RequestType::LIST);

        // Send single request
        stream << request;

        // Receive errors
        size_t nErrors;
        stream >> nErrors;
        if (nErrors > 0) {
            std::stringstream ss;
            ss << "MarsListerClient received " << nErrors << " server-side error(s):" << std::endl;
            for (size_t i = 0; i < nErrors; i++) {
                std::string error;
                stream >> error;
                ss << error << std::endl;
            }
            throw eckit::RemoteException(ss.str(), Here());
        }

        // Receive JSON response
        std::string json;
        stream >> json;

        eckit::Log::info() << "MarsListerClient: received JSON: " << json << std::endl;

        // Parse JSON array of {path, offsets[], lengths[]}
        eckit::Value parsed = eckit::JSONParser::decodeString(json);

        for (size_t i = 0; i < parsed.size(); i++) {
            const eckit::Value& entry = parsed[i];
            std::string path = entry["path"];
            eckit::Value offsets = entry["offsets"];
            // eckit::Value lengths = entry["lengths"]; // TODO: use lengths when needed

            for (size_t j = 0; j < offsets.size(); j++) {
                long long offset = offsets[j];
                eckit::URI uri("file", eckit::PathName(path));
                uri.fragment(std::to_string(offset));
                allURIs.push_back(uri);
            }
        }

        eckit::Log::info() << "MarsListerClient: parsed " << parsed.size()
                           << " URI(s) for request" << std::endl;
    }

    return allURIs;
}

std::map<std::string, std::unordered_set<std::string>> MarsListerClient::axes(const std::string& request, int level) {
    NOTIMP;
}

// we expect the parsed to look like this:
// XXX : I couldnt help but notice the offsets are in reverse order...
//    [ // start list
//     { // start shape
//         // key mars:  value // object
//         "mars":{"class":"od","date":"2025-11-30","expver":"1","levtype":"pl","month":"202511","stream":"enfo","time":"00:00:00","type":"pf","year":"2025"}, // labels shape
    
//         // key files: value // list of (marsfs) filepaths
//         "files":["marsfs://mvr000/data/fc/marsdev_mvr000_p_pool1_a/prearc/hpss/marsodenfo/0001/pf/20251130/pl/0.20260505.180542.marsdev-mvr000.575525617664"],
    
//         // key fields: value // list of lists
//         "fields":[
//             // each list contains 4 elements:
//             // 0. dict representing a partial mars request.
//             // 1. the file_id
//             // 2. the offset 
//             // 3. the length
//             [{"step":"0","number":"1","levelist":"300","param":"130.128"},0,16402260,3280452], // inherits from shape.
//             [{"levelist":"400"},0,13121808,3280452], // Inherits from previous.
//             [{"levelist":"500"},0,9841356,3280452],
//             [{"levelist":"700"},0,6560904,3280452],
//             [{"levelist":"850"},0,3280452,3280452],
//             [{"levelist":"1000"},0,0,3280452]]
//     } // end shape
// ] // end list
//  void MarsListerClient::parseResult(const eckit::Value& parsed) {

//     // Start by accumulating a list of URIs.
//     ASSERT(parsed.size() == 1); // one mars request in, one object out
//     const eckit::Value& obj = parsed[0];
//     eckit::Value files = obj["files"];
//     eckit::Value fields = obj["fields"];
//     eckit::Value mars_request = obj["mars"];

//     std::vector<eckit::URI> uris;
//     for (size_t i = 0; i < fields.size(); i++) {
//         const eckit::Value& field = fields[i];
//         eckit::Value mars_request = field[0];
//         int file_id = field[1];
//         long long offset = field[2];
//         long long length = field[3]; // TODO: use length when needed
//         std::string path = files[file_id];
//         // eckit::URI uri("file", eckit::PathName(path));

//         eckit::PathName p(path);
//         eckit::URI uri("file", p.path());
//         uri.host(p.node());
//         uri.port(0);
//         uri.fragment(std::to_string(offset));
//         uris.push_back(uri);
//     }
// }

filemap_t MarsListerClient::fileMap(const metkit::mars::MarsRequest& marsRequest, const ExItemMap& reqToExtractionItem) {
    // debug, print everything we have
    if (LibGribJump::instance().debug()) {
        std::cout << "XXX:" << "MarsListerClient::fileMap -- marsRequest: " << marsRequest << std::endl;
        std::cout << "XXX:" << "MarsListerClient::fileMap -- reqToExtractionItem has " << reqToExtractionItem.size() << " items" << std::endl;
        for (const auto& [key, extractionItemPtr] : reqToExtractionItem) {
            std::cout << "XXX:" << "  key: " << key << std::endl;
            std::cout << ">> ";
            extractionItemPtr->debug_print();
            std::cout << std::endl;
        }
    }

    filemap_t filemap;

    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));

    // Send header
    stream << protocolVersion_;
    stream << static_cast<uint16_t>(RequestType::LIST);

    // Send single request
    stream << marsRequest;

    // Receive errors
    size_t nErrors;
    stream >> nErrors;
    if (nErrors > 0) {
        std::stringstream ss;
        ss << "MarsListerClient received " << nErrors << " server-side error(s):" << std::endl;
        for (size_t i = 0; i < nErrors; i++) {
            std::string error;
            stream >> error;
            ss << error << std::endl;
        }
        throw eckit::RemoteException(ss.str(), Here());
    }

    // Receive the ListAggregation object directly off the wire.
    dhskit::ListAggregation aggregation(stream);

    // Lazily walk the flattened fields, matching each to its ExtractionItem by canonical key.
    for (const auto& field : aggregation) {
        const std::string key = marsRequestToKey(field.request);
        std::cout << "Searching for key: " << key << std::endl;

        auto it = reqToExtractionItem.find(key);
        if (it == reqToExtractionItem.end()) {
            // Field is not one we requested; skip it.
            continue;
        }

        // Build the URI from the (marsfs) file path and offset.
        eckit::PathName p(field.file);
        eckit::URI uri("file", p.path());
        uri.host(p.node());
        uri.port(0);
        uri.fragment(std::to_string(static_cast<long long>(field.offset)));

        ExtractionItem* extractionItem = it->second.get();
        extractionItem->URI(uri);
        insertFileMap(filemap, uri.path(), extractionItem);
    }

    logFileMap(filemap);

    return filemap;
}

filemap_t MarsListerClient::fileMap_old(const metkit::mars::MarsRequest& marsRequest, const ExItemMap& reqToExtractionItem) {

    // std::vector<eckit::URI> uris = list({marsRequest});

    filemap_t filemap; // temporary until we implement this properly

    eckit::net::TCPClient client;
    eckit::net::InstantTCPStream stream(client.connect(host_, port_));

    // Send header
    stream << protocolVersion_;
    stream << static_cast<uint16_t>(RequestType::LIST);

    // Send single request
    stream << marsRequest;

    // Receive errors
    size_t nErrors;
    stream >> nErrors;
    if (nErrors > 0) {
        std::stringstream ss;
        ss << "MarsListerClient received " << nErrors << " server-side error(s):" << std::endl;
        for (size_t i = 0; i < nErrors; i++) {
            std::string error;
            stream >> error;
            ss << error << std::endl;
        }
        throw eckit::RemoteException(ss.str(), Here());
    }

    // Receive JSON response

    std::string jsonString;
    stream >> jsonString;
    eckit::Value decoded = eckit::JSONParser::decodeString(jsonString);
    eckit::Log::info() << "MarsListerClient::fileMap -- received JSON: " << jsonString << std::endl;


    // Start by accumulating a list of URIs.
    // @todo: I suspect you get multiple objects if you vary high up the tree e.g. DATE. Check and if so we will need to loop over objects here and then combine results.
    ASSERT(decoded.size() == 1); // one mars request in, one object out
    const eckit::Value& shape = decoded[0];

    const eckit::Value& files = shape["files"];
    const eckit::Value& fields = shape["fields"];
    const eckit::Value& shape_mars = shape["mars"]; // Only the first part of the full request.

    // Split marsfs paths into node and path components
    
    using marsfs_path = std::tuple<std::string, std::string>; // <node, path>
    std::vector<marsfs_path> marsfs_paths;
    marsfs_paths.reserve(files.size());

    for (size_t i = 0; i < files.size(); i++) {
        const std::string& file = files[i];
        eckit::PathName p(file);
        marsfs_paths.emplace_back(p.node(), p.path());
    }

    // Combine with offsets to get full URIs

    std::vector<eckit::URI> uris;
    for (size_t i = 0; i < fields.size(); i++) {
        const eckit::Value& field = fields[i];
        const eckit::Value& mars_request = field[0]; // the 
        int file_id = field[1];
        long long offset = field[2];
        long long length = field[3]; // TODO: use length when needed

        const std::string& node = std::get<0>(marsfs_paths[file_id]);
        const std::string& path = std::get<1>(marsfs_paths[file_id]);

        eckit::URI uri("file", path);
        uri.host(node);
        uri.port(0);
        uri.fragment(std::to_string(offset));
        uris.push_back(uri);

    }

    // XXX FOR DEMO: We will just assign URIs to the next available ExtractionItem in the map..
    // Maybe it is time to move to a more tree-like structure.
    size_t i = 0;
    for (const auto& [key, extractionItemPtr] : reqToExtractionItem) {
        if (i >= uris.size()) {
            throw eckit::SeriousBug("MarsListerClient::fileMap -- Not enough URIs parsed from response to assign to all ExtractionItems");
        }
        ExtractionItem* extractionItem = extractionItemPtr.get();
        eckit::URI uri = uris[i++];

        extractionItem->URI(uri);
        insertFileMap(filemap, uri.path(), extractionItem);
    }

    return filemap;
}

}  // namespace gribjump
