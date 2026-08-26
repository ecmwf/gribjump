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
            std::string path          = entry["path"];
            eckit::Value offsets      = entry["offsets"];
            // eckit::Value lengths = entry["lengths"]; // TODO: use lengths when needed

            for (size_t j = 0; j < offsets.size(); j++) {
                long long offset = offsets[j];
                eckit::URI uri("file", eckit::PathName(path));
                uri.fragment(std::to_string(offset));
                allURIs.push_back(uri);
            }
        }

        eckit::Log::info() << "MarsListerClient: parsed " << parsed.size() << " URI(s) for request" << std::endl;
    }

    return allURIs;
}

std::map<std::string, std::unordered_set<std::string>> MarsListerClient::axes(const std::string& request, int level) {
    NOTIMP;
}

filemap_t MarsListerClient::fileMap(const metkit::mars::MarsRequest& marsRequest,
                                    const ExItemMap& reqToExtractionItem) {
    // debug, print everything we have
    if (LibGribJump::instance().debug()) {
        std::cout << "XXX:" << "MarsListerClient::fileMap -- marsRequest: " << marsRequest << std::endl;
        std::cout << "XXX:" << "MarsListerClient::fileMap -- reqToExtractionItem has " << reqToExtractionItem.size()
                  << " items" << std::endl;
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

    // Construct the MarsExpansion once and reuse it
    metkit::mars::MarsExpansion expand(false, true);
    for (const auto& field : aggregation) {
        const std::string key = marsRequestToKey(field.request, expand);
        LOG_DEBUG_LIB(LibGribJump) << "Searching for key: " << key << std::endl;

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

}  // namespace gribjump
