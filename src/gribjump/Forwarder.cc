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

#include "gribjump/Forwarder.h"
#include <atomic>
#include <numeric>
#include <unordered_map>
#include "eckit/filesystem/URI.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {

Forwarder::Forwarder() {}

Forwarder::~Forwarder() {}

TaskOutcome<size_t> Forwarder::scan(const std::vector<eckit::URI>& uris) {

    ASSERT(uris.size() > 0);

    // scanmap_t: filename -> offsets
    std::unordered_map<eckit::net::Endpoint, scanmap_t> serverfilemaps;

    // Match servers with files
    for (auto& uri : uris) {
        eckit::net::Endpoint server = this->serverForURI(uri);
        eckit::Offset offset        = URIHelper::offset(uri);
        std::string fname           = uri.path();
        serverfilemaps[server][fname].push_back(offset);
    }

    TaskGroup taskGroup;
    std::atomic<size_t> nFields(0);
    for (auto& [endpoint, scanmap] : serverfilemaps) {
        taskGroup.enqueueTask<ForwardScanTask>(endpoint, scanmap, nFields);
    }
    taskGroup.waitForTasks();

    return {nFields, taskGroup.report()};
}

TaskReport Forwarder::extract(filemap_t& filemap) {
    std::unordered_map<eckit::net::Endpoint, filemap_t> serverfilemaps = serverFileMap(filemap);

    TaskGroup taskGroup;
    for (auto& [endpoint, subfilemap] : serverfilemaps) {
        taskGroup.enqueueTask<ForwardExtractionTask>(endpoint, subfilemap);
    }
    taskGroup.waitForTasks();

    return taskGroup.report();
}


const eckit::net::Endpoint& Forwarder::serverForURI(const eckit::URI& uri) const {
    const Config::ServerMap& servermap = LibGribJump::instance().config().serverMap();

    eckit::net::Endpoint fdbEndpoint(uri.host(), uri.port());

    if (servermap.find(fdbEndpoint) == servermap.end()) {
        throw eckit::SeriousBug("No gribjump endpoint found for fdb endpoint: " + std::string(fdbEndpoint));
    }

    return servermap.at(fdbEndpoint);
}

// Splits a filemap into subfilemaps, each to be handled by a seperate remote server
std::unordered_map<eckit::net::Endpoint, filemap_t> Forwarder::serverFileMap(filemap_t& filemap) {

    Config::ServerMap servermap = LibGribJump::instance().config().serverMap();

    // Match servers with files
    std::unordered_map<eckit::net::Endpoint, std::vector<std::string>> serverfiles;
    for (auto& [fname, extractionItems] : filemap) {
        eckit::URI uri              = extractionItems[0]->URI();  // URIs are already grouped by file
        eckit::net::Endpoint server = serverForURI(uri);
        serverfiles[server].push_back(fname);
    }

    // make subfilemaps for each server
    std::unordered_map<eckit::net::Endpoint, filemap_t> serverfilemaps;

    for (auto& [server, files] : serverfiles) {
        filemap_t subfilemap;
        for (auto& fname : files) {
            subfilemap[fname] = filemap[fname];
        }
        serverfilemaps[server] = subfilemap;
    }

    return serverfilemaps;
}


}  // namespace gribjump