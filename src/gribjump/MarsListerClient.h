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

#pragma once

#include <string>

#include "eckit/net/Endpoint.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"

#include "gribjump/Lister.h"

namespace gribjump {

class MarsListerClient : public Lister {
public:

    MarsListerClient(const std::string& host, int port);

    ~MarsListerClient();

    std::vector<eckit::URI> list(const std::vector<metkit::mars::MarsRequest> requests) override;

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) override;

    filemap_t fileMap(const metkit::mars::MarsRequest& unionRequest,
                      const ExItemMap& reqToExtractionItem) override;

private:

    static constexpr uint16_t protocolVersion_ = 1;

    enum class RequestType : uint16_t {
        LIST = 0
    };

    std::string host_;
    int port_;
};

}  // namespace gribjump
