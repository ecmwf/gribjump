/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/utils/Regex.h"

#include "fdb5/api/FDB.h"
#include "gribjump/info/InfoAggregator.h"

namespace gribjump {

// For when GribJump is a plugin to FDB
class FDBPlugin {
public:

    static FDBPlugin& instance();
    void addFDB(fdb5::FDB& fdb);

private:

    FDBPlugin();

    void parseConfig(eckit::PathName path);
    bool matches(const fdb5::Key& key) const;

private:
    bool configParsed_ = false;
    // std::vector<std::shared_ptr<InfoAggregator>> aggregators_;
    std::vector<std::shared_ptr<SerialAggregator>> aggregators_;
    std::map<std::string, eckit::Regex> selectDict_;

};

} // namespace fdb5