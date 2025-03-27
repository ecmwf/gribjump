/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include <unordered_map>

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionItem.h"

namespace gribjump {


/// @todo Do we want a Lister factory that builds depending on config?

class Lister {
public:

    virtual std::vector<eckit::URI> list(
        const std::vector<metkit::mars::MarsRequest> requests) = 0;  // <-- May not want to use mars request?
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level) = 0;

protected:

    Lister();
    ~Lister();
};

//  ------------------------------------------------------------------

class FDBLister : public Lister {
public:

    static FDBLister& instance();

    virtual std::vector<eckit::URI> list(const std::vector<metkit::mars::MarsRequest> requests) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request,
                                                                         int level) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const fdb5::FDBToolRequest& request,
                                                                         int level);

    filemap_t fileMap(const metkit::mars::MarsRequest& unionRequest,
                      const ExItemMap& reqToXRR);  // Used during extraction

    std::map<eckit::PathName, eckit::OffsetList> filesOffsets(
        const std::vector<metkit::mars::MarsRequest>& requests);  // Used during scan
    std::map<eckit::PathName, eckit::OffsetList> filesOffsets(const std::vector<eckit::URI>& uris);

    std::vector<eckit::URI> URIs(const std::vector<metkit::mars::MarsRequest>& requests);

private:

    FDBLister();
    ~FDBLister();

private:

    bool allowMissing_;
};

// ------------------------------------------------------------------

}  // namespace gribjump
