/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include <unordered_map>

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionItem.h"

namespace gribjump {


class Lister {
public:

    /// Returns the configured Lister implementation (FDBLister or MarsListerClient).
    /// Determined by config key "lister" ("fdb" by default, or "marslister").
    static Lister& instance();

    virtual std::vector<eckit::URI> list(
        const std::vector<metkit::mars::MarsRequest> requests) = 0;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request, int level) = 0;

    virtual filemap_t fileMap(const metkit::mars::MarsRequest& unionRequest,
                              const ExItemMap& reqToExtractionItem) = 0;

    filemap_t fileMap(const ExItemMap& reqToExtractionItem);

    virtual ~Lister();

protected:

    Lister();

    static void insertFileMap(filemap_t& filemap, const eckit::PathName& fname, ExtractionItem* item);
    static void logFileMap(const filemap_t& filemap);
};

//  ------------------------------------------------------------------

class FDBLister : public Lister {
public:

    static FDBLister& instance();

    using Lister::fileMap;

    virtual std::vector<eckit::URI> list(const std::vector<metkit::mars::MarsRequest> requests) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request,
                                                                         int level) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const fdb5::FDBToolRequest& request,
                                                                         int level);

    filemap_t fileMap(const metkit::mars::MarsRequest& unionRequest,
                      const ExItemMap& reqToXRR) override;

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
