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

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionItem.h"

namespace gribjump {


/// @todo Do we want a Lister factory that builds depending on config?

class Lister {
public:

    virtual std::vector<eckit::URI> list(const std::vector<metkit::mars::MarsRequest> requests) = 0; // <-- May not want to use mars request?
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request) = 0 ;

    void lock() { mutex_.lock(); locked_ = true; }
    void unlock() { mutex_.unlock(); locked_ = false; }

protected:

    Lister();
    ~Lister();
    
private:
    std::mutex mutex_;
    bool locked_ = false;
};

//  ------------------------------------------------------------------
using reqToXRR_t = std::map<std::string, ExtractionItem*>;
// We explicitly want this map to be randomly sorted.
using filemap_t = std::unordered_map<std::string, std::vector<ExtractionItem*>>; // String is filepath, eckit::PathName is not hashable?

class FDBLister : public Lister {
public:

    static FDBLister& instance();

    virtual std::vector<eckit::URI> list(const std::vector<metkit::mars::MarsRequest> requests) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const std::string& request) override;
    virtual std::map<std::string, std::unordered_set<std::string> > axes(const fdb5::FDBToolRequest& request);
    
    filemap_t fileMap(const metkit::mars::MarsRequest& unionRequest, const reqToXRR_t& reqToXRR); // Used during extraction

    std::map< eckit::PathName, eckit::OffsetList > filesOffsets(std::vector<metkit::mars::MarsRequest> requests); // Used during scan

    
private:
    FDBLister();
    ~FDBLister();

private:
    fdb5::FDB fdb_;
};

// ------------------------------------------------------------------

} // namespace gribjump
