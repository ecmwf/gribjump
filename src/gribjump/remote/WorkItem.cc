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
/// @author Tiago Quintino


#include "gribjump/remote/WorkItem.h"
#include "gribjump/remote/ExtractRequest.h"

namespace gribjump {

WorkItem::WorkItem(): task_(nullptr) {}
WorkItem::WorkItem(Task* task): task_(task) {}

void WorkItem::run(GribJump& gj) {
    if(!task_) return;
    task_->execute(gj);
}

void WorkItem::error(const std::string& s) {
    if(!task_) return;
    task_->notifyError(s);
}

}  // namespace gribjump
