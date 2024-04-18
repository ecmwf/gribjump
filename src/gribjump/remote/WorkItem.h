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

#pragma once

#include "gribjump/GribJump.h"

namespace gribjump {

class Task;

class WorkItem {
public:

    WorkItem();
    WorkItem(Task* task);

    void run(GribJump& gj);

    void error(const std::string& s);

private:
    Task* task_;
};

}  // namespace gribjump
