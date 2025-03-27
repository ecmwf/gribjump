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

#include "gribjump/jumper/Jumper.h"

namespace gribjump {

class JumperBuilderBase {
    std::string name_;

public:

    JumperBuilderBase(const std::string& name);
    virtual ~JumperBuilderBase();

    virtual Jumper* make() const = 0;
};

template <class T>
class JumperBuilder : public JumperBuilderBase {

    Jumper* make() const override { return new T(); }

public:

    JumperBuilder(const std::string& name) : JumperBuilderBase(name) {}
    ~JumperBuilder() override = default;
};

class JumperFactory {
public:

    static JumperFactory& instance();

    Jumper* build(const JumpInfo& info);
    Jumper* build(const std::string packingType);

    void enregister(const std::string& name, JumperBuilderBase* builder);
    void deregister(const std::string& name);

    JumperBuilderBase* get(const std::string& name);

private:

    JumperFactory();
    ~JumperFactory();

private:

    std::map<std::string, JumperBuilderBase*> builders_;
    std::mutex mutex_;
};

}  // namespace gribjump
