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

#pragma once

#include <list>
#include <unordered_map>
#include "eckit/exception/Exceptions.h"

namespace gribjump {

// Note: not a thread safe container, use an external lock if needed
template <typename K, typename V>
class LRUCache {
public:

    LRUCache(size_t capacity) : capacity_(capacity) {}

    void put(const K& key, const V& value) {
        if (map_.find(key) == map_.end()) {
            if (list_.size() == capacity_) {
                auto last = list_.back();
                list_.pop_back();
                map_.erase(last);
            }
            list_.push_front(key);
        }
        else {
            list_.remove(key);
            list_.push_front(key);
        }
        map_[key] = value;
    }


    V& get(const K& key) {
        if (map_.find(key) == map_.end()) {
            throw eckit::BadValue("Key does not exist");
        }
        list_.remove(key);
        list_.push_front(key);
        return map_[key];
    }

    bool exists(const K& key) { return map_.find(key) != map_.end(); }

    typename std::unordered_map<K, V>::const_iterator begin() const { return map_.begin(); }

    typename std::unordered_map<K, V>::const_iterator end() const { return map_.end(); }

    void clear() {
        list_.clear();
        map_.clear();
    }

private:

    size_t capacity_;
    std::list<K> list_;
    std::unordered_map<K, V> map_;
};

}  // namespace gribjump
