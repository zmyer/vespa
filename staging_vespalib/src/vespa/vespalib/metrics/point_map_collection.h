// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <mutex>
#include <map>
#include <vector>
#include "point_map.h"

namespace vespalib {
namespace metrics {

// internal
class PointMapCollection {
private:
    using PointMapMap = std::map<PointMap, size_t>;

    mutable std::mutex _lock;
    PointMapMap _map;
    std::vector<PointMapMap::const_iterator> _vec;
public:
    const PointMap &lookup(size_t id);
    size_t resolve(PointMap map);
    size_t size() const;

    PointMapCollection() = default;
    ~PointMapCollection() {}
};

} // namespace vespalib::metrics
} // namespace vespalib
