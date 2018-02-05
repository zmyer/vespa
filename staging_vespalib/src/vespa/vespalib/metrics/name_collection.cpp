// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include "name_collection.h"
#include <assert.h>

namespace vespalib {
namespace metrics {

using Guard = std::lock_guard<std::mutex>;

const vespalib::string &
NameCollection::lookup(size_t id) const
{
    Guard guard(_lock);
    assert(id < _names_by_id.size());
    return _names_by_id[id]->first;
}

size_t
NameCollection::resolve(const vespalib::string& name)
{
    Guard guard(_lock);
    size_t nextId = _names_by_id.size();
    auto iter_check = _names.emplace(name, nextId);
    if (iter_check.second) {
        _names_by_id.push_back(iter_check.first);
    }
    return iter_check.first->second;
}

size_t
NameCollection::size() const
{
    Guard guard(_lock);
    return _names_by_id.size();
}

} // namespace vespalib::metrics
} // namespace vespalib
