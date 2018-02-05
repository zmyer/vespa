// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <cstddef>

namespace vespalib {
namespace metrics {

/**
 * Common implementation of an opaque handle identified only
 * by a (64-bit) integer.  Templated to avoid different concepts
 * sharing a superclass.
 **/
template <typename T>
class Handle {
private:
    const size_t _id;
public:
    explicit Handle(size_t id) : _id(id) {}
    size_t id() const { return _id; }
};

template <typename T>
bool
operator< (const Handle<T> &a, const Handle<T> &b) noexcept
{
    return a.id() < b.id();
}

template <typename T>
bool
operator> (const Handle<T> &a, const Handle<T> &b) noexcept
{
    return a.id() > b.id();
}

template <typename T>
bool
operator== (const Handle<T> &a, const Handle<T> &b) noexcept
{
    return a.id() == b.id();
}

template <typename T>
bool
operator!= (const Handle<T> &a, const Handle<T> &b) noexcept
{
    return a.id() != b.id();
}

} // namespace vespalib::metrics
} // namespace vespalib
