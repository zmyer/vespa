// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "hash_map_insert.hpp"
#include "hashtable.hpp"

namespace vespalib {

template <typename K, typename V, typename H, typename EQ, typename M>
hash_map<K, V, H, EQ, M>::hash_map(size_t reserveSize) :
    _ht(reserveSize)
{ }

template <typename K, typename V, typename H, typename EQ, typename M>
hash_map<K, V, H, EQ, M>::hash_map(size_t reserveSize, H hasher, EQ equality) :
    _ht(reserveSize, hasher, equality)
{ }

template <typename K, typename V, typename H, typename EQ, typename M>
hash_map<K, V, H, EQ, M>::~hash_map() = default;

template <typename K, typename V, typename H, typename EQ, typename M>
void
hash_map<K, V, H, EQ, M>::erase(const K & key) {
    return _ht.erase(key);
}

template <typename K, typename V, typename H, typename EQ, typename M>
void
hash_map<K, V, H, EQ, M>::clear() {
    _ht.clear();
}

template <typename K, typename V, typename H, typename EQ, typename M>
void
hash_map<K, V, H, EQ, M>::resize(size_t newSize) {
    _ht.resize(newSize);
}

template <typename K, typename V, typename H, typename EQ, typename M>
void
hash_map<K, V, H, EQ, M>::swap(hash_map & rhs) {
    _ht.swap(rhs._ht);
}

template <typename K, typename V, typename H, typename EQ, typename M>
size_t
hash_map<K, V, H, EQ, M>::getMemoryConsumption() const {
    return _ht.getMemoryConsumption();
}

template <typename K, typename V, typename H, typename EQ, typename M>
size_t
hash_map<K, V, H, EQ, M>::getMemoryUsed() const
{
    return _ht.getMemoryUsed();
}

}


#define VESPALIB_HASH_MAP_INSTANTIATE_H_E_M(K, V, H, E, M) \
    template class vespalib::hash_map<K, V, H, E, M>; \
    template class vespalib::hashtable<K, std::pair<K,V>, H, E, std::_Select1st<std::pair<K,V>>, M>; \
    template vespalib::hashtable<K, std::pair<K,V>, H, E, std::_Select1st<std::pair<K,V>>, M>::insert_result \
             vespalib::hashtable<K, std::pair<K,V>, H, E, std::_Select1st<std::pair<K,V>>, M>::insert(std::pair<K,V> &&); \
    template vespalib::hashtable<K, std::pair<K,V>, H, E, std::_Select1st<std::pair<K,V>>, M>::insert_result \
             vespalib::hashtable<K, std::pair<K,V>, H, E, std::_Select1st<std::pair<K,V>>, M>::insertInternal(std::pair<K,V> &&); \
    template class vespalib::Array<vespalib::hash_node<std::pair<K,V>>>;

#define VESPALIB_HASH_MAP_INSTANTIATE_H_E(K, V, H, E) \
    VESPALIB_HASH_MAP_INSTANTIATE_H_E_M(K, V, H, E, vespalib::hashtable_base::prime_modulator)

#define VESPALIB_HASH_MAP_INSTANTIATE_H(K, V, H) VESPALIB_HASH_MAP_INSTANTIATE_H_E(K, V, H, std::equal_to<K>)

#define VESPALIB_HASH_MAP_INSTANTIATE(K, V) VESPALIB_HASH_MAP_INSTANTIATE_H(K, V, vespalib::hash<K>)

