// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "dense_tensor_address_combiner.h"
#include <vespa/vespalib/util/exceptions.h>
#include <cassert>

namespace vespalib::tensor {

DenseTensorAddressCombiner::~DenseTensorAddressCombiner() = default;

DenseTensorAddressCombiner::DenseTensorAddressCombiner(const eval::ValueType &combined, const eval::ValueType &lhs,
                                                       const eval::ValueType &rhs)
    : _rightAddress(rhs),
      _combinedAddress(combined),
      _left(),
      _commonRight(),
      _right()
{
    auto rhsItr = rhs.dimensions().cbegin();
    auto rhsItrEnd = rhs.dimensions().cend();
    uint32_t numDimensions(0);
    for (const auto &lhsDim : lhs.dimensions()) {
        while ((rhsItr != rhsItrEnd) && (rhsItr->name < lhsDim.name)) {
            _right.emplace_back(numDimensions++, rhsItr-rhs.dimensions().cbegin());
            ++rhsItr;
        }
        if ((rhsItr != rhsItrEnd) && (rhsItr->name == lhsDim.name)) {
            _left.emplace_back(numDimensions, _left.size());
            _commonRight.emplace_back(numDimensions, rhsItr-rhs.dimensions().cbegin());
            ++numDimensions;
            ++rhsItr;
        } else {
            _left.emplace_back(numDimensions++, _left.size());
        }
    }
    while (rhsItr != rhsItrEnd) {
        _right.emplace_back(numDimensions++, rhsItr-rhs.dimensions().cbegin());
        ++rhsItr;
    }
}

AddressContext::AddressContext(const eval::ValueType &type)
    : _type(type),
      _accumulatedSize(_type.dimensions().size()),
      _address(type.dimensions().size(), 0)

{
    size_t multiplier = 1;
    for (int32_t i(_address.size() - 1); i >= 0; i--) {
        _accumulatedSize[i] = multiplier;
        multiplier *= type.dimensions()[i].size;
    }
}

AddressContext::~AddressContext() = default;

eval::ValueType
DenseTensorAddressCombiner::combineDimensions(const eval::ValueType &lhs, const eval::ValueType &rhs)
{
    // NOTE: both lhs and rhs are sorted according to dimension names.
    std::vector<eval::ValueType::Dimension> result;
    auto lhsItr = lhs.dimensions().cbegin();
    auto rhsItr = rhs.dimensions().cbegin();
    while (lhsItr != lhs.dimensions().end() &&
           rhsItr != rhs.dimensions().end()) {
        if (lhsItr->name == rhsItr->name) {
            result.emplace_back(lhsItr->name, std::min(lhsItr->size, rhsItr->size));
            ++lhsItr;
            ++rhsItr;
        } else if (lhsItr->name < rhsItr->name) {
            result.emplace_back(*lhsItr++);
        } else {
            result.emplace_back(*rhsItr++);
        }
    }
    while (lhsItr != lhs.dimensions().end()) {
        result.emplace_back(*lhsItr++);
    }
    while (rhsItr != rhs.dimensions().end()) {
        result.emplace_back(*rhsItr++);
    }
    return (result.empty() ?
            eval::ValueType::double_type() :
            eval::ValueType::tensor_type(std::move(result)));
}

}
