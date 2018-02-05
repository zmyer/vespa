// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "sparse_tensor_apply.h"
#include "sparse_tensor_address_combiner.h"
#include <vespa/eval/tensor/direct_tensor_builder.h>
#include "direct_sparse_tensor_builder.h"

namespace vespalib::tensor::sparse {

template <typename Function>
std::unique_ptr<Tensor>
apply(const SparseTensor &lhs, const SparseTensor &rhs, Function &&func)
{
    DirectTensorBuilder<SparseTensor> builder(lhs.combineDimensionsWith(rhs));
    TensorAddressCombiner addressCombiner(lhs.fast_type(), rhs.fast_type());
    size_t estimatedCells = (lhs.cells().size() * rhs.cells().size());
    if (addressCombiner.numOverlappingDimensions() != 0) {
        estimatedCells = std::min(lhs.cells().size(), rhs.cells().size());
    }
    builder.reserve(estimatedCells*2);
    for (const auto &lhsCell : lhs.cells()) {
        for (const auto &rhsCell : rhs.cells()) {
            bool combineSuccess = addressCombiner.combine(lhsCell.first, rhsCell.first);
            if (combineSuccess) {
                builder.insertCell(addressCombiner.getAddressRef(),
                                   func(lhsCell.second, rhsCell.second));
            }
        }
    }
    return builder.build();
}

}
