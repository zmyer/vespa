// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "sparse_tensor_address_reducer.h"
#include <vespa/eval/tensor/direct_tensor_builder.h>
#include "direct_sparse_tensor_builder.h"

namespace vespalib::tensor::sparse {

template <typename Function>
std::unique_ptr<Tensor>
reduceAll(const SparseTensor &tensor,
          DirectTensorBuilder<SparseTensor> &builder, Function &&func)
{
    auto itr = tensor.cells().begin();
    auto itrEnd = tensor.cells().end();
    double result = 0.0;
    if (itr != itrEnd) {
        result = itr->second;
        ++itr;
    }
    for (; itr != itrEnd; ++itr) {
        result = func(result, itr->second);
    }
    builder.insertCell(SparseTensorAddressBuilder().getAddressRef(), result);
    return builder.build();
}

template <typename Function>
std::unique_ptr<Tensor>
reduceAll(const SparseTensor &tensor, Function &&func)
{
    DirectTensorBuilder<SparseTensor> builder;
    return reduceAll(tensor, builder, func);
}

template <typename Function>
std::unique_ptr<Tensor>
reduce(const SparseTensor &tensor,
       const std::vector<vespalib::string> &dimensions, Function &&func)
{
    if (dimensions.empty()) {
        return reduceAll(tensor, func);
    }
    DirectTensorBuilder<SparseTensor> builder(tensor.fast_type().reduce(dimensions));
    if (builder.fast_type().dimensions().empty()) {
        return reduceAll(tensor, builder, func);
    }
    TensorAddressReducer addressReducer(tensor.fast_type(), dimensions);
    builder.reserve(tensor.cells().size()*2);
    for (const auto &cell : tensor.cells()) {
        addressReducer.reduce(cell.first);
        builder.insertCell(addressReducer.getAddressRef(), cell.second, func);
    }
    return builder.build();
}

}
