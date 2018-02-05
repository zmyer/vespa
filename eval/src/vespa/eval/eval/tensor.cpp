// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "tensor.h"
#include "tensor_engine.h"
#include "tensor_spec.h"

namespace vespalib {
namespace eval {

bool
operator==(const Tensor &lhs, const Tensor &rhs)
{
    auto lhs_spec = lhs.engine().to_spec(lhs);
    auto rhs_spec = rhs.engine().to_spec(rhs);
    return (lhs_spec == rhs_spec);
}

std::ostream &
operator<<(std::ostream &out, const Tensor &tensor)
{
    out << tensor.engine().to_spec(tensor).to_string();
    return out;
}

} // namespace vespalib::eval
} // namespace vespalib
