// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchlib/fef/featureexecutor.h>
#include <vespa/eval/eval/tensor_spec.h>
#include <vespa/eval/eval/value.h>
#include <vespa/eval/eval/value_type.h>
#include <vespa/eval/tensor/default_tensor_engine.h>
#include <memory>

namespace search {
namespace features {

/**
 * Feature executor that returns a constant tensor.
 */
class ConstantTensorExecutor : public fef::FeatureExecutor
{
private:
    vespalib::eval::Value::UP _tensor;

public:
    ConstantTensorExecutor(vespalib::eval::Value::UP tensor)
        : _tensor(std::move(tensor))
    {}
    virtual bool isPure() override { return true; }
    virtual void execute(uint32_t) override {
        outputs().set_object(0, *_tensor);
    }
    static fef::FeatureExecutor &create(std::unique_ptr<vespalib::eval::Tensor> tensor, vespalib::Stash &stash) {
        return stash.create<ConstantTensorExecutor>(std::move(tensor));
    }
    static fef::FeatureExecutor &createEmpty(const vespalib::eval::ValueType &valueType, vespalib::Stash &stash) {
        const auto &engine = vespalib::tensor::DefaultTensorEngine::ref();
        auto spec = vespalib::eval::TensorSpec(valueType.to_spec());
        return stash.create<ConstantTensorExecutor>(engine.from_spec(spec));
    }
    static fef::FeatureExecutor &createEmpty(vespalib::Stash &stash) {
        return createEmpty(vespalib::eval::ValueType::double_type(), stash);
    }
};

} // namespace features
} // namespace search
