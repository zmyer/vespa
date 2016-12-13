// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <vespa/fastos/fastos.h>
#include "featureoverrider.h"

namespace search {
namespace fef {

void
FeatureOverrider::handle_bind_inputs(vespalib::ConstArrayRef<const NumberOrObject *> inputs)
{
    _executor.bind_inputs(inputs);
}

void
FeatureOverrider::handle_bind_outputs(vespalib::ArrayRef<NumberOrObject> outputs)
{
    _executor.bind_outputs(outputs);
}

FeatureOverrider::FeatureOverrider(FeatureExecutor &executor, uint32_t outputIdx, feature_t value)
    : _executor(executor),
      _outputIdx(outputIdx),
      _value(value)
{
}

bool
FeatureOverrider::isPure()
{
    return _executor.isPure();
}

void
FeatureOverrider::execute(uint32_t docId)
{
    _executor.execute(docId);
    if (_outputIdx < outputs().size()) {
        outputs().set_number(_outputIdx, _value);
    }
}

void
FeatureOverrider::handle_bind_match_data(MatchData &md)
{
    _executor.bind_match_data(md);
}

} // namespace fef
} // namespace search
