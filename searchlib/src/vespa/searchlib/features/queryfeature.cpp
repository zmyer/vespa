// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "queryfeature.h"
#include "utils.h"
#include "valuefeature.h"

#include <vespa/searchlib/features/constant_tensor_executor.h>
#include <vespa/searchlib/fef/featureexecutor.h>
#include <vespa/searchlib/fef/indexproperties.h>
#include <vespa/searchlib/fef/properties.h>
#include <vespa/searchlib/fef/feature_type.h>
#include <vespa/vespalib/objects/nbostream.h>
#include <vespa/eval/tensor/default_tensor.h>
#include <vespa/eval/tensor/tensor_mapper.h>
#include <vespa/eval/tensor/serialization/typed_binary_format.h>
#include <vespa/eval/eval/value_type.h>
#include <vespa/vespalib/locale/c.h>

using namespace search::fef;
using namespace search::fef::indexproperties;
using vespalib::tensor::DefaultTensor;
using vespalib::tensor::TensorBuilder;
using vespalib::eval::ValueType;
using search::fef::FeatureType;

namespace search {
namespace features {

namespace {

/**
 * Convert a string to a feature value using special quoting
 * mechanics; a string that can be converted directly into a feature
 * (numeric value) will be converted. If the string cannot be
 * converted directly, it will be hashed, after stripping the leading
 * "'" if it exists.
 *
 * @return feature value
 * @param str string value to be converted
 **/
feature_t asFeature(const vespalib::string &str) {
    char *end;
    errno = 0;
    double val = vespalib::locale::c::strtod(str.c_str(), &end);
    if (errno != 0 || *end != '\0') { // not happy
        if (str.size() > 0 && str[0] == '\'') {
            val = vespalib::hash_code(str.substr(1));
        } else {
            val = vespalib::hash_code(str);
        }
    }
    return val;
}

} // namespace search::features::<unnamed>

QueryBlueprint::QueryBlueprint() :
    Blueprint("query"),
    _key(),
    _key2(),
    _defaultValue(0),
    _valueType(ValueType::double_type())
{
}

QueryBlueprint::~QueryBlueprint()
{
}

void
QueryBlueprint::visitDumpFeatures(const IIndexEnvironment &,
                                  IDumpFeatureVisitor &) const
{
}

Blueprint::UP
QueryBlueprint::createInstance() const
{
    return Blueprint::UP(new QueryBlueprint());
}

bool
QueryBlueprint::setup(const IIndexEnvironment &env,
                      const ParameterList &params)
{
    _key = params[0].getValue();
    _key2 = "$";
    _key2.append(_key);

    vespalib::string key3;
    key3.append("query(");
    key3.append(_key);
    key3.append(")");
    Property p = env.getProperties().lookup(key3);
    if (!p.found()) {
        p = env.getProperties().lookup(_key2);
    }
    if (p.found()) {
        _defaultValue = asFeature(p.get());
    }
    vespalib::string queryFeatureType = type::QueryFeature::lookup(env.getProperties(), _key);
    if (!queryFeatureType.empty()) {
        _valueType = ValueType::from_spec(queryFeatureType);
    }
    FeatureType output_type = _valueType.is_tensor()
                              ? FeatureType::object(_valueType)
                              : FeatureType::number();
    describeOutput("out", "The value looked up in query properties using the given key.",
                   output_type);
    return true;
}

namespace {

FeatureExecutor &
createTensorExecutor(const search::fef::IQueryEnvironment &env,
                     const vespalib::string &queryKey,
                     const ValueType &valueType, vespalib::Stash &stash)
{
    search::fef::Property prop = env.getProperties().lookup(queryKey);
    if (prop.found() && !prop.get().empty()) {
        const vespalib::string &value = prop.get();
        vespalib::nbostream stream(value.data(), value.size());
        vespalib::tensor::Tensor::UP tensor = vespalib::tensor::TypedBinaryFormat::deserialize(stream);
        if (tensor->type() != valueType) {
            vespalib::tensor::TensorMapper mapper(valueType);
            vespalib::tensor::Tensor::UP mappedTensor = mapper.map(*tensor);
            tensor = std::move(mappedTensor);
        }
        return ConstantTensorExecutor::create(std::move(tensor), stash);
    }
    return ConstantTensorExecutor::createEmpty(valueType, stash);
}

}

FeatureExecutor &
QueryBlueprint::createExecutor(const IQueryEnvironment &env, vespalib::Stash &stash) const
{
    if (_valueType.is_tensor()) {
        return createTensorExecutor(env, _key, _valueType, stash);
    } else {
        std::vector<feature_t> values;
        Property p = env.getProperties().lookup(_key);
        if (!p.found()) {
            p = env.getProperties().lookup(_key2);
        }
        if (p.found()) {
            values.push_back(asFeature(p.get()));
        } else {
            values.push_back(_defaultValue);
        }
        return stash.create<ValueExecutor>(values);
    }
}

} // namespace features
} // namespace search
