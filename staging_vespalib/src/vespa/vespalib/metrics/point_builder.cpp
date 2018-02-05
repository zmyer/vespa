// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include "point.h"
#include "metrics_manager.h"

namespace vespalib {
namespace metrics {

PointBuilder::PointBuilder(std::shared_ptr<MetricsManager> m)
    : _owner(std::move(m)), _map()
{}

PointBuilder::PointBuilder(std::shared_ptr<MetricsManager> m,
                           const PointMap::BackingMap &copyFrom)
    : _owner(std::move(m)), _map(copyFrom)
{}

PointBuilder &&
PointBuilder::bind(Dimension dimension, Label label) &&
{
    _map.erase(dimension);
    _map.emplace(dimension, label);
    return std::move(*this);
}

PointBuilder &&
PointBuilder::bind(Dimension dimension, LabelValue label) &&
{
    Label c = _owner->label(label);
    return std::move(*this).bind(dimension, c);
}

PointBuilder &&
PointBuilder::bind(DimensionName dimension, LabelValue label) &&
{
    Dimension a = _owner->dimension(dimension);
    Label c = _owner->label(label);
    return std::move(*this).bind(a, c);
}

Point
PointBuilder::build()
{
    return _owner->pointFrom(PointMap::BackingMap(_map));
}

PointBuilder::operator Point() &&
{
    return _owner->pointFrom(std::move(_map));
}

} // namespace vespalib::metrics
} // namespace vespalib
