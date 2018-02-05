// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/metrics/metrics.h>

namespace proton {

/**
 * Usage metrics for various resources in this search engine.
 */
struct ResourceUsageMetrics : metrics::MetricSet
{
    metrics::DoubleValueMetric disk;
    metrics::DoubleValueMetric diskUtilization;
    metrics::DoubleValueMetric memory;
    metrics::DoubleValueMetric memoryUtilization;
    metrics::LongValueMetric memoryMappings;
    metrics::LongValueMetric openFileDescriptors;
    metrics::LongValueMetric feedingBlocked;

    ResourceUsageMetrics(metrics::MetricSet *parent);
    ~ResourceUsageMetrics();
};

} // namespace proton
