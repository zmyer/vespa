// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <memory>
#include "metric_identifier.h"
#include "point.h"

namespace vespalib {
namespace metrics {

class MetricsManager;
class GaugeAggregator;

/**
 * Represents a gauge metric that can be measured.
 **/
class Gauge {
private:
    std::shared_ptr<MetricsManager> _manager;
    MetricName _id;
public:
    Gauge(std::shared_ptr<MetricsManager> m, MetricName id)
        : _manager(std::move(m)), _id(id)
    {}

    /**
     * Provide a sample for the gauge.
     * @param value the measurement for this sample
     * @param p the point representing labels for this sample (default empty)
     **/
    void sample(double value, Point p = Point::empty) const;

    // internal
    struct Measurement {
        MetricIdentifier idx;
        double value;
        Measurement() = delete;
        Measurement(MetricIdentifier id, double v) : idx(id), value(v) {}
    };

    typedef GaugeAggregator aggregator_type;
    typedef Measurement sample_type;
};

} // namespace vespalib::metrics
} // namespace vespalib
