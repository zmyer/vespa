// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <memory>
#include <vespa/vespalib/net/metrics_producer.h>

namespace vespalib {
namespace metrics {

class MetricsManager;

/**
 * Utility class for wiring a MetricsManager into a StateApi.
 **/
class Producer : public vespalib::MetricsProducer {
private:
    std::shared_ptr<MetricsManager> _manager;
public:
    Producer(std::shared_ptr<MetricsManager> m);
    vespalib::string getMetrics(const vespalib::string &consumer) override;
    vespalib::string getTotalMetrics(const vespalib::string &consumer) override;
};

} // namespace vespalib::metrics
} // namespace vespalib
