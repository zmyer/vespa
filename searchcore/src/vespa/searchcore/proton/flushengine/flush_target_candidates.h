// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "prepare_restart_flush_strategy.h"

namespace proton {

namespace flushengine { class TlsStats; }

/**
 * A set of flush targets that are candidates to be flushed.
 *
 * The total cost of using this set of candidates is:
 *   - the cost of replaying the TLS (after these are flushed) +
 *   - the cost of flushing these to disk
 */
class FlushTargetCandidates
{
public:
    struct TlsReplayCost {
        double bytesCost;
        double operationsCost;
        TlsReplayCost(double bytesCost_, double operationsCost_)
            : bytesCost(bytesCost_),
              operationsCost(operationsCost_)
        {}
        double totalCost() const { return bytesCost + operationsCost; }
    };
private:
    const FlushContext::List *_sortedFlushContexts; // NOTE: ownership is handled outside
    size_t _numCandidates;
    TlsReplayCost _tlsReplayCost;
    double _flushTargetsWriteCost;

    using Config = PrepareRestartFlushStrategy::Config;

public:
    using UP = std::unique_ptr<FlushTargetCandidates>;

    FlushTargetCandidates(const FlushContext::List &sortedFlushContexts,
                          size_t numCandidates,
                          const flushengine::TlsStats &tlsStats,
                          const Config &cfg);

    TlsReplayCost getTlsReplayCost() const { return _tlsReplayCost; }
    double getFlushTargetsWriteCost() const { return _flushTargetsWriteCost; }
    double getTotalCost() const { return getTlsReplayCost().totalCost() + getFlushTargetsWriteCost(); }
    FlushContext::List getCandidates() const;
};

} // namespace proton
