// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/testapp.h>

#include <vespa/searchcore/proton/flushengine/prepare_restart_flush_strategy.h>
#include <vespa/searchcore/proton/flushengine/flush_target_candidates.h>
#include <vespa/searchcore/proton/flushengine/tls_stats_map.h>
#include <vespa/searchcore/proton/test/dummy_flush_handler.h>
#include <vespa/searchcore/proton/test/dummy_flush_target.h>

using namespace proton;
using search::SerialNum;
using searchcorespi::IFlushTarget;

using SimpleFlushHandler = test::DummyFlushHandler;
using FlushCandidatesList = std::vector<FlushTargetCandidates>;
using Config = PrepareRestartFlushStrategy::Config;

const Config DEFAULT_CFG(2.0, 0.0, 4.0);

struct SimpleFlushTarget : public test::DummyFlushTarget
{
    SerialNum flushedSerial;
    uint64_t approxDiskBytes;
    SimpleFlushTarget(const vespalib::string &name,
                      SerialNum flushedSerial_,
                      uint64_t approxDiskBytes_)
        : test::DummyFlushTarget(name),
          flushedSerial(flushedSerial_),
          approxDiskBytes(approxDiskBytes_)
    {}
    SimpleFlushTarget(const vespalib::string &name,
                      const Type &type,
                      SerialNum flushedSerial_,
                      uint64_t approxDiskBytes_)
        : test::DummyFlushTarget(name, type, Component::OTHER),
          flushedSerial(flushedSerial_),
          approxDiskBytes(approxDiskBytes_)
    {}
    virtual SerialNum getFlushedSerialNum() const override {
        return flushedSerial;
    }
    virtual uint64_t getApproxBytesToWriteToDisk() const override {
        return approxDiskBytes;
    }
};

class ContextsBuilder
{
private:
    FlushContext::List _result;
    std::map<vespalib::string, IFlushHandler::SP> _handlers;

    IFlushHandler::SP createAndGetHandler(const vespalib::string &handlerName) {
        auto itr = _handlers.find(handlerName);
        if (itr != _handlers.end()) {
            return itr->second;
        }
        IFlushHandler::SP handler = std::make_shared<SimpleFlushHandler>(handlerName);
        _handlers.insert(std::make_pair(handlerName, handler));
        return handler;
    }

public:
    ContextsBuilder() : _result(), _handlers() {}
    ContextsBuilder &add(const vespalib::string &handlerName,
                         const vespalib::string &targetName,
                         IFlushTarget::Type targetType,
                         SerialNum flushedSerial,
                         uint64_t approxDiskBytes) {
        IFlushHandler::SP handler = createAndGetHandler(handlerName);
        IFlushTarget::SP target = std::make_shared<SimpleFlushTarget>(targetName,
                                                                      targetType,
                                                                      flushedSerial,
                                                                      approxDiskBytes);
        _result.push_back(std::make_shared<FlushContext>(handler, target, 0));
        return *this;
    }
    ContextsBuilder &add(const vespalib::string &handlerName,
                         const vespalib::string &targetName,
                         SerialNum flushedSerial,
                         uint64_t approxDiskBytes) {
        return add(handlerName, targetName, IFlushTarget::Type::FLUSH, flushedSerial, approxDiskBytes);
    }
    ContextsBuilder &add(const vespalib::string &targetName,
                         SerialNum flushedSerial,
                         uint64_t approxDiskBytes) {
        return add("handler1", targetName, IFlushTarget::Type::FLUSH, flushedSerial, approxDiskBytes);
    }
    ContextsBuilder &addGC(const vespalib::string &targetName,
                           SerialNum flushedSerial,
                           uint64_t approxDiskBytes) {
        return add("handler1", targetName, IFlushTarget::Type::GC, flushedSerial, approxDiskBytes);
    }
    FlushContext::List build() const { return _result; }
};

class CandidatesBuilder
{
private:
    const FlushContext::List *_sortedFlushContexts;
    size_t _numCandidates;
    flushengine::TlsStats _tlsStats;
    Config _cfg;

public:
    CandidatesBuilder(const FlushContext::List &sortedFlushContexts)
        : _sortedFlushContexts(&sortedFlushContexts),
          _numCandidates(sortedFlushContexts.size()),
          _tlsStats(1000, 11, 110),
          _cfg(2.0, 3.0, 4.0)
    {}
    CandidatesBuilder &flushContexts(const FlushContext::List &sortedFlushContexts) {
        _sortedFlushContexts = &sortedFlushContexts;
        _numCandidates = sortedFlushContexts.size();
        return *this;
    }
    CandidatesBuilder &numCandidates(size_t numCandidates) {
        _numCandidates = numCandidates;
        return *this;
    }
    CandidatesBuilder &replayEnd(SerialNum replayEndSerial) {
        flushengine::TlsStats oldTlsStats = _tlsStats;
        _tlsStats = flushengine::TlsStats(oldTlsStats.getNumBytes(),
                                          oldTlsStats.getFirstSerial(),
                                          replayEndSerial);
        return *this;
    }
    FlushTargetCandidates build() const {
        return FlushTargetCandidates(*_sortedFlushContexts,
                                     _numCandidates,
                                     _tlsStats,
                                     _cfg);
    }
};

struct CandidatesFixture
{
    FlushContext::List emptyContexts;
    CandidatesBuilder builder;
    CandidatesFixture() : emptyContexts(), builder(emptyContexts) {}
};

void
assertCosts(double tlsReplayBytesCost, double tlsReplayOperationsCost, double flushTargetsWriteCost, const FlushTargetCandidates &candidates)
{
    EXPECT_EQUAL(tlsReplayBytesCost, candidates.getTlsReplayCost().bytesCost);
    EXPECT_EQUAL(tlsReplayOperationsCost, candidates.getTlsReplayCost().operationsCost);
    EXPECT_EQUAL(flushTargetsWriteCost, candidates.getFlushTargetsWriteCost());
    EXPECT_EQUAL(tlsReplayBytesCost + tlsReplayOperationsCost + flushTargetsWriteCost, candidates.getTotalCost());
}

TEST_F("require that tls replay cost is correct for 100% replay", CandidatesFixture)
{
    TEST_DO(assertCosts(1000 * 2, 100 * 3, 0, f.builder.replayEnd(110).build()));
}

TEST_F("require that tls replay cost is correct for 75% replay", CandidatesFixture)
{
    FlushContext::List contexts = ContextsBuilder().add("target1", 10, 0).add("target2", 35, 0).build();
    TEST_DO(assertCosts(750 * 2, 75 * 3, 0, f.builder.flushContexts(contexts).numCandidates(1).replayEnd(110).build()));
}

TEST_F("require that tls replay cost is correct for 25% replay", CandidatesFixture)
{
    FlushContext::List contexts = ContextsBuilder().add("target1", 10, 0).add("target2", 85, 0).build();
    TEST_DO(assertCosts(250 * 2, 25 * 3, 0, f.builder.flushContexts(contexts).numCandidates(1).replayEnd(110).build()));
}

TEST_F("require that tls replay cost is correct for zero operations to replay", CandidatesFixture)
{
    TEST_DO(assertCosts(0, 0, 0, f.builder.replayEnd(10).build()));
}

TEST_F("require that flush cost is correct for zero flush targets", CandidatesFixture)
{
    EXPECT_EQUAL(0, f.builder.build().getFlushTargetsWriteCost());
}

TEST_F("require that flush cost is sum of flush targets", CandidatesFixture)
{
    FlushContext::List contexts = ContextsBuilder().add("target1", 20, 1000).add("target2", 30, 2000).build();
    TEST_DO(assertCosts(0, 0, 1000 * 4 + 2000 * 4, f.builder.flushContexts(contexts).build()));
}


flushengine::TlsStatsMap
defaultTransactionLogStats()
{
    flushengine::TlsStatsMap::Map result;
    result.insert(std::make_pair("handler1", flushengine::TlsStats(1000, 11, 110)));
    result.insert(std::make_pair("handler2", flushengine::TlsStats(2000, 11, 110)));
    return std::move(result);
}

struct FlushStrategyFixture
{
    flushengine::TlsStatsMap _tlsStatsMap;
    PrepareRestartFlushStrategy strategy;
    FlushStrategyFixture()
        : _tlsStatsMap(defaultTransactionLogStats()),
          strategy(DEFAULT_CFG)
    {}
    FlushContext::List getFlushTargets(const FlushContext::List &targetList,
                                       const flushengine::TlsStatsMap &tlsStatsMap) const {
        return strategy.getFlushTargets(targetList, tlsStatsMap);
    }
};

vespalib::string
toString(const FlushContext::List &flushContexts)
{
    std::ostringstream oss;
    oss << "[";
    bool comma = false;
    for (const auto &flushContext : flushContexts) {
        if (comma) {
            oss << ",";
        }
        oss << flushContext->getTarget()->getName();
        comma = true;
    }
    oss << "]";
    return oss.str();
}

void
assertFlushContexts(const vespalib::string &expected, const FlushContext::List &actual)
{
    EXPECT_EQUAL(expected, toString(actual));
}

/**
 * For the following tests the content of the TLS is as follows:
 *   - handler1: serial numbers 10 -> 110, 1000 bytes
 *   - handler2: serial numbers 10 -> 110, 2000 bytes
 *
 * The cost config is: tlsReplayByteCost=2.0, tlsReplayOperationCost=0.0, flushTargetsWriteCost=4.0.
 * The cost of replaying the complete TLS is then:
 *   - handler1: 1000*2.0 = 2000
 *   - handler2: 2000*2.0 = 4000
 *
 * With 3 flush targets that has getApproxBytesToWriteToDisk=167,
 * the total write cost is 3*167*4.0 = 2004.
 *
 * This should give the baseline for understanding the following tests:
 */

TEST_F("require that the best strategy is flushing 0 targets", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("foo", 10, 167).add("bar", 10, 167).add("baz", 10, 167).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[]", targets));
}

TEST_F("require that the best strategy is flushing all targets", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("foo", 10, 166).add("bar", 10, 166).add("baz", 10, 166).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[bar,baz,foo]", targets));
}

TEST_F("require that the best strategy is flushing all targets (with different unflushed serial)", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("foo", 10, 166).add("bar", 11, 166).add("baz", 12, 166).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[foo,bar,baz]", targets));
}

TEST_F("require that the best strategy is flushing 1 target", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("foo", 10, 249).add("bar", 60, 125).add("baz", 60, 125).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[foo]", targets));
}

TEST_F("require that the best strategy is flushing 2 targets", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("foo", 10, 124).add("bar", 11, 124).add("baz", 60, 251).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[foo,bar]", targets));
}

TEST_F("require that GC flush targets are removed", FlushStrategyFixture)
{
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            addGC("foo", 10, 124).add("bar", 11, 124).add("baz", 60, 251).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[bar]", targets));
}

TEST_F("require that flush targets for different flush handlers are treated independently", FlushStrategyFixture)
{
    // best strategy for handler1 is flushing 1 target (foo)
    // best strategy for handler2 is flushing 2 targets (baz,quz)
    FlushContext::List targets = f.getFlushTargets(ContextsBuilder().
            add("handler1", "foo", 10, 249).add("handler1", "bar", 60, 251).
            add("handler2", "baz", 10, 499).add("handler2", "quz", 60, 499).build(), f._tlsStatsMap);
    TEST_DO(assertFlushContexts("[foo,baz,quz]", targets));
}


TEST_MAIN()
{
    TEST_RUN_ALL();
}
