// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <vespa/searchcore/proton/flushengine/cachedflushtarget.h>
#include <vespa/searchcore/proton/flushengine/flush_engine_explorer.h>
#include <vespa/searchcore/proton/flushengine/flushengine.h>
#include <vespa/searchcore/proton/flushengine/threadedflushtarget.h>
#include <vespa/searchcore/proton/flushengine/tls_stats_map.h>
#include <vespa/searchcore/proton/flushengine/i_tls_stats_factory.h>
#include <vespa/searchcore/proton/server/igetserialnum.h>
#include <vespa/searchcore/proton/test/dummy_flush_handler.h>
#include <vespa/searchcore/proton/test/dummy_flush_target.h>
#include <vespa/vespalib/testkit/testapp.h>
#include <vespa/vespalib/data/slime/slime.h>
#include <vespa/vespalib/test/insertion_operators.h>
#include <mutex>
#include <chrono>

#include <vespa/log/log.h>
LOG_SETUP("flushengine_test");

// --------------------------------------------------------------------------------
//
// Setup.
//
// --------------------------------------------------------------------------------

using namespace proton;
using namespace vespalib::slime;
using searchcorespi::IFlushTarget;
using searchcorespi::FlushTask;
using vespalib::Slime;

const long LONG_TIMEOUT = 66666;
const long SHORT_TIMEOUT = 1;
const uint32_t IINTERVAL = 1000;

class SimpleExecutor : public vespalib::Executor {
public:
    vespalib::Gate _done;

public:
    SimpleExecutor()
        : _done()
    {
        // empty
    }

    Task::UP
    execute(Task::UP task) override
    {
        task->run();
        _done.countDown();
        return Task::UP();
    }
};

class SimpleGetSerialNum : public IGetSerialNum
{
    virtual search::SerialNum getSerialNum() const override {
        return 0u;
    }
};

class SimpleTlsStatsFactory : public flushengine::ITlsStatsFactory
{
    virtual flushengine::TlsStatsMap create() override {
        vespalib::hash_map<vespalib::string, flushengine::TlsStats> map;
        return flushengine::TlsStatsMap(std::move(map));
    }
};

class SimpleHandler;

class WrappedFlushTask : public searchcorespi::FlushTask
{
    searchcorespi::FlushTask::UP _task;
    SimpleHandler &_handler;

public:
    virtual void run() override;
    WrappedFlushTask(searchcorespi::FlushTask::UP task,
                     SimpleHandler &handler)
        : _task(std::move(task)),
          _handler(handler)
    {
    }

    virtual search::SerialNum getFlushSerial() const override
    {
        return _task->getFlushSerial();
    }
};

class WrappedFlushTarget : public FlushTargetProxy
{
    SimpleHandler &_handler;
public:
    WrappedFlushTarget(const IFlushTarget::SP &target,
                       SimpleHandler &handler)
        : FlushTargetProxy(target),
          _handler(handler)
    {
    }

    virtual Task::UP initFlush(SerialNum currentSerial) override
    {
        Task::UP task(_target->initFlush(currentSerial));
        if (task) {
            return std::make_unique<WrappedFlushTask>(std::move(task),
                                                      _handler);
        }
        return std::move(task);
    }
};

typedef std::vector<IFlushTarget::SP> Targets;

using FlushDoneHistory = std::vector<search::SerialNum>;

class SimpleHandler : public test::DummyFlushHandler {
public:
    Targets                   _targets;
    search::SerialNum         _oldestSerial;
    search::SerialNum         _currentSerial;
    uint32_t                  _pendingDone;
    std::mutex                _lock;
    vespalib::CountDownLatch  _done;
    FlushDoneHistory          _flushDoneHistory;

public:
    typedef std::shared_ptr<SimpleHandler> SP;

    SimpleHandler(const Targets &targets, const std::string &name = "anon",
                  search::SerialNum currentSerial = -1)
        : test::DummyFlushHandler(name),
          _targets(targets),
          _oldestSerial(0),
          _currentSerial(currentSerial),
          _pendingDone(0u),
          _lock(),
          _done(targets.size()),
          _flushDoneHistory()
    {
        // empty
    }

    search::SerialNum
    getCurrentSerialNumber() const override
    {
        LOG(info, "SimpleHandler(%s)::getCurrentSerialNumber()",
            getName().c_str());
        return _currentSerial;
    }

    std::vector<IFlushTarget::SP>
    getFlushTargets() override
    {
        LOG(info, "SimpleHandler(%s)::getFlushTargets()",
            getName().c_str());
        std::vector<IFlushTarget::SP> wrappedTargets;
        for (const auto &target : _targets) {
            wrappedTargets.push_back(std::make_shared<WrappedFlushTarget>
                                     (target, *this));
        }
        return std::move(wrappedTargets);
    }

    // Called once by flush engine slave thread for each task done
    void taskDone()
    {
        std::lock_guard<std::mutex> guard(_lock);
        ++_pendingDone;
    }

    // Called by flush engine master thread after flush handler is
    // added to flush engine and when one or more flush tasks related
    // to flush handler have completed.
    void
    flushDone(search::SerialNum oldestSerial) override
    {
        std::lock_guard<std::mutex> guard(_lock);
        LOG(info, "SimpleHandler(%s)::flushDone(%" PRIu64 ")",
            getName().c_str(), oldestSerial);
        _oldestSerial = std::max(_oldestSerial, oldestSerial);
        _flushDoneHistory.push_back(oldestSerial);
        while (_pendingDone > 0) {
            --_pendingDone;
            _done.countDown();
        }
    }

    FlushDoneHistory getFlushDoneHistory()
    {
        std::lock_guard<std::mutex> guard(_lock);
        return _flushDoneHistory;
    }
};

void WrappedFlushTask::run()
{
    _task->run();
    _handler.taskDone();
}

class SimpleTask : public searchcorespi::FlushTask {
    search::SerialNum &_flushedSerial;
    search::SerialNum &_currentSerial;
public:
    vespalib::Gate &_start;
    vespalib::Gate &_done;
    vespalib::Gate *_proceed;

public:
    SimpleTask(vespalib::Gate &start,
               vespalib::Gate &done,
               vespalib::Gate *proceed,
               search::SerialNum &flushedSerial,
               search::SerialNum &currentSerial)
        : _flushedSerial(flushedSerial), _currentSerial(currentSerial),
          _start(start), _done(done), _proceed(proceed)
    {
        // empty
    }

    void run() override {
        _start.countDown();
        if (_proceed != NULL) {
            _proceed->await();
        }
        _flushedSerial = _currentSerial;
        _done.countDown();
    }

    virtual search::SerialNum
    getFlushSerial() const override
    {
        return 0u;
    }
};

class SimpleTarget : public test::DummyFlushTarget {
public:
    search::SerialNum _flushedSerial;
    search::SerialNum _currentSerial;
    vespalib::Gate    _proceed;
    vespalib::Gate    _initDone;
    vespalib::Gate    _taskStart;
    vespalib::Gate    _taskDone;
    Task::UP          _task;

public:
    typedef std::shared_ptr<SimpleTarget> SP;

    SimpleTarget(Task::UP task, const std::string &name) :
        test::DummyFlushTarget(name),
        _flushedSerial(0),
        _currentSerial(0),
        _proceed(),
        _initDone(),
        _taskStart(),
        _taskDone(),
        _task(std::move(task))
    {
    }

    SimpleTarget(const std::string &name, search::SerialNum flushedSerial = 0, bool proceedImmediately = true) :
        test::DummyFlushTarget(name),
        _flushedSerial(flushedSerial),
        _proceed(),
        _initDone(),
        _taskStart(),
        _taskDone(),
        _task(new SimpleTask(_taskStart, _taskDone, &_proceed,
                             _flushedSerial, _currentSerial))
    {
        if (proceedImmediately) {
            _proceed.countDown();
        }
    }
    SimpleTarget(search::SerialNum flushedSerial = 0, bool proceedImmediately = true)
        : SimpleTarget("anon", flushedSerial, proceedImmediately)
    { }

    virtual Time
    getLastFlushTime() const override { return fastos::ClockSystem::now(); }

    virtual SerialNum
    getFlushedSerialNum() const override
    {
        LOG(info, "SimpleTarget(%s)::getFlushedSerialNum() = %" PRIu64,
            getName().c_str(), _flushedSerial);
        return _flushedSerial;
    }

    virtual Task::UP
    initFlush(SerialNum currentSerial) override
    {
        LOG(info, "SimpleTarget(%s)::initFlush(%" PRIu64 ")",
            getName().c_str(), currentSerial);
        _currentSerial = currentSerial;
        _initDone.countDown();
        return std::move(_task);
    }

};

class AssertedTarget : public SimpleTarget {
public:
    mutable bool _mgain;
    mutable bool _serial;

public:
    typedef std::shared_ptr<AssertedTarget> SP;

    AssertedTarget()
        : SimpleTarget("anon"),
          _mgain(false),
          _serial(false)
    {
    }

    virtual MemoryGain
    getApproxMemoryGain() const override
    {
        LOG_ASSERT(_mgain == false);
        _mgain = true;
        return SimpleTarget::getApproxMemoryGain();
    }

    virtual search::SerialNum
    getFlushedSerialNum() const override
    {
        LOG_ASSERT(_serial == false);
        _serial = true;
        return SimpleTarget::getFlushedSerialNum();
    }
};

class SimpleStrategy : public IFlushStrategy {
public:
    std::vector<IFlushTarget::SP> _targets;

    struct CompareTarget {
        CompareTarget(const SimpleStrategy &flush) : _flush(flush) { }
        bool operator () (const FlushContext::SP &lhs, const FlushContext::SP &rhs) const {
            return _flush.compare(lhs->getTarget(), rhs->getTarget());
        }
        const SimpleStrategy &_flush;
    };

    virtual FlushContext::List getFlushTargets(const FlushContext::List &targetList,
                                               const flushengine::TlsStatsMap &) const override {
        FlushContext::List fv(targetList);
        std::sort(fv.begin(), fv.end(), CompareTarget(*this));
        return fv;
    }

    bool
    compare(const IFlushTarget::SP &lhs, const IFlushTarget::SP &rhs) const
    {
        LOG(info, "SimpleStrategy::compare(%p, %p)", lhs.get(), rhs.get());
        return indexOf(lhs) < indexOf(rhs);
    }


public:
    typedef std::shared_ptr<SimpleStrategy> SP;

    SimpleStrategy()
    {
        // empty
    }

    uint32_t
    indexOf(const IFlushTarget::SP &target) const
    {
        IFlushTarget *raw = target.get();
        CachedFlushTarget *cached = dynamic_cast<CachedFlushTarget*>(raw);
        if (cached != NULL) {
            raw = cached->getFlushTarget().get();
        }
        WrappedFlushTarget *wrapped = dynamic_cast<WrappedFlushTarget *>(raw);
        if (wrapped != nullptr) {
            raw = wrapped->getFlushTarget().get();
        }
        for (uint32_t i = 0, len = _targets.size(); i < len; ++i) {
            if (raw == _targets[i].get()) {
                LOG(info, "Index of target %p is %d.", raw, i);
                return i;
            }
        }
        LOG(info, "Target %p not found.", raw);
        return -1;
    }
};

class NoFlushStrategy : public SimpleStrategy
{
    virtual FlushContext::List getFlushTargets(const FlushContext::List &,
                                               const flushengine::TlsStatsMap &) const override {
        return FlushContext::List();
    }
};

// --------------------------------------------------------------------------------
//
// Tests.
//
// --------------------------------------------------------------------------------

class AppendTask : public FlushTask
{
public:
    AppendTask(const vespalib::string & name, std::vector<vespalib::string> & list, vespalib::Gate & done) :
        _list(list),
        _done(done),
        _name(name)
    { }
    void run() override {
        _list.push_back(_name);
        _done.countDown();
    }
    virtual search::SerialNum
    getFlushSerial() const override
    {
        return 0u;
    }
    std::vector<vespalib::string> & _list;
    vespalib::Gate    & _done;
    vespalib::string    _name;
};


struct Fixture
{
    std::shared_ptr<flushengine::ITlsStatsFactory> tlsStatsFactory;
    SimpleStrategy::SP strategy;
    FlushEngine engine;

    Fixture(uint32_t numThreads, uint32_t idleIntervalMS, SimpleStrategy::SP strategy_)
        : tlsStatsFactory(std::make_shared<SimpleTlsStatsFactory>()),
          strategy(strategy_),
          engine(tlsStatsFactory, strategy, numThreads, idleIntervalMS)
    {
    }

    Fixture(uint32_t numThreads, uint32_t idleIntervalMS)
        : Fixture(numThreads, idleIntervalMS, std::make_shared<SimpleStrategy>())
    {
    }

    std::shared_ptr<SimpleHandler>
    addSimpleHandler(Targets targets)
    {
        auto handler = std::make_shared<SimpleHandler>(targets, "handler", 20);
        engine.putFlushHandler(DocTypeName("handler"), handler);
        engine.start();
        return handler;
    }

    void assertOldestSerial(SimpleHandler &handler, search::SerialNum expOldestSerial)
    {
        using namespace std::chrono_literals;
        for (int pass = 0; pass < 600; ++pass) {
            std::this_thread::sleep_for(100ms);
            if (handler._oldestSerial == expOldestSerial) {
                break;
            }
        }
        EXPECT_EQUAL(expOldestSerial, handler._oldestSerial);
    }
};


TEST_F("require that strategy controls flush target", Fixture(1, IINTERVAL))
{
    vespalib::Gate fooG, barG;
    std::vector<vespalib::string> order;
    FlushTask::UP fooT(new AppendTask("foo", order, fooG));
    FlushTask::UP barT(new AppendTask("bar", order, barG));
    SimpleTarget::SP foo(new SimpleTarget(std::move(fooT), "foo"));
    SimpleTarget::SP bar(new SimpleTarget(std::move(barT), "bar"));
    f.strategy->_targets.push_back(foo);
    f.strategy->_targets.push_back(bar);

    SimpleHandler::SP handler(new SimpleHandler({bar, foo}));
    DocTypeName dtnvanon("anon");
    f.engine.putFlushHandler(dtnvanon, handler);
    f.engine.start();

    EXPECT_TRUE(fooG.await(LONG_TIMEOUT));
    EXPECT_TRUE(barG.await(LONG_TIMEOUT));
    EXPECT_EQUAL(2u, order.size());
    EXPECT_EQUAL("foo", order[0]);
    EXPECT_EQUAL("bar", order[1]);
}

TEST_F("require that zero handlers does not core", Fixture(2, 50))
{
    f.engine.start();
}

TEST_F("require that zero targets does not core", Fixture(2, 50))
{
    DocTypeName dtnvfoo("foo");
    DocTypeName dtnvbar("bar");
    f.engine.putFlushHandler(dtnvfoo,
                             IFlushHandler::SP(new SimpleHandler({}, "foo")));
    f.engine.putFlushHandler(dtnvbar,
                             IFlushHandler::SP(new SimpleHandler({}, "bar")));
    f.engine.start();
}

TEST_F("require that oldest serial is found", Fixture(1, IINTERVAL))
{
    SimpleTarget::SP foo(new SimpleTarget("foo", 10));
    SimpleTarget::SP bar(new SimpleTarget("bar", 20));
    f.strategy->_targets.push_back(foo);
    f.strategy->_targets.push_back(bar);

    SimpleHandler::SP handler(new SimpleHandler({foo, bar}, "anon", 25));
    DocTypeName dtnvanon("anon");
    f.engine.putFlushHandler(dtnvanon, handler);
    f.engine.start();

    EXPECT_TRUE(handler->_done.await(LONG_TIMEOUT));
    EXPECT_EQUAL(25ul, handler->_oldestSerial);
    FlushDoneHistory handlerFlushDoneHistory(handler->getFlushDoneHistory());
    EXPECT_EQUAL(FlushDoneHistory({ 10, 20, 25 }), handlerFlushDoneHistory);
}

TEST_F("require that oldest serial is found in group", Fixture(2, IINTERVAL))
{
    SimpleTarget::SP fooT1(new SimpleTarget("fooT1", 10));
    SimpleTarget::SP fooT2(new SimpleTarget("fooT2", 20));
    SimpleTarget::SP barT1(new SimpleTarget("barT1",  5));
    SimpleTarget::SP barT2(new SimpleTarget("barT2", 15));
    f.strategy->_targets.push_back(fooT1);
    f.strategy->_targets.push_back(fooT2);
    f.strategy->_targets.push_back(barT1);
    f.strategy->_targets.push_back(barT2);

    SimpleHandler::SP fooH(new SimpleHandler({fooT1, fooT2}, "fooH", 25));
    DocTypeName dtnvfoo("foo");
    f.engine.putFlushHandler(dtnvfoo, fooH);

    SimpleHandler::SP barH(new SimpleHandler({barT1, barT2}, "barH", 20));
    DocTypeName dtnvbar("bar");
    f.engine.putFlushHandler(dtnvbar, barH);

    f.engine.start();

    EXPECT_TRUE(fooH->_done.await(LONG_TIMEOUT));
    EXPECT_EQUAL(25ul, fooH->_oldestSerial);
    // [ 10, 25 ], [10, 10, 25], [ 10, 25, 25 ] and [ 10, 20, 25 ] are
    // legal histories
    FlushDoneHistory fooHFlushDoneHistory(fooH->getFlushDoneHistory());
    if (fooHFlushDoneHistory != FlushDoneHistory({ 10, 25 }) &&
        fooHFlushDoneHistory != FlushDoneHistory({ 10, 10, 25 }) &&
        fooHFlushDoneHistory != FlushDoneHistory({ 10, 25, 25 })) {
        EXPECT_EQUAL(FlushDoneHistory({ 10, 20, 25 }), fooHFlushDoneHistory);
    }
    EXPECT_TRUE(barH->_done.await(LONG_TIMEOUT));
    EXPECT_EQUAL(20ul, barH->_oldestSerial);
    // [ 5, 20 ], [5, 5, 20], [ 5, 20, 20 ] and [ 5, 15, 20 ] are
    // legal histories
    FlushDoneHistory barHFlushDoneHistory(barH->getFlushDoneHistory());
    if (barHFlushDoneHistory != FlushDoneHistory({ 5, 20 }) &&
        barHFlushDoneHistory != FlushDoneHistory({ 5, 5, 20 }) &&
        barHFlushDoneHistory != FlushDoneHistory({ 5, 20, 20 })) {
        EXPECT_EQUAL(FlushDoneHistory({ 5, 15, 20 }), barHFlushDoneHistory);
    }
}

TEST_F("require that target can refuse flush", Fixture(2, IINTERVAL))
{
    SimpleTarget::SP target(new SimpleTarget());
    SimpleHandler::SP handler(new SimpleHandler({target}));
    target->_task = searchcorespi::FlushTask::UP();
    DocTypeName dtnvanon("anon");
    f.engine.putFlushHandler(dtnvanon, handler);
    f.engine.start();

    EXPECT_TRUE(target->_initDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(!target->_taskDone.await(SHORT_TIMEOUT));
    EXPECT_TRUE(!handler->_done.await(SHORT_TIMEOUT));
}

TEST_F("require that targets are flushed when nothing new to flush",
       Fixture(2, IINTERVAL))
{
    SimpleTarget::SP target(new SimpleTarget("anon", 5)); // oldest unflushed serial num = 5
    SimpleHandler::SP handler(new SimpleHandler({target}, "anon", 4)); // current serial num = 4
    DocTypeName dtnvanon("anon");
    f.engine.putFlushHandler(dtnvanon, handler);
    f.engine.start();

    EXPECT_TRUE(target->_initDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(target->_taskDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(handler->_done.await(LONG_TIMEOUT));
}

TEST_F("require that flushing targets are skipped", Fixture(2, IINTERVAL))
{
    SimpleTarget::SP foo(new SimpleTarget("foo"));
    SimpleTarget::SP bar(new SimpleTarget("bar"));
    f.strategy->_targets.push_back(foo);
    f.strategy->_targets.push_back(bar);

    SimpleHandler::SP handler(new SimpleHandler({bar, foo}));
    DocTypeName dtnvanon("anon");
    f.engine.putFlushHandler(dtnvanon, handler);
    f.engine.start();

    EXPECT_TRUE(foo->_taskDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(bar->_taskDone.await(LONG_TIMEOUT)); /* this is the key check */
}

TEST_F("require that updated targets are not skipped", Fixture(2, IINTERVAL))
{
    SimpleTarget::SP target(new SimpleTarget("target", 1));
    f.strategy->_targets.push_back(target);

    SimpleHandler::SP handler(new SimpleHandler({target}, "handler", 0));
    DocTypeName dtnvhandler("handler");
    f.engine.putFlushHandler(dtnvhandler, handler);
    f.engine.start();

    EXPECT_TRUE(target->_taskDone.await(LONG_TIMEOUT));
}

TEST("require that threaded target works")
{
    SimpleExecutor executor;
    SimpleGetSerialNum getSerialNum;
    IFlushTarget::SP target(new SimpleTarget());
    target.reset(new ThreadedFlushTarget(executor, getSerialNum, target));

    EXPECT_FALSE(executor._done.await(SHORT_TIMEOUT));
    EXPECT_TRUE(target->initFlush(0).get() != NULL);
    EXPECT_TRUE(executor._done.await(LONG_TIMEOUT));
}

TEST("require that cached target works")
{
    IFlushTarget::SP target(new AssertedTarget());
    target.reset(new CachedFlushTarget(target));
    for (uint32_t i = 0; i < 2; ++i) {
        EXPECT_EQUAL(0l, target->getApproxMemoryGain().getBefore());
        EXPECT_EQUAL(0l, target->getApproxMemoryGain().getAfter());
        EXPECT_EQUAL(0ul, target->getFlushedSerialNum());
    }
}

TEST_F("require that trigger flush works", Fixture(2, IINTERVAL))
{
    SimpleTarget::SP target(new SimpleTarget("target", 1));
    f.strategy->_targets.push_back(target);

    SimpleHandler::SP handler(new SimpleHandler({target}, "handler", 9));
    DocTypeName dtnvhandler("handler");
    f.engine.putFlushHandler(dtnvhandler, handler);
    f.engine.start();
    f.engine.triggerFlush();
    EXPECT_TRUE(target->_initDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(target->_taskDone.await(LONG_TIMEOUT));
}

bool
asserCorrectHandlers(const FlushEngine::FlushMetaSet & current1, const std::vector<const char *> & targets)
{
    bool retval(targets.size() == current1.size());
    FlushEngine::FlushMetaSet::const_iterator curr(current1.begin());
    if (retval) {
        for (const char * target : targets) {
            if (target != (curr++)->getName()) {
                return false;
            }
        }
    }
    return retval;
}

void
assertThatHandlersInCurrentSet(FlushEngine & engine, const std::vector<const char *> & targets)
{
    FlushEngine::FlushMetaSet current1 = engine.getCurrentlyFlushingSet();
    while ((current1.size() < targets.size()) || !asserCorrectHandlers(current1, targets)) {
        FastOS_Thread::Sleep(1);
        current1 = engine.getCurrentlyFlushingSet();
    }
}

TEST_F("require that concurrency works", Fixture(2, 1))
{
    SimpleTarget::SP target1(new SimpleTarget("target1", 1, false));
    SimpleTarget::SP target2(new SimpleTarget("target2", 2, false));
    SimpleTarget::SP target3(new SimpleTarget("target3", 3, false));
    SimpleHandler::SP handler(new SimpleHandler({target1, target2, target3}, "handler", 9));
    DocTypeName dtnvhandler("handler");
    f.engine.putFlushHandler(dtnvhandler, handler);
    f.engine.start();
    EXPECT_TRUE(target1->_initDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(target2->_initDone.await(LONG_TIMEOUT));
    EXPECT_TRUE(!target3->_initDone.await(SHORT_TIMEOUT));
    assertThatHandlersInCurrentSet(f.engine, {"handler.target1", "handler.target2"});
    EXPECT_TRUE(!target3->_initDone.await(SHORT_TIMEOUT));
    target1->_proceed.countDown();
    EXPECT_TRUE(target1->_taskDone.await(LONG_TIMEOUT));
    assertThatHandlersInCurrentSet(f.engine, {"handler.target2", "handler.target3"});
    target3->_proceed.countDown();
    target2->_proceed.countDown();
}

TEST_F("require that state explorer can list flush targets", Fixture(1, 1))
{
    SimpleTarget::SP target = std::make_shared<SimpleTarget>("target1", 100, false);
    f.engine.putFlushHandler(DocTypeName("handler"),
            std::make_shared<SimpleHandler>(
                    Targets({target, std::make_shared<SimpleTarget>("target2", 50, true)}),
                             "handler", 9));
    f.engine.start();
    target->_initDone.await(LONG_TIMEOUT);
    target->_taskStart.await(LONG_TIMEOUT);

    FlushEngineExplorer explorer(f.engine);
    Slime state;
    SlimeInserter inserter(state);
    explorer.get_state(inserter, true);

    Inspector &all = state.get()["allTargets"];
    EXPECT_EQUAL(2u, all.children());
    EXPECT_EQUAL("handler.target2", all[0]["name"].asString().make_string());
    EXPECT_EQUAL(50, all[0]["flushedSerialNum"].asLong());
    EXPECT_EQUAL("handler.target1", all[1]["name"].asString().make_string());
    EXPECT_EQUAL(100, all[1]["flushedSerialNum"].asLong());

    Inspector &flushing = state.get()["flushingTargets"];
    EXPECT_EQUAL(1u, flushing.children());
    EXPECT_EQUAL("handler.target1", flushing[0]["name"].asString().make_string());

    target->_proceed.countDown();
    target->_taskDone.await(LONG_TIMEOUT);
}

TEST_F("require that oldest serial is updated when closing engine", Fixture(1, 100))
{
    auto target1 = std::make_shared<SimpleTarget>("target1", 10, false);
    auto handler = f.addSimpleHandler({ target1 });
    TEST_DO(f.assertOldestSerial(*handler, 10));
    target1->_proceed.countDown();
    f.engine.close();
    EXPECT_EQUAL(20u, handler->_oldestSerial);
}

TEST_F("require that oldest serial is updated when finishing priority flush strategy", Fixture(1, 100, std::make_shared<NoFlushStrategy>()))
{
    auto target1 = std::make_shared<SimpleTarget>("target1", 10, true);
    auto handler = f.addSimpleHandler({ target1 });
    TEST_DO(f.assertOldestSerial(*handler, 10));
    f.engine.setStrategy(std::make_shared<SimpleStrategy>());
    EXPECT_EQUAL(20u, handler->_oldestSerial);
}


TEST_MAIN()
{
    TEST_RUN_ALL();
}
