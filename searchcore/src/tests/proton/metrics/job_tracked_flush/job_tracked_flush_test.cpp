// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/log/log.h>
LOG_SETUP("job_tracked_flush_test");

#include <vespa/searchcore/proton/metrics/job_tracked_flush_target.h>
#include <vespa/searchcore/proton/metrics/job_tracked_flush_task.h>
#include <vespa/searchcore/proton/test/dummy_flush_target.h>
#include <vespa/searchcore/proton/test/simple_job_tracker.h>
#include <vespa/vespalib/testkit/testapp.h>
#include <vespa/vespalib/util/closuretask.h>
#include <vespa/vespalib/util/threadstackexecutor.h>
#include <vespa/vespalib/util/gate.h>

using namespace proton;
using namespace searchcorespi;
using search::SerialNum;
using test::SimpleJobTracker;
using vespalib::makeTask;
using vespalib::makeClosure;
using vespalib::Gate;
using vespalib::ThreadStackExecutor;

struct MyFlushTask : public searchcorespi::FlushTask
{
    Gate &_execGate;
    MyFlushTask(Gate &execGate) : _execGate(execGate) {}

    // Implements searchcorespi::FlushTask
    virtual void run() override {
        _execGate.await(5000);
    }
    virtual search::SerialNum getFlushSerial() const override { return 5; }
};

struct MyFlushTarget : public test::DummyFlushTarget
{
    typedef std::shared_ptr<MyFlushTarget> SP;
    SerialNum _initFlushSerial;
    Gate _execGate;
    Gate _initGate;
    MyFlushTarget()
        : test::DummyFlushTarget("mytarget", Type::FLUSH, Component::OTHER),
          _initFlushSerial(0),
          _execGate(),
          _initGate()
    {}

    // Implements searchcorespi::IFlushTarget
    virtual FlushTask::UP initFlush(SerialNum currentSerial) override {
        if (currentSerial > 0) {
            _initFlushSerial = currentSerial;
            _initGate.await(5000);
            return FlushTask::UP(new MyFlushTask(_execGate));
        }
        return FlushTask::UP();
    }
};

struct Fixture
{
    SimpleJobTracker::SP _tracker;
    MyFlushTarget::SP _target;
    JobTrackedFlushTarget _trackedFlush;
    FlushTask::UP _task;
    Gate _taskGate;
    ThreadStackExecutor _exec;
    Fixture(uint32_t numJobTrackings = 1)
        : _tracker(new SimpleJobTracker(numJobTrackings)),
          _target(new MyFlushTarget()),
          _trackedFlush(_tracker, _target),
          _task(),
          _taskGate(),
          _exec(1, 64000)
    {
    }
    void initFlush(SerialNum currentSerial) {
        _task = _trackedFlush.initFlush(currentSerial);
        _taskGate.countDown();
    }
};

constexpr SerialNum FLUSH_SERIAL = 10;

TEST_F("require that flush target name, type and component is preserved", Fixture)
{
    EXPECT_EQUAL("mytarget", f._trackedFlush.getName());
    EXPECT_TRUE(IFlushTarget::Type::FLUSH == f._trackedFlush.getType());
    EXPECT_TRUE(IFlushTarget::Component::OTHER == f._trackedFlush.getComponent());
}

TEST_F("require that flush task init is tracked", Fixture)
{
    EXPECT_EQUAL(1u, f._tracker->_started.getCount());
    EXPECT_EQUAL(1u, f._tracker->_ended.getCount());

    f._exec.execute(makeTask(makeClosure(&f, &Fixture::initFlush, FLUSH_SERIAL)));
    f._tracker->_started.await(5000);
    EXPECT_EQUAL(0u, f._tracker->_started.getCount());
    EXPECT_EQUAL(1u, f._tracker->_ended.getCount());

    f._target->_initGate.countDown();
    f._taskGate.await(5000);
    EXPECT_EQUAL(0u, f._tracker->_ended.getCount());
    {
        JobTrackedFlushTask *trackedTask = dynamic_cast<JobTrackedFlushTask *>(f._task.get());
        EXPECT_TRUE(trackedTask != nullptr);
        EXPECT_EQUAL(5u, trackedTask->getFlushSerial());
    }
    EXPECT_EQUAL(FLUSH_SERIAL, f._target->_initFlushSerial);
}

TEST_F("require that flush task execution is tracked", Fixture(2))
{
    f._exec.execute(makeTask(makeClosure(&f, &Fixture::initFlush, FLUSH_SERIAL)));
    f._target->_initGate.countDown();
    f._taskGate.await(5000);

    EXPECT_EQUAL(1u, f._tracker->_started.getCount());
    EXPECT_EQUAL(1u, f._tracker->_ended.getCount());

    f._exec.execute(std::move(f._task));
    f._tracker->_started.await(5000);
    EXPECT_EQUAL(0u, f._tracker->_started.getCount());
    EXPECT_EQUAL(1u, f._tracker->_ended.getCount());

    f._target->_execGate.countDown();
    f._tracker->_ended.await(5000);
    EXPECT_EQUAL(0u, f._tracker->_ended.getCount());
}

TEST_F("require that nullptr flush task is not tracked", Fixture)
{
    FlushTask::UP task = f._trackedFlush.initFlush(0);
    EXPECT_TRUE(task.get() == nullptr);
}

TEST_MAIN() { TEST_RUN_ALL(); }
