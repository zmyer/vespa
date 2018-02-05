// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <vespa/vespalib/testkit/testapp.h>
#include <vespa/vespalib/util/sync.h>

using namespace vespalib;

#define CHECK_LOCKED(m) { TryLock tl(m); EXPECT_TRUE(!tl.hasLock()); }
#define CHECK_UNLOCKED(m) { TryLock tl(m); EXPECT_TRUE(tl.hasLock()); }

class Test : public TestApp
{
private:
    Lock    _lock;
    Monitor _monitor;

    LockGuard    lockLock()      { return LockGuard(_lock); }
    LockGuard    lockMonitor()   { return LockGuard(_monitor); }
    MonitorGuard obtainMonitor() { return MonitorGuard(_monitor); }
public:
    ~Test();
    void testCountDownLatch();
    int Main() override;
};

Test::~Test() {}
void
Test::testCountDownLatch() {
    {
        CountDownLatch latch(5);
        EXPECT_EQUAL(latch.getCount(), 5u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 4u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 3u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 2u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 1u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 0u);
        latch.countDown();
        EXPECT_EQUAL(latch.getCount(), 0u);
        latch.await(); // should not block
        latch.await(); // should not block
    }
    {
        Gate gate;
        EXPECT_EQUAL(gate.getCount(), 1u);
        gate.countDown();
        EXPECT_EQUAL(gate.getCount(), 0u);
        gate.countDown();
        EXPECT_EQUAL(gate.getCount(), 0u);
        gate.await(); // should not block
        gate.await(); // should not block
    }
    {
        Gate gate;
        EXPECT_EQUAL(gate.getCount(), 1u);
        EXPECT_EQUAL(gate.await(0), false);
        EXPECT_EQUAL(gate.await(10), false);
        gate.countDown();
        EXPECT_EQUAL(gate.getCount(), 0u);
        EXPECT_EQUAL(gate.await(0), true);
        EXPECT_EQUAL(gate.await(10), true);
    }
}

int
Test::Main()
{
    TEST_INIT("sync_test");
    {
        Lock lock;
        {
            CHECK_UNLOCKED(lock);
            LockGuard guard(lock);
            CHECK_LOCKED(lock);
        }
        CHECK_UNLOCKED(lock);
        {
            LockGuard guard(lock);
            CHECK_LOCKED(lock);
            guard.unlock();
            CHECK_UNLOCKED(lock);
        }
    }
    // you can use a LockGuard to lock a Monitor
    {
        Monitor monitor;
        {
            CHECK_UNLOCKED(monitor);
            LockGuard guard(monitor);
            CHECK_LOCKED(monitor);
        }
        CHECK_UNLOCKED(monitor);
        {
            LockGuard guard(monitor);
            CHECK_LOCKED(monitor);
            guard.unlock();
            CHECK_UNLOCKED(monitor);
        }
    }
    {
        Monitor monitor;
        {
            CHECK_UNLOCKED(monitor);
            MonitorGuard guard(monitor);
            guard.signal();
            guard.broadcast();
            guard.wait(10);
            CHECK_LOCKED(monitor);
        }
        CHECK_UNLOCKED(monitor);
        {
            MonitorGuard guard(monitor);
            CHECK_LOCKED(monitor);
            guard.unlock();
            CHECK_UNLOCKED(monitor);
        }
    }
    // copy/assign is nop, but legal
    {
        Lock a;
        Lock b(a);
        b = a;
    }
    {
        Monitor a;
        Monitor b(a);
        b = a;
    }
    // you can lock const objects
    {
        const Lock lock;
        CHECK_UNLOCKED(lock);
        LockGuard guard(lock);
        CHECK_LOCKED(lock);
    }
    {
        const Monitor lock;
        CHECK_UNLOCKED(lock);
        LockGuard guard(lock);
        CHECK_LOCKED(lock);
    }
    {
        const Monitor monitor;
        CHECK_UNLOCKED(monitor);
        MonitorGuard guard(monitor);
        CHECK_LOCKED(monitor);
    }
    // TryLock hands the lock over to a LockGuard/MonitorGuard
    {
        Lock    lock;
        CHECK_UNLOCKED(lock);
        TryLock a(lock);
        CHECK_LOCKED(lock);
        if (a.hasLock()) {
            LockGuard guard(std::move(a));
            CHECK_LOCKED(lock);
        }
        CHECK_UNLOCKED(lock);
    }
    {
        Monitor mon;
        CHECK_UNLOCKED(mon);
        TryLock a(mon);
        CHECK_LOCKED(mon);
        if (a.hasLock()) {
            LockGuard guard(std::move(a));
            CHECK_LOCKED(mon);
        }
        CHECK_UNLOCKED(mon);
    }
    {
        Monitor mon;
        CHECK_UNLOCKED(mon);
        TryLock a(mon);
        CHECK_LOCKED(mon);
        if (a.hasLock()) {
            MonitorGuard guard(std::move(a));
            CHECK_LOCKED(mon);
        }
        CHECK_UNLOCKED(mon);
    }
    {
        Lock lock;

        CHECK_UNLOCKED(lock);
        TryLock a(lock);
        CHECK_LOCKED(lock);
        TryLock b(lock);
        CHECK_LOCKED(lock);

        EXPECT_TRUE(a.hasLock());
        EXPECT_TRUE(!b.hasLock());
        {
            CHECK_LOCKED(lock);
            EXPECT_TRUE(a.hasLock());
            LockGuard guard(std::move(a));
            EXPECT_TRUE(!a.hasLock());
            CHECK_LOCKED(lock);
        }
        CHECK_UNLOCKED(lock);
    }
    // TryLock will unlock when exiting scope if lock was not passed on
    {
        Lock    lock;
        Monitor mon;
        CHECK_UNLOCKED(lock);
        CHECK_UNLOCKED(mon);
        {
            TryLock a(lock);
            EXPECT_TRUE(a.hasLock());
            TryLock b(mon);
            EXPECT_TRUE(b.hasLock());
            CHECK_LOCKED(lock);
            CHECK_LOCKED(mon);
        }
        CHECK_UNLOCKED(lock);
        CHECK_UNLOCKED(mon);
    }
    // TryLock explicitt unlock of lock
    {
        Lock    lock;
        TryLock tl(lock);
        EXPECT_TRUE(tl.hasLock());
        tl.unlock();
        EXPECT_FALSE(tl.hasLock());
        tl.unlock();
        EXPECT_FALSE(tl.hasLock());
    }
    // TryLock explicitt unlock of monitor
    {
        Monitor    lock;
        TryLock tl(lock);
        EXPECT_TRUE(tl.hasLock());
        tl.unlock();
        EXPECT_FALSE(tl.hasLock());
        tl.unlock();
        EXPECT_FALSE(tl.hasLock());
    }
    // LockGuard/MonitorGuard have destructive move
    {
        Lock lock;
        CHECK_UNLOCKED(lock);
        LockGuard a(lock);
        CHECK_LOCKED(lock);
        {
            CHECK_LOCKED(lock);
            LockGuard b(std::move(a));
            CHECK_LOCKED(lock);
        }
        CHECK_UNLOCKED(lock);
    }
    {
        Monitor mon;
        CHECK_UNLOCKED(mon);
        MonitorGuard a(mon);
        CHECK_LOCKED(mon);
        {
            CHECK_LOCKED(mon);
            MonitorGuard b(std::move(a));
            CHECK_LOCKED(mon);
        }
        CHECK_UNLOCKED(mon);
    }
    // Destructive copy also works for return value handover
    {
        CHECK_UNLOCKED(_lock);
        CHECK_UNLOCKED(_monitor);
        {
            CHECK_UNLOCKED(_lock);
            CHECK_UNLOCKED(_monitor);
            LockGuard a(lockLock());
            CHECK_LOCKED(_lock);
            CHECK_UNLOCKED(_monitor);
            LockGuard b = lockMonitor(); // copy, not assign
            CHECK_LOCKED(_lock);
            CHECK_LOCKED(_monitor);
        }
        CHECK_UNLOCKED(_lock);
        CHECK_UNLOCKED(_monitor);
    }
    {
        CHECK_UNLOCKED(_monitor);
        {
            CHECK_UNLOCKED(_monitor);
            MonitorGuard guard(obtainMonitor());
            CHECK_LOCKED(_monitor);
        }
        CHECK_UNLOCKED(_monitor);
    }
    // Test that guards can be matched to locks/monitors
    {
        Lock lock1;
        Lock lock2;
        LockGuard lockGuard1(lock1);
        LockGuard lockGuard2(lock2);
        EXPECT_TRUE(lockGuard1.locks(lock1));
        EXPECT_FALSE(lockGuard1.locks(lock2));
        EXPECT_TRUE(lockGuard2.locks(lock2));
        EXPECT_FALSE(lockGuard2.locks(lock1));
        lockGuard1.unlock();
        EXPECT_FALSE(lockGuard1.locks(lock1));
    }
    {
        Monitor lock1;
        Monitor lock2;
        MonitorGuard lockGuard1(lock1);
        MonitorGuard lockGuard2(lock2);
        EXPECT_TRUE(lockGuard1.monitors(lock1));
        EXPECT_FALSE(lockGuard1.monitors(lock2));
        EXPECT_TRUE(lockGuard2.monitors(lock2));
        EXPECT_FALSE(lockGuard2.monitors(lock1));
        lockGuard1.unlock();
        EXPECT_FALSE(lockGuard1.monitors(lock1));
    }
    testCountDownLatch();
    TEST_DONE();
}

TEST_APPHOOK(Test)
