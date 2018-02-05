// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/test_kit.h>
#include <vespa/fnet/fnet.h>
#include "dummy.h"

TEST("lock speed") {
  FastOS_Time      start;
  FastOS_Time       stop;
  DummyLock        dummy;
  std::mutex        lock;
  double       dummyTime;
  double      actualTime;
  double        overhead;
  uint32_t             i;

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
    dummy.Lock();
    dummy.Unlock();
  }
  stop.SetNow();
  stop -= start;
  dummyTime = stop.MilliSecs();

  fprintf(stderr,
          "10M dummy lock/unlock: %f ms (%1.2f/ms)\n",
          dummyTime, 10000000.0 / dummyTime);

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
  }
  stop.SetNow();
  stop -= start;
  actualTime = stop.MilliSecs();

  fprintf(stderr,
          "10M actual lock/unlock: %f ms (%1.2f/ms)\n",
          stop.MilliSecs(), 10000000.0 / stop.MilliSecs());

  overhead = (actualTime - dummyTime) / 10000.0;

  fprintf(stderr,
          "approx overhead per lock/unlock: %f microseconds\n",
          overhead);

  //---------------------------------------------------------------------------

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
      std::mutex lock0;
      std::mutex lock1;
      std::mutex lock2;
      std::mutex lock3;
      std::mutex lock4;
      std::mutex lock5;
      std::mutex lock6;
      std::mutex lock7;
      std::mutex lock8;
      std::mutex lock9;
  }
  stop.SetNow();
  stop -= start;
  fprintf(stderr, "10M mutex create/destroy %f ms (%1.2f/ms)\n",
      stop.MilliSecs(), 10000000.0 / stop.MilliSecs());

  //---------------------------------------------------------------------------

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
      std::condition_variable cond0;
      std::condition_variable cond1;
      std::condition_variable cond2;
      std::condition_variable cond3;
      std::condition_variable cond4;
      std::condition_variable cond5;
      std::condition_variable cond6;
      std::condition_variable cond7;
      std::condition_variable cond8;
      std::condition_variable cond9;
  }
  stop.SetNow();
  stop -= start;
  fprintf(stderr, "10M cond create/destroy %f ms (%1.2f/ms)\n",
      stop.MilliSecs(), 10000000.0 / stop.MilliSecs());

  //---------------------------------------------------------------------------

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
      DummyObj dummy0;
      DummyObj dummy1;
      DummyObj dummy2;
      DummyObj dummy3;
      DummyObj dummy4;
      DummyObj dummy5;
      DummyObj dummy6;
      DummyObj dummy7;
      DummyObj dummy8;
      DummyObj dummy9;
  }
  stop.SetNow();
  stop -= start;
  fprintf(stderr, "10M dummy create/destroy %f ms (%1.2f/ms)\n",
      stop.MilliSecs(), 10000000.0 / stop.MilliSecs());

  //---------------------------------------------------------------------------

  start.SetNow();
  for (i = 0; i < 1000000; i++) {
      DummyObj *dummy0 = new DummyObj();
      DummyObj *dummy1 = new DummyObj();
      DummyObj *dummy2 = new DummyObj();
      DummyObj *dummy3 = new DummyObj();
      DummyObj *dummy4 = new DummyObj();
      DummyObj *dummy5 = new DummyObj();
      DummyObj *dummy6 = new DummyObj();
      DummyObj *dummy7 = new DummyObj();
      DummyObj *dummy8 = new DummyObj();
      DummyObj *dummy9 = new DummyObj();
      delete dummy9;
      delete dummy8;
      delete dummy7;
      delete dummy6;
      delete dummy5;
      delete dummy4;
      delete dummy3;
      delete dummy2;
      delete dummy1;
      delete dummy0;
  }
  stop.SetNow();
  stop -= start;
  fprintf(stderr, "10M dummy new/delete %f ms (%1.2f/ms)\n",
          stop.MilliSecs(), 10000000.0 / stop.MilliSecs());

  //---------------------------------------------------------------------------
}

TEST_MAIN() { TEST_RUN_ALL(); }
