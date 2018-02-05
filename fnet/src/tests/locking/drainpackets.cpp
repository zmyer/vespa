// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/test_kit.h>
#include <vespa/fnet/fnet.h>
#include <mutex>

class MyPacket : public FNET_Packet
{
public:
  uint32_t  GetPCODE() override { return 0; }
  uint32_t GetLength() override { return 0; }
  void Encode(FNET_DataBuffer *) override {}
  bool Decode(FNET_DataBuffer *, uint32_t) override { return true; }
};


TEST("drain packets") {
  FastOS_Time     start;
  FastOS_Time     stop;

  std::mutex      lock;

  FNET_PacketQueue   q1(512);
  FNET_PacketQueue   q2(512);
  FNET_PacketQueue   q3(512);

  int        i;

  // create dummy packets

  for (i = 0; i < 500; i++) {
    q1.QueuePacket_NoLock(new MyPacket(), FNET_Context());
  }

  // drain packets directly with single lock interval

  start.SetNow();

  for (i = 0; i < 10000; i++) {

    FNET_Packet  *packet;
    FNET_Context  context;

    {
        std::lock_guard<std::mutex> guard(lock);
        while (!q1.IsEmpty_NoLock()) {
            packet = q1.DequeuePacket_NoLock(&context);
            q3.QueuePacket_NoLock(packet, context);
        }
    }

    //------------------------

    {
        std::lock_guard<std::mutex> guard(lock);
        while (!q3.IsEmpty_NoLock()) {
            packet = q3.DequeuePacket_NoLock(&context);
            q1.QueuePacket_NoLock(packet, context);
        }
    }
  }

  stop.SetNow();
  stop -= start;
  fprintf(stderr, "direct, single lock interval (10M packets): %1.2f ms\n",
          stop.MilliSecs());

  // flush packets, then move without lock

  start.SetNow();

  for (i = 0; i < 10000; i++) {

    FNET_Packet  *packet;
    FNET_Context  context;

    {
        std::lock_guard<std::mutex> guard(lock);
        q1.FlushPackets_NoLock(&q2);
    }

    while (!q2.IsEmpty_NoLock()) {
      packet = q2.DequeuePacket_NoLock(&context);
      q3.QueuePacket_NoLock(packet, context);
    }

    //------------------------

    {
        std::lock_guard<std::mutex> guard(lock);
        q3.FlushPackets_NoLock(&q2);
    }

    while (!q2.IsEmpty_NoLock()) {
      packet = q2.DequeuePacket_NoLock(&context);
      q1.QueuePacket_NoLock(packet, context);
    }
  }

  stop.SetNow();
  stop -= start;
  fprintf(stderr, "indirect (10M packets): %1.2f ms\n", stop.MilliSecs());

  // drain packets directly with multiple lock intervals

  start.SetNow();

  for (i = 0; i < 10000; i++) {

    FNET_Packet  *packet;
    FNET_Context  context;

    while ((packet = q1.DequeuePacket(0, &context)) != nullptr) {
      q3.QueuePacket_NoLock(packet, context);
    }

    //------------------------

    while ((packet = q3.DequeuePacket(0, &context)) != nullptr) {
      q1.QueuePacket_NoLock(packet, context);
    }
  }

  stop.SetNow();
  stop -= start;
  fprintf(stderr, "direct, multiple lock intervals (10M packets): %1.2f ms\n",
          stop.MilliSecs());

  EXPECT_TRUE(q1.GetPacketCnt_NoLock() == 500 &&
         q2.GetPacketCnt_NoLock() == 0 &&
         q3.GetPacketCnt_NoLock() == 0);
}

TEST_MAIN() { TEST_RUN_ALL(); }
