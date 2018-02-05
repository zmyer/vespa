// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include "sequencer.h"
#include "tracelevel.h"
#include <vespa/vespalib/util/stringfmt.h>

using vespalib::make_string;

namespace mbus {

Sequencer::Sequencer(IMessageHandler &sender) :
    _lock(),
    _sender(sender),
    _seqMap()
{
    // empty
}

Sequencer::~Sequencer()
{
    for (QueueMap::iterator it = _seqMap.begin(); it != _seqMap.end(); ++it) {
        MessageQueue *queue = it->second;
        if (queue != nullptr) {
            while (queue->size() > 0) {
                Message *msg = queue->front();
                queue->pop();
                msg->discard();
                delete msg;
            }
            delete queue;
        }
    }
}

Message::UP
Sequencer::filter(Message::UP msg)
{
    uint64_t seqId = msg->getSequenceId();
    msg->setContext(Context(seqId));
    {
        vespalib::LockGuard guard(_lock);
        QueueMap::iterator it = _seqMap.find(seqId);
        if (it != _seqMap.end()) {
            if (it->second == nullptr) {
                it->second = new MessageQueue();
            }
            msg->getTrace().trace(TraceLevel::COMPONENT,
                                  make_string("Sequencer queued message with sequence id '%" PRIu64 "'.", seqId));
            it->second->push(msg.get());
            msg.release();
            return Message::UP();
        }
        _seqMap[seqId] = nullptr; // insert empty queue
    }
    return std::move(msg);
}

void
Sequencer::sequencedSend(Message::UP msg)
{
    msg->getTrace().trace(TraceLevel::COMPONENT,
                          make_string("Sequencer sending message with sequence id '%" PRIu64 "'.",
                                      msg->getContext().value.UINT64));
    msg->pushHandler(*this);
    _sender.handleMessage(std::move(msg));
}

void
Sequencer::handleMessage(Message::UP msg)
{
    if (msg->hasSequenceId()) {
        msg = filter(std::move(msg));
        if (msg.get() != nullptr) {
            sequencedSend(std::move(msg));
        }
    } else {
        _sender.handleMessage(std::move(msg)); // unsequenced
    }
}

void
Sequencer::handleReply(Reply::UP reply)
{
    uint64_t seq = reply->getContext().value.UINT64;
    reply->getTrace().trace(TraceLevel::COMPONENT,
                            make_string("Sequencer received reply with sequence id '%" PRIu64 "'.", seq));
    Message::UP msg;
    {
        vespalib::LockGuard guard(_lock);
        QueueMap::iterator it = _seqMap.find(seq);
        MessageQueue *que = it->second;
        assert(it != _seqMap.end());
        if (que == nullptr || que->size() == 0) {
            if (que != nullptr) {
                delete que;
            }
            _seqMap.erase(it);
        } else {
            msg.reset(que->front());
            que->pop();
        }
    }
    if (msg.get() != nullptr) {
        sequencedSend(std::move(msg));
    }
    IReplyHandler &handler = reply->getCallStack().pop(*reply);
    handler.handleReply(std::move(reply));
}

} // namespace mbus
