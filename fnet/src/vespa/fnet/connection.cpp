// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "connection.h"
#include "dummypacket.h"
#include "channel.h"
#include "controlpacket.h"
#include "ipacketstreamer.h"
#include "iserveradapter.h"
#include "config.h"
#include "transport_thread.h"
#include "transport.h"

#include <vespa/log/log.h>
LOG_SETUP(".fnet");

namespace {
class SyncPacket : public FNET_DummyPacket {
private:
    std::mutex              _lock;
    std::condition_variable _cond;
    bool _done;
    bool _waiting;

public:
    SyncPacket()
            : _lock(),
              _cond(),
              _done(false),
              _waiting(false) {}

    ~SyncPacket() {}

    void WaitFree() {
        std::unique_lock<std::mutex> guard(_lock);
        _waiting = true;
        while (!_done)
            _cond.wait(guard);
        _waiting = false;
    }

    void Free() override;
};


void
SyncPacket::Free()
{
    std::lock_guard<std::mutex> guard(_lock);
    _done = true;
    if (_waiting) {
        _cond.notify_one();
    }
}
}


FNET_Connection::ResolveHandler::ResolveHandler(FNET_Connection *conn)
    : connection(conn),
      address()
{
    connection->AddRef();
}

void
FNET_Connection::ResolveHandler::handle_result(vespalib::SocketAddress result)
{
    address = result;
    connection->Owner()->Add(connection);
}

FNET_Connection::ResolveHandler::~ResolveHandler()
{
    connection->SubRef();
}


///////////////////////
// PROTECTED METHODS //
///////////////////////

const char*
FNET_Connection::GetStateString(State state)
{
    switch(state) {
    case FNET_CONNECTING:
        return "CONNECTING";
    case FNET_CONNECTED:
        return "CONNECTED";
    case FNET_CLOSING:
        return "CLOSING";
    case FNET_CLOSED:
        return "CLOSED";
    default:
        return "ILLEGAL";
    }
}

void
FNET_Connection::SetState(State state)
{
    State         oldstate;

    std::vector<FNET_Channel::UP> toDelete;
    std::unique_lock<std::mutex> guard(_ioc_lock);
    oldstate = _state;
    _state = state;
    if (LOG_WOULD_LOG(debug) && state != oldstate) {
        LOG(debug, "Connection(%s): State transition: %s -> %s", GetSpec(),
            GetStateString(oldstate), GetStateString(state));
    }
    if (oldstate < FNET_CLOSING && state >= FNET_CLOSING) {

        if (_flags._writeLock) {
            _flags._discarding = true;
            while (_flags._writeLock)
                _ioc_cond.wait(guard);
            _flags._discarding = false;
        }

        while (!_queue.IsEmpty_NoLock() || !_myQueue.IsEmpty_NoLock()) {
            _flags._discarding = true;
            _queue.FlushPackets_NoLock(&_myQueue);
            guard.unlock();
            _myQueue.DiscardPackets_NoLock();
            guard.lock();
            _flags._discarding = false;
        }

        BeforeCallback(guard, nullptr);
        toDelete = _channels.Broadcast(&FNET_ControlPacket::ChannelLost);
        AfterCallback(guard);
    }

    if ( ! toDelete.empty() ) {
        FNET_Channel *ach        = _adminChannel;

        for (const FNET_Channel::UP & ch : toDelete) {
            if (ch.get() == ach) {
                _adminChannel = nullptr;
            } else {
                SubRef_NoLock();
            }
        }
    }
}


void
FNET_Connection::HandlePacket(uint32_t plen, uint32_t pcode,
                              uint32_t chid)
{
    FNET_Packet *packet;
    FNET_Channel *channel;
    FNET_IPacketHandler::HP_RetCode hp_rc;

    std::unique_lock<std::mutex> guard(_ioc_lock);
    channel = _channels.Lookup(chid);

    if (channel != nullptr) { // deliver packet on open channel
        channel->prefetch(); // Prefetch in the shadow of the lock operation in BeforeCallback.
        __builtin_prefetch(&_streamer);
        __builtin_prefetch(&_input);

        BeforeCallback(guard, channel);
        __builtin_prefetch(channel->GetHandler(), 0);  // Prefetch the handler while packet is being decoded.
        packet = _streamer->Decode(&_input, plen, pcode, channel->GetContext());
        hp_rc = (packet != nullptr) ? channel->Receive(packet)
                : channel->Receive(&FNET_ControlPacket::BadPacket);
        AfterCallback(guard);

        FNET_Channel::UP toDelete;
        if (hp_rc > FNET_IPacketHandler::FNET_KEEP_CHANNEL) {
            _channels.Unregister(channel);

            if (hp_rc == FNET_IPacketHandler::FNET_FREE_CHANNEL) {
                if (channel == _adminChannel) {
                    _adminChannel = nullptr;
                } else {
                    SubRef_NoLock();
                }
                toDelete.reset(channel);
            }
        }
    } else if (CanAcceptChannels() && IsFromPeer(chid)) { // open new channel
        FNET_Channel::UP newChannel(new FNET_Channel(chid, this));
        channel = newChannel.get();
        AddRef_NoLock();
        BeforeCallback(guard, channel);

        if (_serverAdapter->InitChannel(channel, pcode)) {

            packet = _streamer->Decode(&_input, plen, pcode, channel->GetContext());
            hp_rc = (packet != nullptr) ? channel->Receive(packet)
                    : channel->Receive(&FNET_ControlPacket::BadPacket);
            AfterCallback(guard);

            if (hp_rc == FNET_IPacketHandler::FNET_FREE_CHANNEL) {
                SubRef_NoLock();
            } else if (hp_rc == FNET_IPacketHandler::FNET_KEEP_CHANNEL) {
                _channels.Register(newChannel.release());
            } else {
                newChannel.release(); // It has already been taken care of, so we should not free it here.
            }
        } else {

            AfterCallback(guard);
            SubRef_NoLock();
            guard.unlock();

            LOG(debug, "Connection(%s): channel init failed", GetSpec());
            _input.DataToDead(plen);
        }

    } else { // skip unhandled packet

        guard.unlock();
        LOG(spam, "Connection(%s): skipping unhandled packet", GetSpec());
        _input.DataToDead(plen);
    }
}


bool
FNET_Connection::Read()
{
    uint32_t readData    = 0;     // total data read
    uint32_t readPackets = 0;     // total packets read
    int      readCnt     = 0;     // read count
    bool     broken      = false; // is this conn broken ?
    ssize_t  res;                 // single read result

    _input.EnsureFree(FNET_READ_SIZE);
    res = _socket.read(_input.GetFree(), _input.GetFreeLen());
    readCnt++;

    while (res > 0) {
        _input.FreeToData((uint32_t)res);
        readData += (uint32_t)res;

        for (;;) { // handle each complete packet in the buffer.

            if (!_flags._gotheader)
                _flags._gotheader = _streamer->GetPacketInfo(&_input, &_packetLength,
                        &_packetCode, &_packetCHID,
                        &broken);

            if (_flags._gotheader && _input.GetDataLen() >= _packetLength) {
                readPackets++;
                HandlePacket(_packetLength, _packetCode, _packetCHID);
                _flags._gotheader = false; // reset header flag.
            } else {
                if (broken)
                    goto done_read;
                break;
            }
        }
        _input.resetIfEmpty();

        if (_input.GetFreeLen() > 0
            || readCnt >= FNET_READ_REDO) // prevent starvation
            goto done_read;

        _input.EnsureFree(FNET_READ_SIZE);
        res = _socket.read(_input.GetFree(), _input.GetFreeLen());
        readCnt++;
    }

done_read:

    if (readData > 0) {
        UpdateTimeOut();
        CountDataRead(readData);
        CountPacketRead(readPackets);
        uint32_t maxSize = GetConfig()->_maxInputBufferSize;
        if (maxSize > 0 && _input.GetBufSize() > maxSize)
        {
            if (!_flags._gotheader || _packetLength < maxSize) {
                _input.Shrink(maxSize);
            }
        }
    }

    if (res <= 0) {
        if (res == 0) {
            broken = true; // handle EOF
        } else { // res < 0
            broken = ((errno != EWOULDBLOCK) && (errno != EAGAIN));
            if (broken && (errno != ECONNRESET)) {
                LOG(debug, "Connection(%s): read error: %d", GetSpec(), errno);
            }
        }
    }

    return !broken;
}


bool
FNET_Connection::Write(bool direct)
{
    uint32_t writtenData    = 0;     // total data written
    uint32_t writtenPackets = 0;     // total packets written
    int      writeCnt       = 0;     // write count
    bool     broken         = false; // is this conn broken ?
    ssize_t  res;                    // single write result

    FNET_Packet     *packet;
    FNET_Context     context;

    do {

        // fill output buffer

        while (_output.GetDataLen() < FNET_WRITE_SIZE) {
            if (_myQueue.IsEmpty_NoLock())
                break;

            packet = _myQueue.DequeuePacket_NoLock(&context);
            if (packet->IsRegularPacket()) { // ignore non-regular packets
                _streamer->Encode(packet, context._value.INT, &_output);
                writtenPackets++;
            }
            packet->Free();
        }

        if (_output.GetDataLen() == 0) {
            res = 0;
            break;
        }

        // write data

        res = _socket.write(_output.GetData(), _output.GetDataLen());
        writeCnt++;
        if (res > 0) {
            _output.DataToDead((uint32_t)res);
            writtenData += (uint32_t)res;
            _output.resetIfEmpty();
        }
    } while (res > 0 &&
             _output.GetDataLen() == 0 &&
             !_myQueue.IsEmpty_NoLock() &&
             writeCnt < FNET_WRITE_REDO);

    if (writtenData > 0) {
        uint32_t maxSize = GetConfig()->_maxOutputBufferSize;
        if (maxSize > 0 && _output.GetBufSize() > maxSize) {
            _output.Shrink(maxSize);
        }
    }

    if (res < 0) {
        broken = ((errno != EWOULDBLOCK) && (errno != EAGAIN));
        if (broken && (errno != ECONNRESET)) {
            LOG(debug, "Connection(%s): write error: %d", GetSpec(), errno);
        }
    }

    std::unique_lock<std::mutex> guard(_ioc_lock);
    _writeWork = _queue.GetPacketCnt_NoLock()
                 + _myQueue.GetPacketCnt_NoLock()
                 + ((_output.GetDataLen() > 0) ? 1 : 0);
    _flags._writeLock = false;
    if (_flags._discarding) {
        _ioc_cond.notify_all();
    }
    bool writePending = (_writeWork > 0);

    if (direct) { // direct write (from post packet)
        if (writtenData > 0) {
            CountDirectDataWrite(writtenData);
            CountDirectPacketWrite(writtenPackets);
        }
        if (writePending) {
            AddRef_NoLock();
            guard.unlock();
            if (broken) {
                Owner()->Close(this, /* needRef = */ false);
            } else {
                Owner()->EnableWrite(this, /* needRef = */ false);
            }
        }
    } else {      // normal write (from event loop)
        guard.unlock();
        if (writtenData > 0) {
            CountDataWrite(writtenData);
            CountPacketWrite(writtenPackets);
        }
        if (!writePending)
            EnableWriteEvent(false);
    }

    return !broken;
}

////////////////////
// PUBLIC METHODS //
////////////////////


FNET_Connection::FNET_Connection(FNET_TransportThread *owner,
                                 FNET_IPacketStreamer *streamer,
                                 FNET_IServerAdapter *serverAdapter,
                                 vespalib::SocketHandle socket,
                                 const char *spec)
    : FNET_IOComponent(owner, socket.get(), spec, /* time-out = */ true),
      _streamer(streamer),
      _serverAdapter(serverAdapter),
      _adminChannel(nullptr),
      _socket(std::move(socket)),
      _resolve_handler(nullptr),
      _context(),
      _state(FNET_CONNECTED), // <-- NB
      _flags(),
      _packetLength(0),
      _packetCode(0),
      _packetCHID(0),
      _writeWork(0),
      _currentID(1), // <-- NB
      _input(FNET_READ_SIZE * 2),
      _queue(256),
      _myQueue(256),
      _output(FNET_WRITE_SIZE * 2),
      _channels(),
      _callbackTarget(nullptr),
      _cleanup(nullptr)
{
    assert(_socket.valid());
    LOG(debug, "Connection(%s): State transition: %s -> %s", GetSpec(),
        GetStateString(FNET_CONNECTING), GetStateString(FNET_CONNECTED));
}


FNET_Connection::FNET_Connection(FNET_TransportThread *owner,
                                 FNET_IPacketStreamer *streamer,
                                 FNET_IServerAdapter *serverAdapter,
                                 FNET_IPacketHandler *adminHandler,
                                 FNET_Context adminContext,
                                 FNET_Context context,
                                 const char *spec)
    : FNET_IOComponent(owner, -1, spec, /* time-out = */ true),
      _streamer(streamer),
      _serverAdapter(serverAdapter),
      _adminChannel(nullptr),
      _socket(),
      _resolve_handler(nullptr),
      _context(context),
      _state(FNET_CONNECTING),
      _flags(),
      _packetLength(0),
      _packetCode(0),
      _packetCHID(0),
      _writeWork(0),
      _currentID(0),
      _input(FNET_READ_SIZE * 2),
      _queue(256),
      _myQueue(256),
      _output(FNET_WRITE_SIZE * 2),
      _channels(),
      _callbackTarget(nullptr),
      _cleanup(nullptr)
{
    if (adminHandler != nullptr) {
        FNET_Channel::UP admin(new FNET_Channel(FNET_NOID, this, adminHandler, adminContext));
        _adminChannel = admin.get();
        _channels.Register(admin.release());
    }
}


FNET_Connection::~FNET_Connection()
{
    if (_adminChannel != nullptr) {
        _channels.Unregister(_adminChannel);
        delete _adminChannel;
    }
    assert(_cleanup == nullptr);
    assert(!_flags._writeLock);
}


bool
FNET_Connection::Init()
{
    // set up relevant events
    EnableReadEvent(true);
    if (IsClient()) {
        EnableWriteEvent(true);
    }

    // init server admin channel
    if (CanAcceptChannels() && _adminChannel == nullptr) {
        FNET_Channel::UP ach(new FNET_Channel(FNET_NOID, this));
        if (_serverAdapter->InitAdminChannel(ach.get())) {
            AddRef_NoLock();
            _channels.Register(ach.release());
        }
    }

    // handle close by admin channel init
    if (_state == FNET_CLOSED) {
        return false;
    }

    // initiate async resolve
    if (IsClient()) {
        _resolve_handler = std::make_shared<ResolveHandler>(this);
        Owner()->owner().resolve_async(GetSpec(), _resolve_handler);
    }
    return true;
}


bool
FNET_Connection::handle_add_event()
{
    if (_resolve_handler) {
        auto tweak = [this](vespalib::SocketHandle &handle) { return Owner()->tune(handle); };
        _socket = _resolve_handler->address.connect(tweak);
        _ioc_socket_fd = _socket.get();
        _resolve_handler.reset();
    }
    return _socket.valid();
}


void
FNET_Connection::SetCleanupHandler(FNET_IConnectionCleanupHandler *handler)
{
    _cleanup = handler;
}


FNET_Channel*
FNET_Connection::OpenChannel(FNET_IPacketHandler *handler,
                             FNET_Context context,
                             uint32_t *chid)
{
    FNET_Channel::UP newChannel(new FNET_Channel(FNET_NOID, this, handler, context));
    FNET_Channel * ret = nullptr;

    std::unique_lock<std::mutex> guard(_ioc_lock);
    if (__builtin_expect(_state < FNET_CLOSING, true)) {
        newChannel->SetID(GetNextID());
        if (chid != nullptr) {
            *chid = newChannel->GetID();
        }
        WaitCallback(guard, nullptr);
        AddRef_NoLock();
        ret = newChannel.release();
        _channels.Register(ret);
    }
    return ret;
}


FNET_Channel*
FNET_Connection::OpenChannel()
{

    uint32_t chid;
    {
        std::lock_guard<std::mutex> guard(_ioc_lock);
        chid = GetNextID();
        AddRef_NoLock();
    }
    return new FNET_Channel(chid, this);
}


bool
FNET_Connection::CloseChannel(FNET_Channel *channel)
{
    std::unique_lock<std::mutex> guard(_ioc_lock);
    WaitCallback(guard, channel);
    return _channels.Unregister(channel);
}


void
FNET_Connection::FreeChannel(FNET_Channel *channel)
{
    delete channel;
    SubRef_HasLock(std::unique_lock<std::mutex>(_ioc_lock));
}


void
FNET_Connection::CloseAndFreeChannel(FNET_Channel *channel)
{
    std::unique_lock<std::mutex> guard(_ioc_lock);
    WaitCallback(guard, channel);
    _channels.Unregister(channel);
    SubRef_HasLock(std::move(guard));
    delete channel;
}


void
FNET_Connection::CloseAdminChannel()
{
    FNET_Channel::UP toDelete;
    std::unique_lock<std::mutex> guard(_ioc_lock);
    if (_adminChannel != nullptr) {
        WaitCallback(guard, _adminChannel);
        if (_adminChannel != nullptr) {
            _channels.Unregister(_adminChannel);
            toDelete.reset(_adminChannel);
            _adminChannel = nullptr;
        }
    }
}


bool
FNET_Connection::PostPacket(FNET_Packet *packet, uint32_t chid)
{
    uint32_t writeWork;

    assert(packet != nullptr);
    std::unique_lock<std::mutex> guard(_ioc_lock);
    if (_state >= FNET_CLOSING) {
        if (_flags._discarding) {
            _queue.QueuePacket_NoLock(packet, FNET_Context(chid));
        } else {
            guard.unlock();
            packet->Free(); // discard packet
        }
        return false;     // connection is down
    }
    writeWork = _writeWork;
    _writeWork++;
    _queue.QueuePacket_NoLock(packet, FNET_Context(chid));
    if (writeWork == 0 && !_flags._writeLock &&
        _state == FNET_CONNECTED)
    {
        if (GetConfig()->_directWrite) {
            _flags._writeLock = true;
            _queue.FlushPackets_NoLock(&_myQueue);
            guard.unlock();
            Write(true);
        } else {
            AddRef_NoLock();
            guard.unlock();
            Owner()->EnableWrite(this, /* needRef = */ false);
        }
    }
    return true;
}


uint32_t
FNET_Connection::GetQueueLen()
{
    std::lock_guard<std::mutex> guard(_ioc_lock);
    return _queue.GetPacketCnt_NoLock() + _myQueue.GetPacketCnt_NoLock();
}


void
FNET_Connection::Sync()
{
    SyncPacket sp;
    PostPacket(&sp, FNET_NOID);
    sp.WaitFree();
}


void
FNET_Connection::CleanupHook()
{
    if (_cleanup != nullptr) {
        _cleanup->Cleanup(this);
        _cleanup = nullptr;
    }
}


void
FNET_Connection::Close()
{
    _resolve_handler.reset();
    detach_selector();
    SetState(FNET_CLOSED);
    _ioc_socket_fd = -1;
    _socket.reset();
}


bool
FNET_Connection::HandleReadEvent()
{
    bool broken = false;  // is connection broken ?

    switch(_state) {
    case FNET_CONNECTING: // ignore read events while connecting
        break;
    case FNET_CONNECTED:
        broken = !Read();
        break;
    case FNET_CLOSING:
    case FNET_CLOSED:
    default:
        broken = true;
    }
    return !broken;
}


bool
FNET_Connection::writePendingAfterConnect()
{
    std::lock_guard<std::mutex> guard(_ioc_lock);
    _state = FNET_CONNECTED; // SetState(FNET_CONNECTED)
    LOG(debug, "Connection(%s): State transition: %s -> %s", GetSpec(),
        GetStateString(FNET_CONNECTING), GetStateString(FNET_CONNECTED));
    return (_writeWork > 0);
}

bool
FNET_Connection::HandleWriteEvent()
{
    int  error;           // socket error code
    bool broken = false;  // is connection broken ?

    switch(_state) {
    case FNET_CONNECTING:
        error = _socket.get_so_error();
        if (error == 0) { // connect ok
            if (!writePendingAfterConnect()) {
                EnableWriteEvent(false);
            }
        } else {
            LOG(debug, "Connection(%s): connect error: %d", GetSpec(), error);

            SetState(FNET_CLOSED); // connect failed.
            broken = true;
        }
        break;
    case FNET_CONNECTED:
        {
            std::unique_lock<std::mutex> guard(_ioc_lock);
            if (_flags._writeLock) {
                guard.unlock();
                EnableWriteEvent(false);
                return true;
            }
            _flags._writeLock = true;
            _queue.FlushPackets_NoLock(&_myQueue);
        }
        broken = !Write(false);
        break;
    case FNET_CLOSING:
    case FNET_CLOSED:
    default:
        broken = true;
    }
    return !broken;
}
