// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "iocomponent.h"
#include "databuffer.h"
#include "context.h"
#include "channellookup.h"
#include "packetqueue.h"
#include <vespa/vespalib/net/socket_handle.h>
#include <vespa/vespalib/net/async_resolver.h>

class FNET_IPacketStreamer;
class FNET_IServerAdapter;
class FNET_IPacketHandler;

/**
 * Interface implemented by objects that want to perform connection
 * cleanup. Use the SetCleanupHandler method to register with a
 * connection. Currently, there can only be one cleanup handler per
 * connection.
 **/
class FNET_IConnectionCleanupHandler
{
public:

    /**
     * Destructor.  No cleanup needed for base class.
     */
    virtual ~FNET_IConnectionCleanupHandler(void) {}

    /**
     * Perform connection cleanup.
     *
     * @param conn the connection
     **/
    virtual void Cleanup(FNET_Connection *conn) = 0;
};


/**
 * This class represents a single connection with another
 * computer. The binary format on a connection is defined by the
 * PacketStreamer given to the constructor. Each connection object may
 * represent either the client or server end-point of a single TCP
 * connection. Only the client side may open new channels on the
 * connection.
 **/
class FNET_Connection : public FNET_IOComponent
{
public:
    enum State {
        FNET_CONNECTING,
        FNET_CONNECTED,
        FNET_CLOSING,
        FNET_CLOSED
    };

    enum {
        FNET_READ_SIZE  = 8192,
        FNET_READ_REDO  = 10,
        FNET_WRITE_SIZE = 8192,
        FNET_WRITE_REDO = 10
    };

private:
    struct Flags {
        Flags() :
            _gotheader(false),
            _writeLock(false),
            _inCallback(false),
            _callbackWait(false),
            _discarding(false)
        { }
        bool _gotheader;
        bool _writeLock;
        bool _inCallback;
        bool _callbackWait;
        bool _discarding;
    };
    struct ResolveHandler : public vespalib::AsyncResolver::ResultHandler {
        FNET_Connection *connection;
        vespalib::SocketAddress address;
        ResolveHandler(FNET_Connection *conn);
        void handle_result(vespalib::SocketAddress result) override;
        ~ResolveHandler();
    };
    using ResolveHandlerSP = std::shared_ptr<ResolveHandler>;
    FNET_IPacketStreamer    *_streamer;        // custom packet streamer
    FNET_IServerAdapter     *_serverAdapter;   // only on server side
    FNET_Channel            *_adminChannel;    // only on client side
    vespalib::SocketHandle   _socket;          // socket for this conn
    ResolveHandlerSP         _resolve_handler; // async resolve callback
    FNET_Context             _context;         // connection context
    State                    _state;           // connection state
    Flags                    _flags;           // Packed flags.
    uint32_t                 _packetLength;    // packet length
    uint32_t                 _packetCode;      // packet code
    uint32_t                 _packetCHID;      // packet chid
    uint32_t                 _writeWork;       // pending write work
    uint32_t                 _currentID;       // current channel ID
    FNET_DataBuffer          _input;           // input buffer
    FNET_PacketQueue_NoLock  _queue;           // outer output queue
    FNET_PacketQueue_NoLock  _myQueue;         // inner output queue
    FNET_DataBuffer          _output;          // output buffer
    FNET_ChannelLookup       _channels;        // channel 'DB'
    FNET_Channel            *_callbackTarget;  // target of current callback

    FNET_IConnectionCleanupHandler *_cleanup;  // cleanup handler

    FNET_Connection(const FNET_Connection &);
    FNET_Connection &operator=(const FNET_Connection &);


    /**
     * Get next ID that may be used for multiplexing on this connection.
     *
     * @return (hopefully) unique ID used for multiplexing.
     **/
    uint32_t GetNextID()
    {
        uint32_t ret = _currentID;
        if (ret == FNET_NOID)
            ret += 2;
        _currentID = ret + 2;
        return ret;
    }

    /**
     * Wait until the given channel is no longer the target of a
     * callback. The caller must have exclusive access to this object.
     * Note that calling this method from the callback you want to wait
     * for completion of will generate a deadlock (waiting for
     * yourself).
     *
     * @param channel the channel you want to wait for callback
     *                completion on. Note that when broadcasting
     *                control packets all channels are callback
     *                targets at the same time.
     **/
    void WaitCallback(std::unique_lock<std::mutex> &guard, FNET_Channel *channel)
    {
        while (_flags._inCallback
               && (_callbackTarget == channel || _callbackTarget == nullptr)) {
            _flags._callbackWait = true;
            _ioc_cond.wait(guard);
        }
    }

    /**
     * This method is called before a packet is delivered through a
     * callback. The object must be locked when this method is
     * called. When this method returns the object will be in callback
     * mode, but not locked.
     **/
    void BeforeCallback(std::unique_lock<std::mutex> &guard, FNET_Channel *channel)
    {
        _flags._inCallback     = true;
        _callbackTarget = channel;
        guard.unlock();
    }

    /**
     * This method is called after a packet has been delivered through a
     * callback. The object must be in callback mode when this method is
     * called, but not locked. When this method returns the object is no
     * longer in callback mode, but locked.
     **/
    void AfterCallback(std::unique_lock<std::mutex> &guard)
    {
        guard.lock();
        _flags._inCallback = false;
        if (_flags._callbackWait) {
            _flags._callbackWait = false;
            _ioc_cond.notify_all();
        }
    }

    /**
     * @return a string describing the given connection state.
     **/
    const char *GetStateString(State state);

    /**
     * Set connection state.
     *
     * @param state new state for this connection.
     **/
    void SetState(State state);

    /**
     * Handle incoming packet. The packet data is located in the input
     * databuffer. This method tries to decode the packet data into a
     * packet object and deliver it on the appropriate channel.
     *
     * @param plen packet length
     * @param pcode packet code
     * @param chid channel id
     **/
    void HandlePacket(uint32_t plen, uint32_t pcode, uint32_t chid);

    /**
     * Read incoming data from socket.
     *
     * @return false if socket is broken.
     **/
    bool Read();

    /**
     * Write outgoing data to socket.
     *
     * @return false if socket is broken.
     * @param direct is this a direct write (called directly from
     *        postpacket, without waiting for a write event)
     **/
    bool Write(bool direct);

    bool writePendingAfterConnect();
public:

    /**
     * Construct a connection in server aspect.
     *
     * @param owner the TransportThread object serving this connection
     * @param streamer custom packet streamer
     * @param serverAdapter object for custom channel creation
     * @param socket the underlying socket used for IO
     * @param spec listen spec
     **/
    FNET_Connection(FNET_TransportThread *owner,
                    FNET_IPacketStreamer *streamer,
                    FNET_IServerAdapter *serverAdapter,
                    vespalib::SocketHandle socket,
                    const char *spec);

    /**
     * Construct a connection in client aspect.
     *
     * @param owner the TransportThread object serving this connection
     * @param streamer custom packet streamer
     * @param serverAdapter object for custom channel creation
     * @param adminHandler packet handler for admin channel
     * @param adminContext context for admin channel
     * @param context initial context for this connection
     * @param spec connect spec
     **/
    FNET_Connection(FNET_TransportThread *owner,
                    FNET_IPacketStreamer *streamer,
                    FNET_IServerAdapter *serverAdapter,
                    FNET_IPacketHandler *adminHandler,
                    FNET_Context adminContext,
                    FNET_Context context,
                    const char *spec);

    /**
     * Destructor.
     **/
    ~FNET_Connection();


    /**
     * Is this the server endpoint of a network connection?
     *
     * @return true/false
     **/
    bool IsServer() const { return (_currentID & 0x1) == 1; }


    /**
     * Is this the client endpoint of a network connection?
     *
     * @return true/false
     **/
    bool IsClient() const { return (_currentID & 0x1) == 0; }


    /**
     * Is the given channel id generated by our peer?
     *
     * @return true/false
     **/
    bool IsFromPeer(uint32_t chid) const {
        return ((_currentID & 0x01) != (chid & 0x01));
    }


    /**
     * Does this connection have the ability to accept incoming channels ?
     *
     * @return true/false
     **/
    bool CanAcceptChannels() const { return _serverAdapter != nullptr; }


    /**
     * Assign a context to this connection.
     * @param context the context for this connection
     **/
    void SetContext(FNET_Context context) { _context = context; }


    /**
     * @return the context for this connection
     **/
    FNET_Context GetContext() { return _context; }


    /**
     * @return a pointer to the context for this connection
     **/
    FNET_Context *GetContextPT() { return &_context; }


    /**
     * @return current connection state.
     **/
    State GetState() { return _state; }


    /**
     * Initialize connection. This method should be called directly
     * after creation, before the object is shared. If this method
     * returns false, the object should be deleted on the spot.
     *
     * @return success(true)/failure(false)
     **/
    bool Init();

    /**
     * Called by the transport thread as the initial part of adding
     * this connection to the selection loop. If this is an incoming
     * connection (already connected) this function does nothing. If
     * this is an outgoing connection this function will use the async
     * resolve result to create a socket and initiate async connect.
     *
     * @return false if connection broken, true otherwise.
     **/
    bool handle_add_event() override;

    /**
     * Register a cleanup handler to be invoked when this connection is
     * about to be destructed.
     *
     * @param handler the cleanup handler
     **/
    void SetCleanupHandler(FNET_IConnectionCleanupHandler *handler);


    /**
     * Open a new channel on this connection. This method will return
     * nullptr of this connection is broken. Note that this method may only
     * be invoked on connections in the client aspect. Channels on
     * server aspect connections are opened with help from a server
     * adapter.
     *
     * @return an object representing the opened channel or nullptr.
     * @param handler the owner of the channel to be opened.
     * @param context application specific context.
     * @param chid store the channel id at this location if not nullptr
     **/
    FNET_Channel *OpenChannel(FNET_IPacketHandler *handler,
                              FNET_Context context,
                              uint32_t *chid = nullptr);


    /**
     * Open a new channel on this connection that may only be used to
     * send packets. This method will never return nullptr. Note that this
     * method may only be invoked on connections in the client
     * aspect. Channels on server aspect connections are opened with
     * help from a server adapter.
     *
     * @return an object representing the opened channel.
     **/
    FNET_Channel *OpenChannel();


    /**
     * Close a channel. Note that this only closes the network to
     * application path; the application may still send packets on a
     * closed channel.
     *
     * @return false if the channel was not found.
     * @param channel the channel to close.
     **/
    bool CloseChannel(FNET_Channel *channel);


    /**
     * Free a channel object. The channel may not be used after this
     * method has been called.
     *
     * @param channel the channel to free.
     **/
    void FreeChannel(FNET_Channel *channel);


    /**
     * Close and Free a channel in a single operation. This has the same
     * effect as invoking @ref CloseChannel then @ref FreeChannel, but
     * is more efficient.
     *
     * @param channel the channel to close and free.
     **/
    void CloseAndFreeChannel(FNET_Channel *channel);


    /**
     * Close the admin channel on a client connection. Invoking this
     * method on connections in the server aspect will have no effect.
     *
     * This method is needed because on the client side the application
     * does not own the actual admin channel object. The admin channel
     * on server-side connections are handled as normal channels.
     *
     * After this method returns, no more packets will be delivered to
     * the admin packet handler. In other words: this method is used to
     * make the connection loose the reference to the admin packet
     * handler in a thread-safe way, enabling the admin packet handler
     * to be deleted or the like.
     **/
    void CloseAdminChannel();


    /**
     * Post a packet on the output queue. Note that the packet will not
     * be sent if the connection is in CLOSING or CLOSED state. NOTE:
     * packet handover (caller TO invoked object).
     *
     * @return false if connection was down, true otherwise.
     * @param packet the packet to queue for sending.
     * @param chid the channel id for the packet
     **/
    bool PostPacket(FNET_Packet *packet, uint32_t chid);


    /**
     * Obtain the number of packets located in the output queue for this
     * connection. Note that this number is volatile and should only be
     * used as an estimate. Also note that since a queue latching
     * strategy is used, this method requires a mutex lock/unlock and is
     * therefore not as cheap as may be expected.
     *
     * @return number of packets currently located in the output queue
     *         for this connection.
     **/
    uint32_t GetQueueLen();


    /**
     * Sync with this connection. When this method is invoked it will
     * block until all packets currently posted on this connection is
     * encoded into the output buffer. Also, the amount of data in the
     * connection output buffer will be less than the amount given by
     * FNET_Connection::FNET_WRITE_SIZE.
     **/
    void Sync();


    /**
     * Invoked by the io component superclass before the object is
     * destructed. Will invoke the Cleanup method on the cleanup handler
     * for this connection, if present.
     **/
    void CleanupHook() override;


    /**
     * Close this connection immidiately. NOTE: this method should only
     * be called by the transport thread.
     **/
    void Close() override;


    /**
     * Called by the transport layer when a read event has occurred.
     *
     * @return false if connection broken, true otherwise.
     **/
    bool HandleReadEvent() override;


    /**
     * Called by the transport layer when a write event has occurred.
     *
     * @return false is connection broken, true otherwise.
     **/
    bool HandleWriteEvent() override;

    /**
     * @return Returns the size of this connection's output buffer.
     */
    uint32_t getOutputBufferSize() const { return _output.GetBufSize(); }

    /**
     * @return Returns the size of this connection's input buffer.
     */
    uint32_t getInputBufferSize() const { return _input.GetBufSize(); }

};

