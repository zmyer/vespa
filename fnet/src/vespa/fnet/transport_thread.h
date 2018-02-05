// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "scheduler.h"
#include "config.h"
#include "task.h"
#include "packetqueue.h"
#include "stats.h"
#include <vespa/fastos/thread.h>
#include <vespa/fastos/time.h>
#include <vespa/vespalib/net/socket_handle.h>
#include <vespa/vespalib/net/selector.h>
#include <mutex>
#include <condition_variable>

class FNET_Transport;
class FNET_ControlPacket;
class FNET_IPacketStreamer;
class FNET_IServerAdapter;

/**
 * This class represents a transport thread and handles a subset of
 * the network related work for the application in both client and
 * server aspects.
 **/
class FNET_TransportThread : public FastOS_Runnable
{
    friend class FNET_IOComponent;

public:
    using Selector = vespalib::Selector<FNET_IOComponent>;

#ifndef IAM_DOXYGEN
    class StatsTask : public FNET_Task
    {
    private:
        FNET_TransportThread *_transport;
        StatsTask(const StatsTask &);
        StatsTask &operator=(const StatsTask &);
    public:
        StatsTask(FNET_Scheduler *scheduler,
                  FNET_TransportThread *transport) : FNET_Task(scheduler),
                                                     _transport(transport) {}
        void PerformTask() override;
    };
    friend class FNET_TransportThread::StatsTask;
#endif // DOXYGEN

private:
    FNET_Transport          &_owner;          // owning transport layer
    FastOS_Time              _startTime;      // when event loop started
    FastOS_Time              _now;            // current time sampler
    FNET_Scheduler           _scheduler;      // transport thread scheduler
    FNET_StatCounters        _counters;       // stat counters
    FNET_Stats               _stats;          // current stats
    StatsTask                _statsTask;      // stats task
    FastOS_Time              _statTime;       // last stat update
    FNET_Config              _config;         // FNET configuration [static]
    FNET_IOComponent        *_componentsHead; // I/O component list head
    FNET_IOComponent        *_timeOutHead;    // first IOC in list to time out
    FNET_IOComponent        *_componentsTail; // I/O component list tail
    uint32_t                 _componentCnt;   // # of components
    FNET_IOComponent        *_deleteList;     // IOC delete list
    Selector                 _selector;       // I/O event generator
    FNET_PacketQueue_NoLock  _queue;          // outer event queue
    FNET_PacketQueue_NoLock  _myQueue;        // inner event queue
    std::mutex               _lock;           // used for synchronization
    std::condition_variable  _cond;           // used for synchronization
    bool                     _started;        // event loop started ?
    bool                     _shutdown;       // should stop event loop ?
    bool                     _finished;       // event loop stopped ?
    bool                     _waitFinished;   // someone is waiting for _finished
    bool                     _deleted;        // destructor called ?


    FNET_TransportThread(const FNET_TransportThread &);
    FNET_TransportThread &operator=(const FNET_TransportThread &);


    /**
     * Add an IOComponent to the list of components. This operation is
     * performed immidiately and without locking. This method should
     * only be called in the transport thread.
     *
     * @param comp the component to add.
     **/
    void AddComponent(FNET_IOComponent *comp);


    /**
     * Remove an IOComponent from the list of components. This operation is
     * performed immidiately and without locking. This method should
     * only be called in the transport thread.
     *
     * @param comp the component to remove.
     **/
    void RemoveComponent(FNET_IOComponent *comp);


    /**
     * Update time-out information for the given I/O component. This
     * method may only be called in the transport thread. Calling this
     * method will update the timestamp on the given IOC and perform a
     * remove/add IOC operation, putting it last in the time-out queue.
     *
     * @param comp component to update time-out info for.
     **/
    void UpdateTimeOut(FNET_IOComponent *comp);


    /**
     * Add an IOComponent to the delete list. This operation is
     * performed immidiately and without locking. This method should
     * only be called in the transport thread. NOTE: the IOC must be
     * removed from the list of active components before this method may
     * be called.
     *
     * @param comp the component to add to the delete list.
     **/
    void AddDeleteComponent(FNET_IOComponent *comp);


    /**
     * Delete (call SubRef on) all IO Components in the delete list.
     **/
    void FlushDeleteList();


    /**
     * Post an event (ControlPacket) on the transport thread event
     * queue. This is done to tell the transport thread that it needs to
     * do an operation that could not be performed in other threads due
     * to thread-safety. If the event queue is empty, invoking this
     * method will wake up the transport thread in order to reduce
     * latency. Note that when posting events that have a reference
     * counted object as parameter you need to increase the reference
     * counter to ensure that the object will not be deleted before the
     * event is handled.
     *
     * @return true if the event was accepted (false if rejected)
     * @param cpacket the event command
     * @param context the event parameter
     **/
    bool PostEvent(FNET_ControlPacket *cpacket, FNET_Context context);


    /**
     * Discard an event. This method is used to discard events that will
     * not be handled due to shutdown.
     *
     * @param cpacket the event command
     * @param context the event parameter
     **/
    void DiscardEvent(FNET_ControlPacket *cpacket, FNET_Context context);


    /**
     * Update internal FNET statistics. This method is called regularly
     * by the statistics update task.
     **/
    void UpdateStats();


    /**
     * Obtain a reference to the stat counters used by this transport
     * object.
     *
     * @return stat counters for this transport object.
     **/
    FNET_StatCounters *GetStatCounters() { return &_counters; }


    /**
     * Count event loop iteration(s).
     *
     * @param cnt event loop iterations (default is 1).
     **/
    void CountEventLoop(uint32_t cnt = 1)
    { _counters.CountEventLoop(cnt); }


    /**
     * Count internal event(s).
     *
     * @param cnt number of internal events.
     **/
    void CountEvent(uint32_t cnt)
    { _counters.CountEvent(cnt); }


    /**
     * Count IO events.
     *
     * @param cnt number of IO events.
     **/
    void CountIOEvent(uint32_t cnt)
    { _counters.CountIOEvent(cnt); }


    /**
     * Obtain a reference to the object holding the configuration for
     * this transport object.
     *
     * @return config object.
     **/
    FNET_Config *GetConfig() { return &_config; }


public:
    /**
     * Construct a transport object. To activate your newly created
     * transport object you need to call either the Start method to
     * spawn a new thread to handle IO, or the Main method to let the
     * current thread become the transport thread.
     *
     * @param owner owning transport layer
     **/
    FNET_TransportThread(FNET_Transport &owner_in);


    /**
     * Destruct object. This should NOT be done before the transport
     * thread has completed it's work and raised the finished flag.
     **/
    ~FNET_TransportThread();


    /**
     * Obtain the owning transport layer
     *
     * @return transport layer owning this transport thread
     **/
    FNET_Transport &owner() const { return _owner; }

    /**
     * Tune the given socket handle to be used as an async transport
     * connection.
     **/
    bool tune(vespalib::SocketHandle &handle) const;

    /**
     * Add a network listener in an abstract way. The given 'spec'
     * string has the following format: 'type/where'. 'type' specifies
     * the protocol used; currently only 'tcp' is allowed, but it is
     * included for future extensions. 'where' specifies where to listen
     * in a way depending on the 'type' field; with tcp this field holds
     * a port number. Example: listen for tcp/ip connections on port
     * 8001: spec = 'tcp/8001'. If you want to enable strict binding you
     * may supply a hostname as well, like this:
     * 'tcp/mycomputer.mydomain:8001'.
     *
     * @return the connector object, or nullptr if listen failed.
     * @param spec string specifying how and where to listen.
     * @param streamer custom packet streamer.
     * @param serverAdapter object for custom channel creation.
     **/
    FNET_Connector *Listen(const char *spec, FNET_IPacketStreamer *streamer,
                           FNET_IServerAdapter *serverAdapter);


    /**
     * Connect to a target host in an abstract way. The given 'spec'
     * string has the following format: 'type/where'. 'type' specifies
     * the protocol used; currently only 'tcp' is allowed, but it is
     * included for future extensions. 'where' specifies where to
     * connect in a way depending on the type field; with tcp this field
     * holds a host name (or IP address) and a port number. Example:
     * connect to www.fast.no on port 80 (using tcp/ip): spec =
     * 'tcp/www.fast.no:80'. The newly created connection will be
     * serviced by this transport layer object. If the adminHandler
     * parameter is given, an internal admin channel is created in the
     * connection object. The admin channel will be used to deliver
     * packets tagged with the reserved channel id (FNET_NOID) to the
     * admin handler.
     *
     * @return an object representing the new connection.
     * @param spec string specifying how and where to connect.
     * @param streamer custom packet streamer.
     * @param adminHandler packet handler for incoming packets on the
     *                     admin channel.
     * @param adminContext application context to be used for incoming
     *                     packets on the admin channel.
     * @param serverAdapter adapter used to support 2way channel creation.
     * @param connContext application context for the connection.
     **/
    FNET_Connection *Connect(const char *spec, FNET_IPacketStreamer *streamer,
                             FNET_IPacketHandler *adminHandler = nullptr,
                             FNET_Context adminContext = FNET_Context(),
                             FNET_IServerAdapter *serverAdapter = nullptr,
                             FNET_Context connContext = FNET_Context());


    /**
     * This method may be used to determine how many IO Components are
     * currently controlled by this transport layer object. Note that
     * locking is not used, since this information is volatile anyway.
     *
     * @return the current number of IOComponents.
     **/
    uint32_t GetNumIOComponents() { return _componentCnt; }


    /**
     * Set the I/O Component timeout. Idle I/O Components with timeout
     * enabled (determined by calling the ShouldTimeOut method) will
     * time out if idle for the given number of milliseconds. An I/O
     * component reports its un-idle-ness by calling the UpdateTimeOut
     * method in the owning transport object. Calling this method with 0
     * as parameter will disable I/O Component timeouts. Note that newly
     * created transport objects begin their lives with I/O Component
     * timeouts disabled. An I/O Component timeout has the same effect
     * as calling the Close method in the transport object with the
     * target I/O Component as parameter.
     *
     * @param ms number of milliseconds before IOC idle timeout occurs.
     **/
    void SetIOCTimeOut(uint32_t ms) { _config._iocTimeOut = ms; }


    /**
     * Set maximum input buffer size. This value will only affect
     * connections that use a common input buffer when decoding
     * incoming packets. Note that this value is not an absolute
     * max. The buffer will still grow larger than this value if
     * needed to decode big packets. However, when the buffer becomes
     * larger than this value, it will be shrunk back when possible.
     *
     * @param bytes buffer size in bytes. 0 means unlimited.
     **/
    void SetMaxInputBufferSize(uint32_t bytes)
    { _config._maxInputBufferSize = bytes; }


    /**
     * Set maximum output buffer size. This value will only affect
     * connections that use a common output buffer when encoding
     * outgoing packets. Note that this value is not an absolute
     * max. The buffer will still grow larger than this value if needed
     * to encode big packets. However, when the buffer becomes larger
     * than this value, it will be shrunk back when possible.
     *
     * @param bytes buffer size in bytes. 0 means unlimited.
     **/
    void SetMaxOutputBufferSize(uint32_t bytes)
    { _config._maxOutputBufferSize = bytes; }


    /**
     * Enable or disable the direct write optimization. This is
     * enabled by default and favors low latency above throughput.
     *
     * @param directWrite enable direct write?
     **/
    void SetDirectWrite(bool directWrite) {
        _config._directWrite = directWrite;
    }


    /**
     * Enable or disable use of the TCP_NODELAY flag with sockets
     * created by this transport object.
     *
     * @param noDelay true if TCP_NODELAY flag should be used.
     **/
    void SetTCPNoDelay(bool noDelay) { _config._tcpNoDelay = noDelay; }


    /**
     * Enable or disable logging of FNET statistics. This feature is
     * disabled by default.
     *
     * @param logStats true if stats should be logged.
     **/
    void SetLogStats(bool logStats) { _config._logStats = logStats; }


    /**
     * Add an I/O component to the working set of this transport
     * object. Note that the actual work is performed by the transport
     * thread. This method simply posts an event on the transport thread
     * event queue. NOTE: in order to post async events regarding I/O
     * components, an extra reference to the component needs to be
     * allocated. The needRef flag indicates wether the caller already
     * has done this.
     *
     * @param comp the component you want to add.
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void Add(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Calling this method enables read events for the given I/O
     * component. Note that the actual work is performed by the
     * transport thread. This method simply posts an event on the
     * transport thread event queue. NOTE: in order to post async events
     * regarding I/O components, an extra reference to the component
     * needs to be allocated. The needRef flag indicates wether the
     * caller already has done this.
     *
     * @param comp the component that wants read events.
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void EnableRead(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Calling this method disables read events for the given I/O
     * component. Note that the actual work is performed by the
     * transport thread. This method simply posts an event on the
     * transport thread event queue. NOTE: in order to post async events
     * regarding I/O components, an extra reference to the component
     * needs to be allocated. The needRef flag indicates wether the
     * caller already has done this.
     *
     * @param comp the component that no longer wants read events.
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void DisableRead(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Calling this method enables write events for the given I/O
     * component. Note that the actual work is performed by the
     * transport thread. This method simply posts an event on the
     * transport thread event queue. NOTE: in order to post async events
     * regarding I/O components, an extra reference to the component
     * needs to be allocated. The needRef flag indicates wether the
     * caller already has done this.
     *
     * @param comp the component that wants write events.
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void EnableWrite(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Calling this method disables write events for the given I/O
     * component. Note that the actual work is performed by the
     * transport thread. This method simply posts an event on the
     * transport thread event queue. NOTE: in order to post async events
     * regarding I/O components, an extra reference to the component
     * needs to be allocated. The needRef flag indicates wether the
     * caller already has done this.
     *
     * @param comp the component that no longer wants write events.
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void DisableWrite(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Close an I/O component and remove it from the working set of this
     * transport object. Note that the actual work is performed by the
     * transport thread. This method simply posts an event on the
     * transport thread event queue. NOTE: in order to post async events
     * regarding I/O components, an extra reference to the component
     * needs to be allocated. The needRef flag indicates wether the
     * caller already has done this.
     *
     * @param comp the component you want to close (and remove).
     * @param needRef should be set to false if the caller of this
     *        method already has obtained an extra reference to the
     *        component. If this flag is true, this method will call the
     *        AddRef method on the component.
     **/
    void Close(FNET_IOComponent *comp, bool needRef = true);


    /**
     * Post an execution event on the transport event queue. The return
     * value from this method indicate whether the execution request was
     * accepted or not. If it was accepted, the transport thread will
     * execute the given executable at a later time. However, if it was
     * rejected (this method returns false), the caller of this method
     * will need to handle the fact that the executor will never be
     * executed. Also note that it is the responsibility of the caller
     * to ensure that all needed context for the executor is kept alive
     * until the time of execution. It is ok to assume that execution
     * requests will only be rejected due to transport thread shutdown.
     *
     * @return true if the execution request was accepted, false if it was rejected
     * @param exe the executable we want to execute in the transport thread
     **/
    bool execute(FNET_IExecutable *exe);


    /**
     * Synchronize with the transport thread. This method will block
     * until all events posted before this method was invoked has been
     * processed. If the transport thread has been shut down (or is in
     * the progress of being shut down) this method will instead wait
     * for the transport thread to complete, since no more commands will
     * be performed, and waiting would be forever. Invoking this method
     * from the transport thread is not a good idea.
     **/
    void sync();


    /**
     * Obtain a pointer to the current time sampler. The current time
     * sampler may only be used by the transport thread. Also, it SHOULD
     * be used by ALL methods driven by the transport thread that wants
     * to have an estimate of the current time. This includes the custom
     * application hook, packet delivery callbacks and pingable objects.
     **/
    FastOS_Time *GetTimeSampler() { return &_now; }


    /**
     * Obtain a pointer to the transport thread scheduler. This
     * scheduler may be used to schedule tasks to be run by the
     * transport thread.
     *
     * @return transport thread scheduler.
     **/
    FNET_Scheduler *GetScheduler() { return &_scheduler; }


    /**
     * Calling this method will shut down the transport layer in a nice
     * way. Note that the actual task of shutting down is performed by
     * the transport thread. This method simply posts an event on the
     * transport thread event queue telling it to shut down.
     *
     * @param waitFinished if this flag is set, the method call will not
     *        return until the transport layer is shut down. NOTE: do
     *        not set this flag if you are calling this method from a
     *        callback from the transport layer, as it will create a
     *        deadlock.
     **/
    void ShutDown(bool waitFinished);


    /**
     * This method will make the calling thread wait until the transport
     * layer has been shut down. NOTE: do not invoke this method if you
     * are in a callback from the transport layer, as it will create a
     * deadlock. See @ref ShutDown.
     **/
    void WaitFinished();


    /**
     * This method is called to initialize the transport thread event
     * loop. It is called from the FRT_Transport::Run method. If you
     * want to customize the event loop, you should do this by invoking
     * this method once, then invoke the @ref EventLoopIteration method
     * until it returns false (indicating transport shutdown).
     *
     * @return true on success, false on failure.
     **/
    bool InitEventLoop();


    // selector call-back for selector wakeup
    void handle_wakeup();

    // selector call-back for io-events
    void handle_event(FNET_IOComponent &ctx, bool read, bool write);

    /**
     * Perform a single transport thread event loop iteration. This
     * method is called by the FRT_Transport::Run method. If you want to
     * customize the event loop, you should do this by invoking the @ref
     * InitEventLoop method once, then invoke this method until it
     * returns false (indicating transport shutdown).
     *
     * @return true when active, false after shutdown.
     **/
    bool EventLoopIteration();


    /**
     * Start transport layer operation in a separate thread. Note that
     * the return value of this method only indicates whether the
     * spawning of the new thread went ok.
     *
     * @return thread create status.
     * @param pool threadpool that may be used to spawn a new thread.
     **/
    bool Start(FastOS_ThreadPool *pool);


    /**
     * Calling this method will give the current thread to the transport
     * layer. The method will not return until the transport layer is
     * shut down by calling the @ref ShutDown method.
     **/
    void Main();


    /**
     * This is where the transport thread lives, when started by
     * invoking one of the @ref Main or @ref Start methods. If you want
     * to combine the FNET event loop with your own, you may use the
     * @ref InitEventLoop and @ref EventLoopIteration methods directly.
     **/
    void Run(FastOS_ThreadInterface *thisThread, void *args) override;
};
