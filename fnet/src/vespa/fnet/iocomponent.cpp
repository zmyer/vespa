// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "iocomponent.h"
#include "transport_thread.h"
#include <cassert>
#include <cstring>

FNET_IOComponent::FNET_IOComponent(FNET_TransportThread *owner,
                                   int socket_fd,
                                   const char *spec,
                                   bool shouldTimeOut)
    : _ioc_next(nullptr),
      _ioc_prev(nullptr),
      _ioc_owner(owner),
      _ioc_counters(_ioc_owner->GetStatCounters()),
      _ioc_socket_fd(socket_fd),
      _ioc_selector(nullptr),
      _ioc_spec(nullptr),
      _flags(shouldTimeOut),
      _ioc_timestamp(fastos::ClockSystem::now()),
      _ioc_lock(),
      _ioc_cond(),
      _ioc_refcnt(1),
      _ioc_directPacketWriteCnt(0),
      _ioc_directDataWriteCnt(0)
{
    _ioc_spec = strdup(spec);
    assert(_ioc_spec != nullptr);
}


FNET_IOComponent::~FNET_IOComponent()
{
    free(_ioc_spec);
    assert(_ioc_selector == nullptr);
}

FNET_Config *
FNET_IOComponent::GetConfig() {
    return _ioc_owner->GetConfig();
}

void
FNET_IOComponent::UpdateTimeOut() {
    _ioc_owner->UpdateTimeOut(this);
}

void
FNET_IOComponent::AddRef()
{
    std::lock_guard<std::mutex> guard(_ioc_lock);
    assert(_ioc_refcnt > 0);
    _ioc_refcnt++;
}


void
FNET_IOComponent::AddRef_NoLock()
{
    assert(_ioc_refcnt > 0);
    _ioc_refcnt++;
}


void
FNET_IOComponent::SubRef()
{
    {
        std::lock_guard<std::mutex> guard(_ioc_lock);
        assert(_ioc_refcnt > 0);
        if (--_ioc_refcnt > 0) {
            return;
        }
    }
    CleanupHook();
    delete this;
}


void
FNET_IOComponent::SubRef_HasLock(std::unique_lock<std::mutex> guard)
{
    assert(_ioc_refcnt > 0);
    if (--_ioc_refcnt > 0) {
        return;
    }
    guard.unlock();
    CleanupHook();
    delete this;
}


void
FNET_IOComponent::SubRef_NoLock()
{
    assert(_ioc_refcnt > 1);
    _ioc_refcnt--;
}


void
FNET_IOComponent::attach_selector(Selector &selector)
{
    detach_selector();
    _ioc_selector = &selector;
    _ioc_selector->add(_ioc_socket_fd, *this, _flags._ioc_readEnabled, _flags._ioc_writeEnabled);
}


void
FNET_IOComponent::detach_selector()
{
    if (_ioc_selector != nullptr) {
        _ioc_selector->remove(_ioc_socket_fd);
    }
    _ioc_selector = nullptr;
}

void
FNET_IOComponent::EnableReadEvent(bool enabled)
{
    _flags._ioc_readEnabled = enabled;
    if (_ioc_selector != nullptr) {
        _ioc_selector->update(_ioc_socket_fd, *this, _flags._ioc_readEnabled, _flags._ioc_writeEnabled);
    }
}


void
FNET_IOComponent::EnableWriteEvent(bool enabled)
{
    _flags._ioc_writeEnabled = enabled;
    if (_ioc_selector != nullptr) {
        _ioc_selector->update(_ioc_socket_fd, *this, _flags._ioc_readEnabled, _flags._ioc_writeEnabled);
    }
}


bool
FNET_IOComponent::handle_add_event()
{
    return true;
}


void
FNET_IOComponent::CleanupHook()
{
}
