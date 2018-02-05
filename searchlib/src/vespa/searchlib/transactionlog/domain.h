// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "domainpart.h"
#include "session.h"
#include <vespa/vespalib/util/threadexecutor.h>
#include <chrono>

namespace search::transactionlog {

struct PartInfo {
    SerialNumRange range;
    size_t numEntries;
    size_t byteSize;
    vespalib::string file;
    PartInfo(SerialNumRange range_in, size_t numEntries_in,
             size_t byteSize_in,
             vespalib::stringref file_in)
        : range(range_in), numEntries(numEntries_in), byteSize(byteSize_in),
          file(file_in) {}
};

struct DomainInfo {
    using DurationSeconds = std::chrono::duration<double>;
    SerialNumRange range;
    size_t numEntries;
    size_t byteSize;
    DurationSeconds maxSessionRunTime;
    std::vector<PartInfo> parts;
    DomainInfo(SerialNumRange range_in, size_t numEntries_in, size_t byteSize_in, DurationSeconds maxSessionRunTime_in)
        : range(range_in), numEntries(numEntries_in), byteSize(byteSize_in), maxSessionRunTime(maxSessionRunTime_in), parts() {}
    DomainInfo()
        : range(), numEntries(0), byteSize(0), maxSessionRunTime(), parts() {}
};

typedef std::map<vespalib::string, DomainInfo> DomainStats;

class Domain
{
public:
    using SP = std::shared_ptr<Domain>;
    using Executor = vespalib::ThreadExecutor;
    Domain(const vespalib::string &name, const vespalib::string &baseDir, Executor & commitExecutor,
           Executor & sessionExecutor, uint64_t domainPartSize, DomainPart::Crc defaultCrcType,
           const common::FileHeaderContext &fileHeaderContext);

    virtual ~Domain();

    DomainInfo getDomainInfo() const;
    const vespalib::string & name() const { return _name; }
    bool erase(SerialNum to);

    void commit(const Packet & packet);
    int visit(const Domain::SP & self, SerialNum from, SerialNum to, FRT_Supervisor & supervisor, FNET_Connection *conn);

    SerialNum begin() const;
    SerialNum end() const;
    SerialNum getSynced() const;
    void triggerSyncNow();
    bool getMarkedDeleted() const { return _markedDeleted; }
    void markDeleted() { _markedDeleted = true; }

    size_t byteSize() const;
    size_t getNumSessions() const { return _sessions.size(); }

    int startSession(int sessionId);
    int closeSession(int sessionId);

    SerialNum findOldestActiveVisit() const;
    DomainPart::SP findPart(SerialNum s);

    static vespalib::string
    getDir(const vespalib::string & base, const vespalib::string & domain) {
        return base + "/" + domain;
    }
    vespalib::Executor::Task::UP execute(vespalib::Executor::Task::UP task) {
        return _sessionExecutor.execute(std::move(task));
    }
    uint64_t size() const;

private:
    SerialNum begin(const vespalib::LockGuard & guard) const;
    SerialNum end(const vespalib::LockGuard & guard) const;
    size_t byteSize(const vespalib::LockGuard & guard) const;
    uint64_t size(const vespalib::LockGuard & guard) const;
    void cleanSessions();
    vespalib::string dir() const { return getDir(_baseDir, _name); }
    void addPart(int64_t partId, bool isLastPart);

    using SerialNumList = std::vector<SerialNum>;

    SerialNumList scanDir();

    using SessionList = std::map<int, Session::SP>;
    using DomainPartList = std::map<int64_t, DomainPart::SP>;
    using DurationSeconds = std::chrono::duration<double>;

    DomainPart::Crc     _defaultCrcType;
    Executor          & _commitExecutor;
    Executor          & _sessionExecutor;
    std::atomic<int>    _sessionId;
    vespalib::Monitor   _syncMonitor;
    bool                _pendingSync;
    vespalib::string    _name;
    uint64_t            _domainPartSize;
    DomainPartList      _parts;
    vespalib::Lock      _lock;
    vespalib::Lock      _sessionLock;
    SessionList         _sessions;
    DurationSeconds     _maxSessionRunTime;
    vespalib::string    _baseDir;
    const common::FileHeaderContext &_fileHeaderContext;
    bool                _markedDeleted;
};

}
