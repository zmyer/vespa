// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchsummary/docsummary/getdocsumargs.h>
#include <vespa/searchlib/common/fslimits.h>
#include <vespa/searchlib/engine/errorcodes.h>
#include <vespa/searchlib/engine/searchrequest.h>
#include <vespa/searchlib/common/packets.h>
#include <vespa/document/base/globalid.h>
#include <limits>
#include <mutex>
#include <condition_variable>

class FastS_ISearch;

//----------------------------------------------------------------

class FastS_SearchContext
{
public:
    union {
        uint32_t  INT;
        void     *VOIDP;
    } _value;

    FastS_SearchContext()
        : _value()
    {
        _value.VOIDP = NULL;
    }
    explicit FastS_SearchContext(void *value)
        : _value()
    {
        _value.VOIDP = value;
    }
    explicit FastS_SearchContext(uint32_t value)
        : _value()
    {
        _value.INT   = value;
    }
};

//----------------------------------------------------------------

class FastS_ISearchOwner
{
public:
    /**
     * Destructor.  No cleanup needed for base class.
     */
    virtual ~FastS_ISearchOwner() { }

    virtual void DoneQuery(FastS_ISearch *search,
                           FastS_SearchContext context) = 0;

    virtual void DoneDocsums(FastS_ISearch *search,
                             FastS_SearchContext context) = 0;
};

//----------------------------------------------------------------

class FastS_hitresult
{
public:
    const document::GlobalId & HT_GetGlobalID() const { return _gid; }
    search::HitRank HT_GetMetric()   const { return      _metric; }
    uint32_t HT_GetPartID()   const { return   _partition; }
    uint32_t getDistributionKey() const { return    _distributionKey; }

    void HT_SetGlobalID(const document::GlobalId & val) { _gid = val; }
    void HT_SetMetric(search::HitRank val)   {    _metric = val; }
    void HT_SetPartID(uint32_t val)   { _partition = val; }
    void setDistributionKey(uint32_t val) {  _distributionKey = val; }
    document::GlobalId _gid;
    search::HitRank    _metric;
    uint32_t           _partition;
private:
    uint32_t           _distributionKey;
};

//----------------------------------------------------------------

struct FastS_fullresult {
    uint32_t              _partition;
    uint32_t              _docid;
    document::GlobalId    _gid;
    search::HitRank       _metric;
    search::fs4transport::FS4Packet_DOCSUM::Buf _buf;
};

//----------------------------------------------------------------

class FastS_SearchInfo
{
public:
    uint32_t  _searchOffset;
    uint32_t  _maxHits;
    uint64_t  _coverageDocs;
    uint64_t  _activeDocs;
    uint64_t  _soonActiveDocs;
    uint32_t  _degradeReason;

    FastS_SearchInfo()
        : _searchOffset(0),
          _maxHits(0),
          _coverageDocs(0),
          _activeDocs(0),
          _soonActiveDocs(0),
          _degradeReason(0)
    {
    }
};

//----------------------------------------------------------------

class FastS_QueryResult
{
private:
    FastS_QueryResult(const FastS_QueryResult &);
    FastS_QueryResult& operator=(const FastS_QueryResult &);

public:
    FastS_hitresult  *_hitbuf;
    uint32_t          _hitCount;
    uint64_t          _totalHitCount;
    search::HitRank   _maxRank;
    double            _queryResultTime;

    uint32_t    _groupResultLen;
    const char *_groupResult;

    uint32_t   *_sortIndex;
    const char *_sortData;

    FastS_QueryResult()
        : _hitbuf(NULL),
          _hitCount(0),
          _totalHitCount(0),
          _maxRank(std::numeric_limits<search::HitRank>::is_integer ?
                   std::numeric_limits<search::HitRank>::min() :
                   - std::numeric_limits<search::HitRank>::max()),
          _queryResultTime(0.0),
          _groupResultLen(0),
          _groupResult(NULL),
          _sortIndex(NULL),
          _sortData(NULL)
    {}
};

//----------------------------------------------------------------

class FastS_DocsumsResult
{
private:
    FastS_DocsumsResult(const FastS_DocsumsResult &);
    FastS_DocsumsResult& operator=(const FastS_DocsumsResult &);

public:
    FastS_fullresult *_fullresult;
    uint32_t          _fullResultCount;
    double            _queryDocSumTime;

    FastS_DocsumsResult()
        : _fullresult(NULL),
          _fullResultCount(0),
          _queryDocSumTime(0.0)
    {}
};

//----------------------------------------------------------------

class FastS_ISearch
{
public:
    /**
     * Destructor.  No cleanup needed for base class.
     */
    virtual ~FastS_ISearch() { }


    enum RetCode {
        RET_OK         = 0, // sync operation performed
        RET_INPROGRESS = 1, // async operation started
        RET_ERROR      = 2  // illegal method invocation
    };

    // OBTAIN META-DATA

    virtual bool IsAsync() = 0;
    virtual uint32_t GetDataSetID() = 0;
    virtual FastS_SearchInfo *GetSearchInfo() = 0;

    // SET PARAMETERS

    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *owner, FastS_SearchContext context) = 0;
    virtual RetCode setSearchRequest(const search::engine::SearchRequest * request) = 0;
    virtual RetCode SetGetDocsumArgs(search::docsummary::GetDocsumArgs *docsumArgs) = 0;

    // SEARCH API

    virtual RetCode Search(uint32_t searchOffset,
                           uint32_t maxhits, uint32_t minhits = 0) = 0;
    virtual RetCode ProcessQueryDone() = 0;
    virtual FastS_QueryResult *GetQueryResult() = 0;

    // DOCSUM API

    virtual RetCode GetDocsums(const FastS_hitresult *hits, uint32_t hitcnt) = 0;
    virtual RetCode ProcessDocsumsDone() = 0;
    virtual FastS_DocsumsResult *GetDocsumsResult() = 0;

    // ERROR HANDLING

    virtual search::engine::ErrorCode GetErrorCode() = 0;
    virtual const char *GetErrorMessage() = 0;

    // INTERRUPT OPERATION

    virtual void Interrupt() = 0;

    // GET RID OF OBJECT

    virtual void Free() = 0;
};

//----------------------------------------------------------------

class FastS_SearchBase : public FastS_ISearch
{
private:
    FastS_SearchBase(const FastS_SearchBase &);
    FastS_SearchBase& operator=(const FastS_SearchBase &);

protected:
    uint32_t             _dataSetID;
    search::engine::ErrorCode _errorCode;
    char                *_errorMessage;
    const search::engine::SearchRequest *_queryArgs;
    search::docsummary::GetDocsumArgs *_docsumArgs;
    FastS_SearchInfo     _searchInfo;
    FastS_QueryResult    _queryResult;
    FastS_DocsumsResult  _docsumsResult;

public:
    FastS_SearchBase(uint32_t dataSetID)
        : _dataSetID(dataSetID),
          _errorCode(search::engine::ECODE_NO_ERROR),
          _errorMessage(NULL),
          _queryArgs(NULL),
          _docsumArgs(NULL),
          _searchInfo(),
          _queryResult(),
          _docsumsResult()
    {
    }

    virtual ~FastS_SearchBase()
    {
        free(_errorMessage);
    }

    const search::engine::SearchRequest * GetQueryArgs()   { return _queryArgs;  }
    search::docsummary::GetDocsumArgs * GetGetDocsumArgs() { return _docsumArgs; }

    void SetError(search::engine::ErrorCode errorCode, const char *errorMessage)
    {
        _errorCode = errorCode;
        if (errorMessage != NULL)
            _errorMessage = strdup(errorMessage);
        else
            _errorMessage = NULL;
    }


    virtual uint32_t GetDataSetID() override
    {
        return _dataSetID;
    }

    virtual FastS_SearchInfo *GetSearchInfo() override
    {
        return &_searchInfo;
    }

    virtual RetCode setSearchRequest(const search::engine::SearchRequest * request) override
    {
        _queryArgs = request;
        return RET_OK;
    }

    virtual RetCode SetGetDocsumArgs(search::docsummary::GetDocsumArgs *docsumArgs) override
    {
        _docsumArgs = docsumArgs;
        return RET_OK;
    }

    virtual RetCode Search(uint32_t searchOffset,
                           uint32_t maxhits, uint32_t minhits = 0) override
    {
        (void) minhits;
        _searchInfo._searchOffset = searchOffset;
        _searchInfo._maxHits      = maxhits;
        return RET_OK;
    }

    virtual RetCode ProcessQueryDone() override
    {
        return RET_OK;
    }

    virtual FastS_QueryResult *GetQueryResult() override
    {
        return &_queryResult;
    }

    virtual RetCode GetDocsums(const FastS_hitresult *hits, uint32_t hitcnt) override
    {
        (void) hits;
        (void) hitcnt;
        return RET_OK;
    }

    virtual RetCode ProcessDocsumsDone() override
    {
        return RET_OK;
    }

    virtual FastS_DocsumsResult *GetDocsumsResult() override
    {
        return &_docsumsResult;
    }

    virtual search::engine::ErrorCode GetErrorCode() override
    {
        return _errorCode;
    }

    virtual const char *GetErrorMessage() override
    {
        if (_errorMessage != NULL)
            return _errorMessage;
        return search::engine::getStringFromErrorCode(_errorCode);
    }

    virtual void Interrupt() override
    {
    }

    virtual void Free() override
    {
        delete this;
    }
};

//----------------------------------------------------------------

class FastS_FailedSearch : public FastS_SearchBase
{
private:
    bool _async;

public:
    FastS_FailedSearch(uint32_t dataSetID,
                       bool async,
                       search::engine::ErrorCode errorCode,
                       const char *errorMessage)
        : FastS_SearchBase(dataSetID),
          _async(async)
    {
        SetError(errorCode, errorMessage);
    }
    virtual ~FastS_FailedSearch() {}

    virtual bool IsAsync() override { return _async; }

    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *owner,
                                 FastS_SearchContext context) override
    {
        (void) owner;
        (void) context;
        return (_async) ? RET_OK : RET_ERROR;
    }
};

//----------------------------------------------------------------

class FastS_SyncSearch : public FastS_SearchBase
{
public:
    FastS_SyncSearch(uint32_t dataSetID)
        : FastS_SearchBase(dataSetID) {}

    bool IsAsync() override { return false; }

    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *,
                                 FastS_SearchContext) override
    {
        return RET_ERROR;
    }
};

//----------------------------------------------------------------

class FastS_AsyncSearch : public FastS_SearchBase
{
private:
    FastS_AsyncSearch(const FastS_AsyncSearch &);
    FastS_AsyncSearch& operator=(const FastS_AsyncSearch &);

protected:
    FastS_ISearchOwner  *_searchOwner;
    FastS_SearchContext  _searchContext;

public:
    FastS_AsyncSearch(uint32_t dataSetID)
        : FastS_SearchBase(dataSetID),
          _searchOwner(NULL),
          _searchContext(FastS_SearchContext()) {}

    bool IsAsync() override { return true; }

    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *owner,
                                 FastS_SearchContext context) override
    {
        _searchOwner   = owner;
        _searchContext = context;
        return RET_OK;
    }
};

//----------------------------------------------------------------

class FastS_SearchAdapter : public FastS_ISearch
{
private:
    FastS_SearchAdapter(const FastS_SearchAdapter &);
    FastS_SearchAdapter& operator=(const FastS_SearchAdapter &);

protected:
    FastS_ISearch *_search;

public:
    explicit FastS_SearchAdapter(FastS_ISearch *search);
    virtual ~FastS_SearchAdapter();

    virtual bool IsAsync() override;
    virtual uint32_t GetDataSetID() override;
    virtual FastS_SearchInfo *GetSearchInfo() override;
    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *owner,
                                 FastS_SearchContext context) override;
    virtual RetCode setSearchRequest(const search::engine::SearchRequest * request) override;
    virtual RetCode SetGetDocsumArgs(search::docsummary::GetDocsumArgs *docsumArgs) override;
    virtual RetCode Search(uint32_t searchOffset,
                           uint32_t maxhits, uint32_t minhits = 0) override;
    virtual RetCode ProcessQueryDone() override;
    virtual FastS_QueryResult *GetQueryResult() override;
    virtual RetCode GetDocsums(const FastS_hitresult *hits, uint32_t hitcnt) override;
    virtual RetCode ProcessDocsumsDone() override;
    virtual FastS_DocsumsResult *GetDocsumsResult() override;
    virtual search::engine::ErrorCode GetErrorCode() override;
    virtual const char *GetErrorMessage() override;
    virtual void Interrupt() override;
    virtual void Free() override;
};

//----------------------------------------------------------------

class FastS_SyncSearchAdapter : public FastS_SearchAdapter,
                                public FastS_ISearchOwner
{
private:
    std::mutex              _lock;
    std::condition_variable _cond;
    bool _waitQuery;
    bool _queryDone;
    bool _waitDocsums;
    bool _docsumsDone;

protected:
    explicit FastS_SyncSearchAdapter(FastS_ISearch *search);

public:
    virtual ~FastS_SyncSearchAdapter();

    static FastS_ISearch *Adapt(FastS_ISearch *search);

    virtual void DoneQuery(FastS_ISearch *, FastS_SearchContext) override;
    virtual void DoneDocsums(FastS_ISearch *, FastS_SearchContext) override;

    void WaitQueryDone();
    void WaitDocsumsDone();

    virtual bool IsAsync() override;
    virtual RetCode SetAsyncArgs(FastS_ISearchOwner *owner,
                                 FastS_SearchContext context) override;
    virtual RetCode Search(uint32_t searchOffset,
                           uint32_t maxhits, uint32_t minhits = 0) override;
    virtual RetCode GetDocsums(const FastS_hitresult *hits, uint32_t hitcnt) override;
};

//----------------------------------------------------------------

