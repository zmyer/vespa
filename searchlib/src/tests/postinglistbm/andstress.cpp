// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "andstress.h"
#include <vector>

#include <vespa/searchlib/common/bitvector.h>
#include <vespa/searchlib/test/fakedata/fakeword.h>
#include <vespa/searchlib/test/fakedata/fakewordset.h>
#include <vespa/searchlib/test/fakedata/fakeposting.h>
#include <vespa/searchlib/test/fakedata/fakefilterocc.h>
#include <vespa/searchlib/test/fakedata/fakeegcompr64filterocc.h>
#include <vespa/searchlib/test/fakedata/fakezcfilterocc.h>
#include <vespa/searchlib/test/fakedata/fakezcbfilterocc.h>
#include <vespa/searchlib/test/fakedata/fpfactory.h>
#include <vespa/fastos/thread.h>
#include <mutex>
#include <condition_variable>

#include <vespa/log/log.h>
LOG_SETUP(".andstress");

using search::fef::TermFieldMatchData;
using search::fef::TermFieldMatchDataArray;
using search::queryeval::SearchIterator;
using namespace search::fakedata;

namespace postinglistbm {

class AndStressWorker;

class AndStressMaster
{
private:
    AndStressMaster(const AndStressMaster &);

    AndStressMaster &
    operator=(const AndStressMaster &);

    search::Rand48 &_rnd;
    unsigned int _numDocs;
    unsigned int _commonDocFreq;
    std::vector<std::string> _postingTypes;
    unsigned int _loops;
    unsigned int _skipCommonPairsRate;
    uint32_t _stride;
    bool _unpack;

    FastOS_ThreadPool *_threadPool;
    std::vector<AndStressWorker *> _workers;
    unsigned int _workersDone;

    FakeWordSet &_wordSet;

    std::vector<std::vector<FakePosting::SP> > _postings;

    std::mutex              _taskLock;
    std::condition_variable _taskCond;
    unsigned int _taskIdx;
    uint32_t _numTasks;

public:
    typedef std::pair<FakePosting *, FakePosting *> Task;
private:
    std::vector<Task> _tasks;
public:
    AndStressMaster(search::Rand48 &rnd,
                    FakeWordSet &wordSet,
                    unsigned int numDocs,
                    unsigned int commonDocFreq,
                    const std::vector<std::string> &postingType,
                    unsigned int loops,
                    unsigned int skipCommonPairsRate,
                    uint32_t numTasks,
                    uint32_t stride,
                    bool unpack);

    ~AndStressMaster();
    void run();
    void makePostingsHelper(FPFactory *postingFactory,
                            const std::string &postingFormat,
                            bool validate, bool verbose);
    void dropPostings();
    void dropTasks();
    void resetTasks();  // Prepare for rerun
    void setupTasks(unsigned int numTasks);
    Task *getTask();
    unsigned int getNumDocs() const { return _numDocs; }
    bool getUnpack() const { return _unpack; }
    double runWorkers(const std::string &postingFormat);
};


class AndStressWorker : public FastOS_Runnable
{
private:
    AndStressWorker(const AndStressWorker &);

    AndStressWorker &
    operator=(const AndStressWorker &);

    AndStressMaster &_master;
    unsigned int _id;
public:
    AndStressWorker(AndStressMaster &master, unsigned int id);
    ~AndStressWorker();
    virtual void Run(FastOS_ThreadInterface *thisThread, void *arg) override;
};


template <class P>
FakePosting *
makePosting(FakeWord &fw)
{
    return new P(fw);
}


AndStressMaster::AndStressMaster(search::Rand48 &rnd,
                                 FakeWordSet &wordSet,
                                 unsigned int numDocs,
                                 unsigned int commonDocFreq,
                                 const std::vector<std::string> &postingTypes,
                                 unsigned int loops,
                                 unsigned int skipCommonPairsRate,
                                 uint32_t numTasks,
                                 uint32_t stride,
                                 bool unpack)
    : _rnd(rnd),
      _numDocs(numDocs),
      _commonDocFreq(commonDocFreq),
      _postingTypes(postingTypes),
      _loops(loops),
      _skipCommonPairsRate(skipCommonPairsRate),
      _stride(stride),
      _unpack(unpack),
      _threadPool(NULL),
      _workers(),
      _workersDone(0),
      _wordSet(wordSet),
      _postings(FakeWordSet::NUM_WORDCLASSES),
      _taskLock(),
      _taskCond(),
      _taskIdx(0),
      _numTasks(numTasks),
      _tasks()
{
    LOG(info, "AndStressMaster::AndStressMaster");

    _threadPool = new FastOS_ThreadPool(128 * 1024, 400);
}

template <class C>
static void
clearPtrVector(std::vector<C> &v)
{
    for (unsigned int i = 0; i < v.size(); ++i)
        delete v[i];
    v.clear();
}


AndStressMaster::~AndStressMaster()
{
    LOG(info, "AndStressMaster::~AndStressMaster");

    _threadPool->Close();
    delete _threadPool;
    _threadPool = NULL;
    clearPtrVector(_workers);
    dropPostings();
}


void
AndStressMaster::dropPostings()
{
    for (unsigned int i = 0; i < _postings.size(); ++i)
        _postings[i].clear();
    dropTasks();
}


void
AndStressMaster::dropTasks()
{
    _tasks.clear();
    _taskIdx = 0;
}


void
AndStressMaster::resetTasks()
{
    _taskIdx = 0;
}


static void
makeSomePostings(FPFactory *postingFactory,
                 std::vector<FakeWord *> &w,
                 std::vector<FakePosting::SP> &p,
                 uint32_t stride,
                 bool validate,
                 bool verbose)
{
    for (unsigned int i = 0; i < w.size(); ++i) {
        FakePosting::SP np(postingFactory->make(*w[i]));
        if (validate) {
            TermFieldMatchData md;
            TermFieldMatchDataArray tfmda;
            tfmda.add(&md);

            std::unique_ptr<SearchIterator> sb(np->createIterator(tfmda));
            if (np->hasWordPositions()) {
                if (stride != 0)
                    w[i]->validate(sb.get(), tfmda, stride, verbose);
                else
                    w[i]->validate(sb.get(), tfmda, verbose);
            } else
                w[i]->validate(sb.get(), verbose);
        }
        p.push_back(np);
    }
}

void
AndStressMaster::makePostingsHelper(FPFactory *postingFactory,
                                    const std::string &postingFormat,
                                    bool validate, bool verbose)
{
    FastOS_Time tv;
    double before;
    double after;

    tv.SetNow();
    before = tv.Secs();
    postingFactory->setup(_wordSet);
    for (unsigned int i = 0; i < _wordSet._words.size(); ++i)
        makeSomePostings(postingFactory,
                         _wordSet._words[i], _postings[i],
                         _stride,
                         validate,
                         verbose);
    tv.SetNow();
    after = tv.Secs();
    LOG(info,
        "AndStressMaster::makePostingsHelper elapsed %10.6f s for %s format",
        after - before,
        postingFormat.c_str());
}


void
AndStressMaster::setupTasks(unsigned int numTasks)
{
    unsigned int wordclass1;
    unsigned int wordclass2;
    unsigned int word1idx;
    unsigned int word2idx;

    for (unsigned int i = 0; i < numTasks; ++i) {
        wordclass1 = _rnd.lrand48() % _postings.size();
        wordclass2 = _rnd.lrand48() % _postings.size();
        while (wordclass1 == FakeWordSet::COMMON_WORD &&
               wordclass2 == FakeWordSet::COMMON_WORD &&
               (_rnd.lrand48() % _skipCommonPairsRate) != 0) {
            wordclass1 = _rnd.lrand48() % _postings.size();
            wordclass2 = _rnd.lrand48() % _postings.size();
        }
        word1idx = _rnd.lrand48() % _postings[wordclass1].size();
        word2idx = _rnd.lrand48() % _postings[wordclass2].size();
        FakePosting::SP p1 = _postings[wordclass1][word1idx];
        FakePosting::SP p2 = _postings[wordclass2][word2idx];
        _tasks.push_back(std::make_pair(p1.get(), p2.get()));
    }
}


AndStressMaster::Task *
AndStressMaster::getTask()
{
    Task *result = NULL;
    std::lock_guard<std::mutex> taskGuard(_taskLock);
    if (_taskIdx < _tasks.size()) {
        result = &_tasks[_taskIdx];
        ++_taskIdx;
    } else {
        _workersDone++;
        if (_workersDone == _workers.size())
            _taskCond.notify_all();
    }
    return result;
}

void
AndStressMaster::run()
{
    LOG(info, "AndStressMaster::run");

    std::vector<std::string>::const_iterator pti;
    std::vector<std::string>::const_iterator ptie = _postingTypes.end() ;

    for (pti = _postingTypes.begin(); pti != ptie; ++pti) {
        std::unique_ptr<FPFactory> ff(getFPFactory(*pti, _wordSet.getSchema()));
        makePostingsHelper(ff.get(), *pti, true, false);
        setupTasks(_numTasks);
        double totalTime = 0;
        for (unsigned int loop = 0; loop < _loops; ++loop) {
            totalTime += runWorkers(*pti);
            resetTasks();
        }
        LOG(info, "AndStressMaster::average run elapsed %10.6f s for workers %s format",
            totalTime / _loops, pti->c_str());
        dropPostings();
    }
    FastOS_Thread::Sleep(250);
}


double
AndStressMaster::runWorkers(const std::string &postingFormat)
{
    FastOS_Time tv;
    double before;
    double after;

    tv.SetNow();
    before = tv.Secs();
    unsigned int numWorkers = 8;
    for (unsigned int i = 0; i < numWorkers; ++i)
        _workers.push_back(new AndStressWorker(*this, i));

    for (unsigned int i = 0; i < _workers.size(); ++i)
        _threadPool->NewThread(_workers[i]);
    {
        std::unique_lock<std::mutex> taskGuard(_taskLock);
        while (_workersDone < _workers.size())
            _taskCond.wait(taskGuard);
    }
    tv.SetNow();
    after = tv.Secs();
    LOG(info,
        "AndStressMaster::run elapsed %10.6f s for workers %s format",
        after - before,
        postingFormat.c_str());
    clearPtrVector(_workers);
    _workersDone = 0;
    return after - before;
}


AndStressWorker::AndStressWorker(AndStressMaster &master, unsigned int id)
    : _master(master),
      _id(id)
{
    LOG(debug, "AndStressWorker::AndStressWorker, id=%u", id);
}

AndStressWorker::~AndStressWorker()
{
    LOG(debug, "AndStressWorker::~AndStressWorker, id=%u", _id);
}


static int
highLevelAndPairPostingScan(SearchIterator &sb1,
                            SearchIterator &sb2,
                            uint32_t numDocs, uint64_t *cycles)
{
    uint32_t hits = 0;
    uint64_t before = fastos::ClockSystem::now();
    sb1.initFullRange();
    sb2.initFullRange();
    uint32_t docId = sb1.getDocId();
    while (docId < numDocs) {
        if (sb1.seek(docId)) {
            if (sb2.seek(docId)) {
                ++hits;
                ++docId;
            } else if (docId < sb2.getDocId())
                docId = sb2.getDocId();
            else
                ++docId;
        } else if (docId < sb1.getDocId())
            docId= sb1.getDocId();
        else
            ++docId;
    }
    uint64_t after = fastos::ClockSystem::now();
    *cycles = after - before;
    return hits;
}


static int
highLevelAndPairPostingScanUnpack(SearchIterator &sb1,
                                  SearchIterator &sb2,
                                  uint32_t numDocs,
                                  uint64_t *cycles)
{
    uint32_t hits = 0;
    uint64_t before = fastos::ClockSystem::now();
    sb1.initFullRange();
    sb2.initFullRange();
    uint32_t docId = sb1.getDocId();
    while (docId < numDocs) {
        if (sb1.seek(docId)) {
            if (sb2.seek(docId)) {
                ++hits;
                sb1.unpack(docId);
                sb2.unpack(docId);
                ++docId;
            } else if (docId < sb2.getDocId())
                docId = sb2.getDocId();
            else
                ++docId;
        } else if (docId < sb1.getDocId())
            docId= sb1.getDocId();
        else
            ++docId;
    }
    uint64_t after = fastos::ClockSystem::now();
    *cycles = after - before;
    return hits;
}

void
testFakePair(FakePosting &f1, FakePosting &f2, unsigned int numDocs,
             bool unpack)
{
    TermFieldMatchData md1;
    TermFieldMatchDataArray tfmda1;
    tfmda1.add(&md1);
    std::unique_ptr<SearchIterator> sb1(f1.createIterator(tfmda1));

    TermFieldMatchData md2;
    TermFieldMatchDataArray tfmda2;
    tfmda1.add(&md2);
    std::unique_ptr<SearchIterator> sb2(f2.createIterator(tfmda2));

    int hits = 0;
    uint64_t scanUnpackTime = 0;
    if (unpack)
        hits = highLevelAndPairPostingScanUnpack(*sb1.get(), *sb2.get(),
                numDocs, &scanUnpackTime);
    else
        hits = highLevelAndPairPostingScan(*sb1.get(), *sb2.get(),
                numDocs, &scanUnpackTime);
#if 0
    printf("Fakepair %s AND %s => %d hits, %" PRIu64 " cycles\n",
           f1.getName().c_str(),
           f2.getName().c_str(),
           hits,
           scanUnpackTime);
#else
    (void)hits;
#endif
}

void
AndStressWorker::Run(FastOS_ThreadInterface *thisThread, void *arg)
{
    (void) thisThread;
    (void) arg;
    LOG(debug, "AndStressWorker::Run, id=%u", _id);

    bool unpack = _master.getUnpack();
    for (;;) {
        AndStressMaster::Task *task = _master.getTask();
        if (task == NULL)
            break;
        testFakePair(*task->first, *task->second, _master.getNumDocs(),
                     unpack);
    }
}


AndStress::AndStress()
{
    LOG(debug, "Andstress::AndStress");
}


AndStress::~AndStress()
{
    LOG(debug, "Andstress::~AndStress");
}

void
AndStress::run(search::Rand48 &rnd,
               FakeWordSet &wordSet,
               unsigned int numDocs,
               unsigned int commonDocFreq,
               const std::vector<std::string> &postingTypes,
               unsigned int loops,
               unsigned int skipCommonPairsRate,
               uint32_t numTasks,
               uint32_t stride,
               bool unpack)
{
    LOG(debug, "Andstress::run");
    AndStressMaster master(rnd, wordSet,
                           numDocs, commonDocFreq, postingTypes, loops,
                           skipCommonPairsRate,
                           numTasks,
                           stride,
                           unpack);
    master.run();
}

}
