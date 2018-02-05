// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "fakeword.h"

#include <vespa/searchlib/index/postinglistfile.h>
#include <vespa/searchlib/index/postinglistcountfile.h>
#include <vespa/searchlib/index/docidandfeatures.h>
#include <vespa/searchlib/bitcompression/compression.h>
#include <vespa/searchlib/bitcompression/posocccompression.h>

using search::fef::TermFieldMatchData;
using search::fef::TermFieldMatchDataPosition;
using search::index::WordDocElementFeatures;
using search::index::WordDocElementWordPosFeatures;
using search::index::PostingListFileSeqWrite;
using search::index::DocIdAndFeatures;
using search::index::DocIdAndPosOccFeatures;
using search::index::PostingListCounts;
using search::index::PostingListFileSeqRead;
using search::diskindex::FieldReader;
using search::diskindex::FieldWriter;

namespace search
{

namespace fakedata
{


static void
fillbitset(search::BitVector *bitvector,
           unsigned int size,
           search::Rand48 &rnd)
{
    unsigned int range;
    unsigned int idx;
    unsigned int j;

    range = bitvector->size();
    assert(range > 0);
    --range;
    bitvector->invalidateCachedCount();

    assert(size <= range);
    if (size > range / 2) {
        if (range > 0)
            bitvector->setInterval(1, range);

        for (j = range; j > size; --j) {
            do {
                idx = (rnd.lrand48() % range) + 1u;
            } while (!bitvector->testBit(idx));
            bitvector->clearBit(idx);
        }
    } else {
        // bitvector->reset();
        bitvector->invalidateCachedCount();
        for (j = bitvector->countTrueBits(); j < size; j++) {
            do {
                idx = (rnd.lrand48() % range) + 1u;
            } while (bitvector->testBit(idx));
            bitvector->setBit(idx);
        }
    }
}


static void
fillcorrelatedbitset(search::BitVector &bitvector,
                     unsigned int size,
                     const FakeWord &otherword,
                     search::Rand48 &rnd)
{
    const FakeWord::DocWordFeatureList &opostings = otherword._postings;

    unsigned int range = opostings.size();
    search::BitVector::UP corrmap(search::BitVector::create(range + 1));

    if (size > range)
        size = range;
    fillbitset(corrmap.get(), size, rnd);

    unsigned int idx = corrmap->getNextTrueBit(1u);
    while (idx < range) {
        unsigned int docId = opostings[idx - 1]._docId;
        bitvector.setBit(docId);
        ++idx;
        if (idx > range)
            break;
        idx = corrmap->getNextTrueBit(idx);
    }
}


FakeWord::DocWordPosFeature::DocWordPosFeature()
    : _elementId(0),
      _wordPos(0),
      _elementWeight(1),
      _elementLen(0)
{
}


FakeWord::DocWordPosFeature::~DocWordPosFeature()
{
}


FakeWord::DocWordCollapsedFeature::DocWordCollapsedFeature()
{
}


FakeWord::DocWordCollapsedFeature::~DocWordCollapsedFeature()
{
}


FakeWord::DocWordFeature::DocWordFeature()
    : _docId(0),
      _collapsedDocWordFeatures(),
      _positions(0),
      _accPositions(0)
{
}

FakeWord::DocWordFeature::~DocWordFeature()
{
}

FakeWord::FakeWord(uint32_t docIdLimit,
                   const std::vector<uint32_t> & docIds,
                   const std::string &name,
                   const PosOccFieldsParams &fieldsParams,
                   uint32_t packedIndex)
    : _postings(),
      _wordPosFeatures(),
      _extraPostings(),
      _extraWordPosFeatures(),
      _docIdLimit(docIdLimit),
      _name(name),
      _fieldsParams(fieldsParams),
      _packedIndex(packedIndex)
{
    search::BitVector::UP bitmap(search::BitVector::create(docIdLimit));
    for (uint32_t docId : docIds) {
        bitmap->setBit(docId);
    }
    search::Rand48 rnd;
    fakeup(*bitmap, rnd, _postings, _wordPosFeatures);
}

FakeWord::FakeWord(uint32_t docIdLimit,
                   uint32_t wordDocs,
                   uint32_t tempWordDocs,
                   const std::string &name,
                   search::Rand48 &rnd,
                   const PosOccFieldsParams &fieldsParams,
                   uint32_t packedIndex)
    : _postings(),
      _wordPosFeatures(),
      _extraPostings(),
      _extraWordPosFeatures(),
      _docIdLimit(docIdLimit),
      _name(name),
      _fieldsParams(fieldsParams),
      _packedIndex(packedIndex)
{
    search::BitVector::UP bitmap(search::BitVector::create(docIdLimit));

    fillbitset(bitmap.get(), wordDocs, rnd);

    fakeup(*bitmap, rnd, _postings, _wordPosFeatures);
    fakeupTemps(rnd, docIdLimit, tempWordDocs);
    setupRandomizer(rnd);
}


FakeWord::FakeWord(uint32_t docIdLimit,
                   uint32_t wordDocs,
                   uint32_t tempWordDocs,
                   const std::string &name,
                   const FakeWord &otherWord,
                   size_t overlapDocs,
                   search::Rand48 &rnd,
                   const PosOccFieldsParams &fieldsParams,
                   uint32_t packedIndex)
    : _postings(),
      _wordPosFeatures(),
      _docIdLimit(docIdLimit),
      _name(name),
      _fieldsParams(fieldsParams),
      _packedIndex(packedIndex)
{
    search::BitVector::UP bitmap(search::BitVector::create(docIdLimit));

    if (wordDocs * 2 < docIdLimit &&
        overlapDocs > 0)
        fillcorrelatedbitset(*bitmap, overlapDocs, otherWord, rnd);
    fillbitset(bitmap.get(), wordDocs, rnd);

    fakeup(*bitmap, rnd, _postings, _wordPosFeatures);
    fakeupTemps(rnd, docIdLimit, tempWordDocs);
    setupRandomizer(rnd);
}


FakeWord::~FakeWord()
{
}


void
FakeWord::fakeup(search::BitVector &bitmap,
                 search::Rand48 &rnd,
                 DocWordFeatureList &postings,
                 DocWordPosFeatureList &wordPosFeatures)
{
    DocWordPosFeatureList wpf;
    unsigned int idx;
    uint32_t numFields = _fieldsParams.getNumFields();
    assert(numFields == 1u);
    (void) numFields;
    uint32_t docIdLimit = bitmap.size();
    idx = bitmap.getNextTrueBit(1u);
    while (idx < docIdLimit) {
        DocWordFeature dwf;
        unsigned int positions;

        dwf._docId = idx;
        positions = ((rnd.lrand48() % 10) == 0) ? 2 : 1;
        dwf._positions = positions;
        wpf.clear();
        for (unsigned int j = 0; j < positions; ++j) {
            DocWordPosFeature dwpf;
            dwpf._wordPos = rnd.lrand48() % 8192;
            dwpf._elementId = 0;
            if (_fieldsParams.getFieldParams()[0]._hasElements)
                dwpf._elementId = rnd.lrand48() % 4;
            wpf.push_back(dwpf);
        }
        if (positions > 1) {
            /* Sort wordpos list and "avoid" duplicate positions */
            std::sort(wpf.begin(), wpf.end());
        }
        do {
            DocWordPosFeatureList::iterator ie(wpf.end());
            DocWordPosFeatureList::iterator i(wpf.begin());
            while (i != ie) {
                uint32_t lastwordpos = i->_wordPos;
                DocWordPosFeatureList::iterator pi(i);
                ++i;
                while (i != ie &&
                       pi->_elementId == i->_elementId) {
                    if (i->_wordPos <= lastwordpos)
                        i->_wordPos = lastwordpos + 1;
                    lastwordpos = i->_wordPos;
                    ++i;
                }
                uint32_t elementLen = (rnd.lrand48() % 8192) + 1 + lastwordpos;
                int32_t elementWeight = 1;
                if (_fieldsParams.getFieldParams()[0].
                    _hasElementWeights) {
                    uint32_t uWeight = rnd.lrand48() % 2001;
                    if ((uWeight & 1) != 0)
                        elementWeight = - (uWeight >> 1) - 1;
                    else
                        elementWeight = (uWeight >> 1);
                    assert(elementWeight <= 1000);
                    assert(elementWeight >= -1000);
                }
                while (pi != i) {
                    pi->_elementLen = elementLen;
                    pi->_elementWeight = elementWeight;
                    ++pi;
                }
            }
        } while (0);
        dwf._accPositions = wordPosFeatures.size();
        assert(dwf._positions == wpf.size());
        postings.push_back(dwf);
        DocWordPosFeatureList::iterator ie(wpf.end());
        DocWordPosFeatureList::iterator i(wpf.begin());
        while (i != ie) {
            wordPosFeatures.push_back(*i);
            ++i;
        }
        ++idx;
        if (idx >= docIdLimit)
            break;
        idx = bitmap.getNextTrueBit(idx);
    }
}


void
FakeWord::fakeupTemps(search::Rand48 &rnd,
                      uint32_t docIdLimit,
                      uint32_t tempWordDocs)
{
    uint32_t maxTempWordDocs = docIdLimit / 2;
    tempWordDocs = std::min(tempWordDocs, maxTempWordDocs);
    if (tempWordDocs > 0) {
        search::BitVector::UP bitmap(search::BitVector::create(docIdLimit));
        fillbitset(bitmap.get(), tempWordDocs, rnd);
        fakeup(*bitmap, rnd, _extraPostings, _extraWordPosFeatures);
    }
}

void
FakeWord::setupRandomizer(search::Rand48 &rnd)
{
    typedef DocWordFeatureList DWFL;
    Randomizer randomAdd;
    Randomizer randomRem;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    int32_t ref = 0;

    while (d != de) {
        do {
            randomAdd._random = rnd.lrand48();
        } while (randomAdd._random < 10000);
        randomAdd._ref = ref;
        assert(!randomAdd.isExtra());
        assert(!randomAdd.isRemove());
        _randomizer.push_back(randomAdd);
        ++d;
        ++ref;
    }

    DWFL::const_iterator ed(_extraPostings.begin());
    DWFL::const_iterator ede(_extraPostings.end());

    int32_t eref = -1;
    uint32_t tref = 0;
    ref = 0;
    int32_t refmax = _randomizer.size();
    while (ed != ede) {
        while (ref < refmax && _postings[ref]._docId < ed->_docId)
            ++ref;
        if (ref < refmax && _postings[ref]._docId == ed->_docId) {
            randomAdd._random = rnd.lrand48() % (_randomizer[ref]._random - 1);
            randomRem._random = _randomizer[ref]._random - 1;
        } else {
            do {
                randomAdd._random = rnd.lrand48();
                randomRem._random = rnd.lrand48();
            } while (randomAdd._random >= randomRem._random);
        }
        randomAdd._ref = eref;
        randomRem._ref = eref - 1;
        assert(randomAdd.isExtra());
        assert(!randomAdd.isRemove());
        assert(randomAdd.extraIdx() == tref);
        assert(randomRem.isExtra());
        assert(randomRem.isRemove());
        assert(randomRem.extraIdx() == tref);
        _randomizer.push_back(randomAdd);
        _randomizer.push_back(randomRem);
        ++ed;
        eref -= 2;
        ++tref;
    }
    std::sort(_randomizer.begin(), _randomizer.end());
}


void
FakeWord::addDocIdBias(uint32_t docIdBias)
{
    typedef DocWordFeatureList DWFL;
    DWFL::iterator d(_postings.begin());
    DWFL::iterator de(_postings.end());
    for (; d != de; ++d) {
        d->_docId += docIdBias;
    }
    d = _extraPostings.begin();
    de = _extraPostings.end();
    for (; d != de; ++d) {
        d->_docId += docIdBias;
    }
    _docIdLimit += docIdBias;
}


bool
FakeWord::validate(search::queryeval::SearchIterator *iterator,
                   const fef::TermFieldMatchDataArray &matchData,
                   uint32_t stride,
                   bool verbose) const
{
    iterator->initFullRange();
    uint32_t docId = 0;

    typedef DocWordFeatureList DWFL;
    typedef DocWordPosFeatureList DWPFL;
    typedef TermFieldMatchData::PositionsIterator TMDPI;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    DWPFL::const_iterator p(_wordPosFeatures.begin());
    DWPFL::const_iterator pe(_wordPosFeatures.end());

    if (verbose)
        printf("Start validate word '%s'\n", _name.c_str());
    int strideResidue = stride;
    while (d != de) {
        if (strideResidue > 1) {
            --strideResidue;
            unsigned int positions = d->_positions;
            while (positions > 0) {
                ++p;
                --positions;
            }
        } else {
            strideResidue = stride;
            docId = d->_docId;
            bool seekRes = iterator->seek(docId);
            assert(seekRes);
            (void) seekRes;
            assert(d != de);
            unsigned int positions = d->_positions;
            iterator->unpack(docId);
            for (size_t lfi = 0; lfi < matchData.size(); ++lfi) {
                if (matchData[lfi]->getDocId() != docId)
                    continue;
                TMDPI mdpe = matchData[lfi]->end();
                TMDPI mdp = matchData[lfi]->begin();
                while (mdp != mdpe) {
                    assert(p != pe);
                    assert(positions > 0);
                    assert(p->_wordPos == mdp->getPosition());
                    assert(p->_elementId == mdp->getElementId());
                    assert(p->_elementWeight == mdp->getElementWeight());
                    assert(p->_elementLen == mdp->getElementLen());
                    ++p;
                    ++mdp;
                    --positions;
                }
            }
            assert(positions == 0);
        }
        ++d;
    }
    assert(p == pe);
    assert(d == de);
    if (verbose)
        printf("word '%s' validated successfully with unpack\n",
               _name.c_str());
    return true;
}


bool
FakeWord::validate(search::queryeval::SearchIterator *iterator,
                   const fef::TermFieldMatchDataArray &matchData,
                   bool verbose) const
{
    iterator->initFullRange();
    uint32_t docId = 1;

    typedef DocWordFeatureList DWFL;
    typedef DocWordPosFeatureList DWPFL;
    typedef TermFieldMatchData::PositionsIterator TMDPI;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    DWPFL::const_iterator p(_wordPosFeatures.begin());
    DWPFL::const_iterator pe(_wordPosFeatures.end());

    if (verbose)
        printf("Start validate word '%s'\n", _name.c_str());
    for (;;) {
        if (iterator->seek(docId)) {
            assert(d != de);
            assert(d->_docId == docId);
            iterator->unpack(docId);
            unsigned int positions = d->_positions;
            for (size_t lfi = 0; lfi < matchData.size(); ++lfi) {
                if (matchData[lfi]->getDocId() != docId)
                    continue;
                TMDPI mdpe = matchData[lfi]->end();
                TMDPI mdp = matchData[lfi]->begin();
                while (mdp != mdpe) {
                    assert(p != pe);
                    assert(positions > 0);
                    assert(p->_wordPos == mdp->getPosition());
                    assert(p->_elementId == mdp->getElementId());
                    assert(p->_elementWeight == mdp->getElementWeight());
                    assert(p->_elementLen == mdp->getElementLen());
                    ++p;
                    ++mdp;
                    --positions;
                }
            }
            assert(positions == 0);
            ++d;
            ++docId;
        } else {
            if (iterator->getDocId() > docId)
                docId = iterator->getDocId();
            else
                ++docId;
        }
        if (docId >= _docIdLimit)
            break;
    }
    assert(p == pe);
    assert(d == de);
    if (verbose)
        printf("word '%s' validated successfully with unpack\n",
               _name.c_str());
    return true;
}


bool
FakeWord::validate(search::queryeval::SearchIterator *iterator, bool verbose) const
{
    iterator->initFullRange();
    uint32_t docId = 1;

    typedef DocWordFeatureList DWFL;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());

    if (verbose)
        printf("Start validate word '%s'\n", _name.c_str());
    for (;;) {
        if (iterator->seek(docId)) {
            assert(d != de);
            assert(d->_docId == docId);
            ++d;
            ++docId;
        } else {
            if (iterator->getDocId() > docId)
                docId = iterator->getDocId();
            else
                ++docId;
        }
        if (docId >= _docIdLimit)
            break;
    }
    assert(d == de);
    if (verbose)
        printf("word '%s' validated successfully without unpack\n",
               _name.c_str());
    return true;
}


bool
FakeWord::validate(FieldReader &fieldReader,
                   uint32_t wordNum,
                   const fef::TermFieldMatchDataArray &matchData,
                   bool verbose) const
{
    uint32_t docId = 0;
    uint32_t numDocs;
    uint32_t residue;
    uint32_t presidue;
    bool unpres;

    typedef DocWordFeatureList DWFL;
    typedef DocWordPosFeatureList DWPFL;
    typedef TermFieldMatchData::PositionsIterator TMDPI;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    DWPFL::const_iterator p(_wordPosFeatures.begin());
    DWPFL::const_iterator pe(_wordPosFeatures.end());

    if (verbose)
        printf("Start validate word '%s'\n", _name.c_str());
#ifdef notyet
    // Validate word number
#else
    (void) wordNum;
#endif
    numDocs = _postings.size();
    for (residue = numDocs; residue > 0; --residue) {
        assert(fieldReader._wordNum == wordNum);
        DocIdAndFeatures &features(fieldReader._docIdAndFeatures);
        docId = features._docId;
        assert(d != de);
        assert(d->_docId == docId);
        if (matchData.valid()) {
#ifdef notyet
            unpres = features.unpack(matchData);
            assert(unpres);
#else
            (void) unpres;

            typedef WordDocElementFeatures Elements;
            typedef WordDocElementWordPosFeatures Positions;

            std::vector<Elements>::const_iterator element =
                features._elements.begin();
            std::vector<Positions>::const_iterator position =
                features._wordPositions.begin();

            TermFieldMatchData *tfmd = matchData[0];
            assert(tfmd != 0);
            tfmd->reset(features._docId);

            uint32_t elementResidue = features._elements.size();
            while (elementResidue != 0) {
                uint32_t positionResidue = element->getNumOccs();
                while (positionResidue != 0) {
                    uint32_t wordPos = position->getWordPos();
                    TermFieldMatchDataPosition pos(element->getElementId(),
                                                   wordPos,
                                                   element->getWeight(),
                                                   element->getElementLen());
                    tfmd->appendPosition(pos);
                    ++position;
                    --positionResidue;
                }
                ++element;
                --elementResidue;
            }
#endif
            unsigned int positions = d->_positions;
            presidue = positions;
            for (size_t lfi = 0; lfi < matchData.size(); ++lfi) {
                if (matchData[lfi]->getDocId() != docId)
                    continue;
                TMDPI mdpe = matchData[lfi]->end();
                TMDPI mdp = matchData[lfi]->begin();
                while (mdp != mdpe) {
                    assert(p != pe);
                    assert(presidue > 0);
                    assert(p->_wordPos == mdp->getPosition());
                    assert(p->_elementId == mdp->getElementId());
                    assert(p->_elementWeight == mdp->getElementWeight());
                    assert(p->_elementLen == mdp->getElementLen());
                    ++p;
                    ++mdp;
                    --presidue;
                }
            }
            assert(presidue == 0);
            ++d;
        }
        fieldReader.read();
    }
    if (matchData.valid()) {
        assert(p == pe);
        assert(d == de);
    }
    if (verbose)
        printf("word '%s' validated successfully %s unpack\n",
               _name.c_str(),
               matchData.valid() ? "with" : "without");
    return true;
}


void
FakeWord::validate(const std::vector<uint32_t> &docIds) const
{
    typedef DocWordFeatureList DWFL;
    typedef std::vector<uint32_t> DL;
    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    DL::const_iterator di(docIds.begin());
    DL::const_iterator die(docIds.end());

    while (d != de) {
        assert(di != die);
        assert(d->_docId == *di);
        ++d;
        ++di;
    }
    assert(di == die);
}


void
FakeWord::validate(const search::BitVector &bv) const
{
    typedef DocWordFeatureList DWFL;
    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    uint32_t bitHits = bv.countTrueBits();
    assert(bitHits == _postings.size());
    (void) bitHits;
    uint32_t bi = bv.getNextTrueBit(1u);
    while (d != de) {
        assert(d->_docId == bi);
        ++d;
        bi = bv.getNextTrueBit(bi + 1);
    }
    assert(bi >= bv.size());
}


bool
FakeWord::dump(FieldWriter &fieldWriter,
               bool verbose) const
{
    uint32_t numDocs;
    uint32_t residue;
    DocIdAndPosOccFeatures features;

    typedef DocWordFeatureList DWFL;
    typedef DocWordPosFeatureList DWPFL;

    DWFL::const_iterator d(_postings.begin());
    DWFL::const_iterator de(_postings.end());
    DWPFL::const_iterator p(_wordPosFeatures.begin());
    DWPFL::const_iterator pe(_wordPosFeatures.end());

    if (verbose)
        printf("Start dumping word '%s'\n", _name.c_str());
    numDocs = _postings.size();
    for (residue = numDocs; residue > 0; --residue) {
        assert(d != de);
        setupFeatures(*d, &*p, features);
        p += d->_positions;
        fieldWriter.add(features);
        ++d;
    }
    assert(p == pe);
    assert(d == de);
    if (verbose)
        printf("word '%s' dumped successfully\n",
               _name.c_str());
    return true;
}


FakeWord::RandomizedReader::RandomizedReader()
    : _r(),
      _fw(NULL),
      _wordIdx(0u),
      _valid(false),
      _ri(),
      _re()
{
}


void
FakeWord::RandomizedReader::read()
{
    if (_ri != _re) {
        _r = *_ri;
        ++_ri;
    } else
        _valid = false;
}


void
FakeWord::RandomizedReader::setup(const FakeWord *fw,
                                  uint32_t wordIdx)
{
    _fw = fw;
    _wordIdx = wordIdx;
    _ri = fw->_randomizer.begin();
    _re = fw->_randomizer.end();
    _valid = _ri != _re;
}


FakeWord::RandomizedWriter::~RandomizedWriter()
{
}


} // namespace fakedata

} // namespace search
