// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "fieldwriter.h"
#include "zcposocc.h"
#include "extposocc.h"
#include "pagedict4file.h"
#include <vespa/vespalib/util/error.h>
#include <vespa/log/log.h>

LOG_SETUP(".diskindex.fieldwriter");

namespace search::diskindex {

using vespalib::getLastErrorString;
using common::FileHeaderContext;

FieldWriter::FieldWriter(uint32_t docIdLimit,
                         uint64_t numWordIds)
    : _wordNum(noWordNum()),
      _prevDocId(0),
      _dictFile(),
      _posoccfile(),
      _bvc(docIdLimit),
      _bmapfile(BitVectorKeyScope::PERFIELD_WORDS),
      _docIdLimit(docIdLimit),
      _numWordIds(numWordIds),
      _prefix(),
      _compactWordNum(0),
      _word()
{
}

FieldWriter::~FieldWriter() { }

bool
FieldWriter::open(const vespalib::string &prefix,
                  uint32_t minSkipDocs,
                  uint32_t minChunkDocs,
                  bool dynamicKPosOccFormat,
                  const Schema &schema,
                  const uint32_t indexId,
                  const TuneFileSeqWrite &tuneFileWrite,
                  const FileHeaderContext &fileHeaderContext)
{
    _prefix = prefix;
    vespalib::string name = prefix + "posocc.dat.compressed";

    PostingListParams params;
    PostingListParams featureParams;
    PostingListParams countParams;

    diskindex::setupDefaultPosOccParameters(&countParams,
            &params,
            _numWordIds,
            _docIdLimit);

    if (minSkipDocs != 0) {
        countParams.set("minSkipDocs", minSkipDocs);
        params.set("minSkipDocs", minSkipDocs);
    }
    if (minChunkDocs != 0) {
        countParams.set("minChunkDocs", minChunkDocs);
        params.set("minChunkDocs", minChunkDocs);
    }

    _dictFile = std::make_unique<PageDict4FileSeqWrite>();
    _dictFile->setParams(countParams);

    _posoccfile.reset(diskindex::makePosOccWrite(name,
                                                 _dictFile.get(),
                                                 dynamicKPosOccFormat,
                                                 params,
                                                 featureParams,
                                                 schema,
                                                 indexId,
                                                 tuneFileWrite));
    vespalib::string cname = _prefix + "dictionary";

    // Open output dictionary file
    if (!_dictFile->open(cname, tuneFileWrite, fileHeaderContext)) {
        LOG(error, "Could not open posocc count file %s for write: %s",
            cname.c_str(), getLastErrorString().c_str());
        return false;
    }

    // Open output posocc.dat file
    if (!_posoccfile->open(name, tuneFileWrite, fileHeaderContext)) {
        LOG(error, "Could not open posocc file %s for write: %s",
            name.c_str(), getLastErrorString().c_str());
        return false;
    }

    // Open output boolocc.bdat file
    vespalib::string booloccbidxname = _prefix + "boolocc";
    _bmapfile.open(booloccbidxname.c_str(), _docIdLimit, tuneFileWrite,
                   fileHeaderContext);

    return true;
}


void
FieldWriter::flush()
{
    _posoccfile->flushWord();
    PostingListCounts &counts = _posoccfile->getCounts();
    if (counts._numDocs != 0) {
        assert(_compactWordNum != 0);
        _dictFile->writeWord(_word, counts);
        // Write bitmap entries
        if (_bvc.getCrossedBitVectorLimit())
            _bmapfile.addWordSingle(_compactWordNum, _bvc.getBitVector());
        _bvc.clear();
        counts.clear();
    } else {
        assert(counts._bitLength == 0);
        assert(_bvc.empty());
        assert(_compactWordNum == 0);
    }
}


void
FieldWriter::newWord(uint64_t wordNum, const vespalib::stringref &word)
{
    assert(wordNum <= _numWordIds);
    assert(wordNum != noWordNum());
    assert(wordNum > _wordNum);
    flush();
    _wordNum = wordNum;
    ++_compactWordNum;
    _word = word;
    _prevDocId = 0;
}


void
FieldWriter::newWord(const vespalib::stringref &word)
{
    newWord(_wordNum + 1, word);
}


bool
FieldWriter::close()
{
    bool ret = true;
    flush();
    _wordNum = noWordNum();
    if (_posoccfile) {
        bool closeRes = _posoccfile->close();
        if (!closeRes) {
            LOG(error,
                "Could not close posocc file for write");
            ret = false;
        }
        _posoccfile.reset();
    }
    if (_dictFile) {
        bool closeRes = _dictFile->close();
        if (!closeRes) {
            LOG(error,
                "Could not close posocc count file for write");
            ret = false;
        }
        _dictFile.reset();
    }

    _bmapfile.close();
    return ret;
}


void
FieldWriter::setFeatureParams(const PostingListParams &params)
{
    _posoccfile->setFeatureParams(params);
}


void
FieldWriter::getFeatureParams(PostingListParams &params)
{
    _posoccfile->getFeatureParams(params);
}


static const char *termOccNames[] =
{
    "boolocc.bdat",
    "boolocc.bidx",
    "boolocc.idx",
    "posocc.ccnt",
    "posocc.cnt",
    "posocc.dat.compressed",
    "dictionary.pdat",
    "dictionary.spdat",
    "dictionary.ssdat",
    "dictionary.words",
    NULL,
};


void
FieldWriter::remove(const vespalib::string &prefix)
{
    for (const char **j = termOccNames; *j != NULL; ++j) {
        vespalib::string tmpName = prefix + *j;
        FastOS_File::Delete(tmpName.c_str());
    }
}

}
