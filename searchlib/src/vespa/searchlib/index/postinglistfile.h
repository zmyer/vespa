// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "postinglistcounts.h"
#include "postinglisthandle.h"
#include "postinglistparams.h"
#include <vespa/searchlib/common/tunefileinfo.h>

class FastOS_FileInterface;

namespace search {

namespace common { class FileHeaderContext; }

namespace index {

class DocIdAndFeatures;

/**
 * Interface for posting list files containing document ids and features
 * for words.
 */
class PostingListFileSeqRead
{
protected:
    PostingListCounts _counts;
    unsigned int _residueDocs;  // Docids left to read for word
public:
    PostingListFileSeqRead();

    virtual ~PostingListFileSeqRead();

    /**
     * Read document id and features.
     */
    virtual void readDocIdAndFeatures(DocIdAndFeatures &features) = 0;

    /**
     * Read counts for a word.
     */
    virtual void readCounts(const PostingListCounts &counts) = 0;

    /**
     * Open posting list file for sequential read.
     */
    virtual bool open(const vespalib::string &name, const TuneFileSeqRead &tuneFileRead) = 0;

    /**
     * Close posting list file.
     */
    virtual bool close() = 0;

    /*
     * Get current parameters.
     */
    virtual void getParams(PostingListParams &params);

    /*
     * Set (word, docid) feature parameters.
     *
     * Typically can only enable or disable cooked features.
     */
    virtual void setFeatureParams(const PostingListParams &params);

    /*
     * Get current (word, docid) feature parameters.
     */
    virtual void getFeatureParams(PostingListParams &params);

    // Methods used when generating posting list for common word pairs.

    /*
     * Get current posting offset, measured in bits.  First posting list
     * starts at 0, i.e.  file header is not accounted for here.
     *
     * @return current posting offset, measured in bits.
     */
    virtual uint64_t getCurrentPostingOffset() const = 0;

    /**
     * Set current posting offset, measured in bits.  First posting
     * list starts at 0, i.e.  file header is not accounted for here.
     *
     * @param Offset start of posting lists for word pair.
     * @param endOffset end of posting lists for word pair.
     * @param readAheadOffset end of posting list for either this or a
     *               later word pair, depending on disk seek cost.
     */
    virtual void setPostingOffset(uint64_t offset, uint64_t endOffset, uint64_t readAheadOffset) = 0;

    /**
     * Get counts read by last readCounts().
     */
    const PostingListCounts &getCounts() const { return _counts; }

    PostingListCounts &getCounts() { return _counts; }
};

/**
 * Interface for posting list files containing document ids and features
 * for words.
 */
class PostingListFileSeqWrite
{
protected:
    PostingListCounts _counts;
public:
    PostingListFileSeqWrite();
    virtual ~PostingListFileSeqWrite();

    /**
     * Write document id and features.
     */
    virtual void writeDocIdAndFeatures(const DocIdAndFeatures &features) = 0;

    /**
     * Flush word (during write) after it is complete to buffers, i.e.
     * prepare for next word, but not for application crash.
     */
    virtual void flushWord() = 0;

    /**
     * Open posting list file for sequential write.
     */
    virtual bool
    open(const vespalib::string &name,
         const TuneFileSeqWrite &tuneFileWrite,
         const common::FileHeaderContext &fileHeaderContext) = 0;

    /**
     * Close posting list file.
     */
    virtual bool close() = 0;

    /*
     * Set parameters.
     */
    virtual void setParams(const PostingListParams &params);

    /*
     * Get current parameters.
     */
    virtual void getParams(PostingListParams &params);

    /*
     * Set (word, docid) feature parameters.
     */
    virtual void setFeatureParams(const PostingListParams &params);

    /*
     * Get current (word, docid) feature parameters.
     */
    virtual void getFeatureParams(PostingListParams &params);

    PostingListCounts &getCounts() { return _counts; }
};


/**
 * Interface for posting list files containing document ids and features
 * for words.
 */
class PostingListFileRandRead
{
protected:
    // Can be examined after open
    bool _memoryMapped;
public:
    typedef std::shared_ptr<PostingListFileRandRead> SP;

    PostingListFileRandRead();
    virtual ~PostingListFileRandRead();

    /**
     * Create iterator for single word.  Semantic lifetime of counts and
     * handle must exceed lifetime of iterator.
     *
     * XXX: TODO: How to read next set of segments from disk if handle
     * didn't cover the whole word, probably need access to higher level
     * API above caches.
     */
    virtual search::queryeval::SearchIterator *
    createIterator(const PostingListCounts &counts,
                   const PostingListHandle &handle,
                   const search::fef::TermFieldMatchDataArray &matchData,
                   bool usebitVector) const = 0;


    /**
     * Read (possibly partial) posting list into handle.
     */
    virtual void
    readPostingList(const PostingListCounts &counts,
                    uint32_t firstSegment,
                    uint32_t numSegments,
                    PostingListHandle &handle) = 0;

    /**
     * Open posting list file for random read.
     */
    virtual bool open(const vespalib::string &name, const TuneFileRandRead &tuneFileRead) = 0;

    /**
     * Close posting list file.
     */
    virtual bool close() = 0;

    bool getMemoryMapped() const { return _memoryMapped; }

protected:
    void afterOpen(FastOS_FileInterface &file);
};


/**
 * Passthrough class.
 */
class PostingListFileRandReadPassThrough : public PostingListFileRandRead
{
protected:
    PostingListFileRandRead *_lower;
    bool _ownLower;

public:
    PostingListFileRandReadPassThrough(PostingListFileRandRead *lower, bool ownLower);
    ~PostingListFileRandReadPassThrough();

    search::queryeval::SearchIterator *
    createIterator(const PostingListCounts &counts,
                   const PostingListHandle &handle,
                   const search::fef::TermFieldMatchDataArray &matchData,
                   bool usebitVector) const override;

    void readPostingList(const PostingListCounts &counts, uint32_t firstSegment,
                         uint32_t numSegments, PostingListHandle &handle) override;

    bool open(const vespalib::string &name, const TuneFileRandRead &tuneFileRead) override;
    bool close() override;
};


} // namespace index

} // namespace search
