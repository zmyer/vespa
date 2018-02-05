// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchlib/util/filealign.h>

namespace search {

class ComprBuffer
{
private:
    void allocComprBuf();
public:
    void *_comprBuf;
    size_t _comprBufSize;
    uint32_t _unitSize; // Size of unit in bytes, doubles up as alignment
    bool _padBefore;
    void *_comprBufMalloc;
    FileAlign _aligner;

    ComprBuffer(const ComprBuffer &) = delete;
    ComprBuffer &operator=(const ComprBuffer &) = delete;
    ComprBuffer(uint32_t unitSize);
    virtual ~ComprBuffer();

    void dropComprBuf();

    void allocComprBuf(size_t comprBufSize, size_t preferredFileAlignment,
                       FastOS_FileInterface *const file, bool padbefore);

    static size_t minimumPadding() { return 8; }
    uint32_t getUnitBitSize() const { return _unitSize * 8; }
    bool getPadBefore() const { return _padBefore; }

    /*
     * When encoding to memory instead of file, the compressed buffer must
     * be able to grow.
     */
    void expandComprBuf(uint32_t overflowUnits);

    /*
     * For unit testing only. Reference data owned by rhs, only works as
     * long as rhs is live and unchanged.
     */
    void referenceComprBuf(const ComprBuffer &rhs);
};

}

