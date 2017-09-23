package com.yahoo.document.serialization;

import com.yahoo.io.GrowableByteBuffer;

public class ReuseableGrowableBuffer extends ReusableBuffer<GrowableByteBuffer> {
    public ReuseableGrowableBuffer(int initialSize, int reuseFactor) {
        super(initialSize, reuseFactor);
    }

    @Override
    protected GrowableByteBuffer allocate(int size) {
        return new GrowableByteBuffer(size);
    }
}
