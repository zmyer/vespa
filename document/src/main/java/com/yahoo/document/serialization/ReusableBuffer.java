package com.yahoo.document.serialization;

import java.nio.ByteBuffer;

public abstract class ReusableBuffer<T> {
    private final AverageSize average;
    private T buffer;
    private final int reuseFactor;
    public ReusableBuffer(int initialSize, int reuseFactor) {
        average = new AverageSize(initialSize);
        buffer = allocate(initialSize);
        this.reuseFactor = reuseFactor;
    }
    public T alloc() {
        T buf = (buffer == null) ? allocate(average.getValue()*2) : buffer;
        buffer = null;
        return buf;
    }
    public void free(T buffer, int used) {
        if (this.buffer == null) {
            average.updateAverage(used);
            if (average.getValue() * reuseFactor > used) {
                this.buffer = clear(buffer);
            } else {
                this.buffer = allocate(average.getValue()*2);
            }
        } else {
            throw new IllegalStateException("Buffer is already present. What have you done .....");
        }
    }
    protected abstract T allocate(int size);
    protected abstract T clear(T buffer);

}
