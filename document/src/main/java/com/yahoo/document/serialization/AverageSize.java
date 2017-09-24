package com.yahoo.document.serialization;

public class AverageSize {
    private long size = 0;
    private long count = 1;
    public AverageSize(int initialSize) {
        size = initialSize;
    }
    public void updateAverage(int sz) {
        size += sz;
        count++;
    }
    public int getValue() {
        return (int)(size/count);
    }
}
