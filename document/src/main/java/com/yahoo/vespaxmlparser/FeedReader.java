// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespaxmlparser;

import com.yahoo.vespaxmlparser.VespaXMLFeedReader.Operation;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Minimal interface for reading operations from a stream for a feeder.
 *
 * Interface extracted from VespaXMLFeedReader to enable JSON feeding.
 *
 * @author steinar
 */
public interface FeedReader {
    static final Semaphore numCores = new Semaphore(Runtime.getRuntime().availableProcessors(), true);

    /**
     * Reads the next operation from the stream.
     * @return A future operation.
     */
    public abstract Future<Operation> readOne() throws Exception;

    default Future<Operation> read() throws Exception {
        numCores.acquire();
        try {
            return readOne();
        } finally {
            numCores.release();
        }
    }

}
