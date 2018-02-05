// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.rotation;

import com.yahoo.vespa.curator.Lock;

import java.util.Objects;

/**
 * A lock for the rotation repository. This is a type-safe wrapper for a curator lock.
 *
 * @author mpolden
 */
public class RotationLock implements AutoCloseable {

    private final Lock lock;

    RotationLock(Lock lock) {
        this.lock = Objects.requireNonNull(lock, "lock cannot be null");
    }

    @Override
    public void close() {
        lock.close();
    }
}
