// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.service.monitor.internal;

import com.yahoo.jdisc.Timer;
import com.yahoo.vespa.service.monitor.ServiceModel;

import java.util.function.Supplier;

public class ServiceModelCache implements Supplier<ServiceModel> {
    public static final long EXPIRY_MILLIS = 10000;

    private final Supplier<ServiceModel> expensiveSupplier;
    private final Timer timer;

    private volatile ServiceModel snapshot;
    private boolean updatePossiblyInProgress = false;

    private final Object updateMonitor = new Object();
    private long snapshotMillis;

    public ServiceModelCache(Supplier<ServiceModel> expensiveSupplier, Timer timer) {
        this.expensiveSupplier = expensiveSupplier;
        this.timer = timer;
    }

    @Override
    public ServiceModel get() {
        if (snapshot == null) {
            synchronized (updateMonitor) {
                if (snapshot == null) {
                    takeSnapshot();
                }
            }
        } else if (expired()) {
            synchronized (updateMonitor) {
                if (updatePossiblyInProgress) {
                    return snapshot;
                }

                updatePossiblyInProgress = true;
            }

            takeSnapshot();

            synchronized (updateMonitor) {
                updatePossiblyInProgress = false;
            }
        }

        return snapshot;
    }

    private void takeSnapshot() {
        snapshot = expensiveSupplier.get();
        snapshotMillis = timer.currentTimeMillis();
    }

    private boolean expired() {
        return timer.currentTimeMillis() - snapshotMillis >= EXPIRY_MILLIS;
    }
}
