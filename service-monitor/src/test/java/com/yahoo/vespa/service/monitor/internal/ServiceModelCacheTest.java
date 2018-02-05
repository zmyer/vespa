// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.service.monitor.internal;

import com.yahoo.jdisc.Timer;
import com.yahoo.vespa.service.monitor.ServiceModel;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceModelCacheTest {
    @SuppressWarnings("unchecked")
    private final Supplier<ServiceModel> rawSupplier = mock(Supplier.class);
    private final Timer timer = mock(Timer.class);
    private final ServiceModelCache cache = new ServiceModelCache(rawSupplier, timer);

    @Test
    public void sanityCheck() {
        ServiceModel serviceModel = mock(ServiceModel.class);
        when(rawSupplier.get()).thenReturn(serviceModel);

        long timeMillis = 0;
        when(timer.currentTimeMillis()).thenReturn(timeMillis);

        // Will always populate cache the first time
        ServiceModel actualServiceModel = cache.get();
        assertTrue(actualServiceModel == serviceModel);
        verify(rawSupplier, times(1)).get();

        // Cache hit
        timeMillis += ServiceModelCache.EXPIRY_MILLIS / 2;
        when(timer.currentTimeMillis()).thenReturn(timeMillis);
        actualServiceModel = cache.get();
        assertTrue(actualServiceModel == serviceModel);

        // Cache expired
        timeMillis += ServiceModelCache.EXPIRY_MILLIS + 1;
        when(timer.currentTimeMillis()).thenReturn(timeMillis);

        ServiceModel serviceModel2 = mock(ServiceModel.class);
        when(rawSupplier.get()).thenReturn(serviceModel2);

        actualServiceModel = cache.get();
        assertTrue(actualServiceModel == serviceModel2);
        // '2' because it's cumulative with '1' from the first times(1).
        verify(rawSupplier, times(2)).get();

        // Cache hit #2
        timeMillis += 1;
        when(timer.currentTimeMillis()).thenReturn(timeMillis);
        actualServiceModel = cache.get();
        assertTrue(actualServiceModel == serviceModel2);
    }
}