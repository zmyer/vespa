// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.service.monitor.internal;

import com.yahoo.config.model.api.ApplicationInfo;
import com.yahoo.config.model.api.SuperModel;
import com.yahoo.vespa.applicationmodel.ConfigId;
import com.yahoo.vespa.applicationmodel.ServiceStatus;
import com.yahoo.vespa.applicationmodel.ServiceType;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlobrokMonitorManagerImplImplTest {
    // IntelliJ complains if parametrized type is specified, Maven complains if not specified.
    @SuppressWarnings("unchecked")
    private final Supplier<SlobrokMonitor> slobrokMonitorFactory = mock(Supplier.class);

    private final SlobrokMonitorManagerImpl slobrokMonitorManager =
            new SlobrokMonitorManagerImpl(slobrokMonitorFactory);
    private final SlobrokMonitor slobrokMonitor = mock(SlobrokMonitor.class);
    private final SuperModel superModel = mock(SuperModel.class);
    private final ApplicationInfo application = mock(ApplicationInfo.class);

    @Before
    public void setup() {
        when(slobrokMonitorFactory.get()).thenReturn(slobrokMonitor);
    }

    @Test
    public void testActivationOfApplication() {
        slobrokMonitorManager.applicationActivated(superModel, application);
        verify(slobrokMonitorFactory, times(1)).get();
    }

    @Test
    public void testGetStatus_ApplicationNotInSlobrok() {
        when(slobrokMonitor.registeredInSlobrok("config.id")).thenReturn(true);
        assertEquals(ServiceStatus.DOWN, getStatus("topleveldispatch"));
    }

    @Test
    public void testGetStatus_ApplicationInSlobrok() {
        slobrokMonitorManager.applicationActivated(superModel, application);
        when(slobrokMonitor.registeredInSlobrok("config.id")).thenReturn(true);
        assertEquals(ServiceStatus.UP, getStatus("topleveldispatch"));
    }

    @Test
    public void testGetStatus_ServiceNotInSlobrok() {
        slobrokMonitorManager.applicationActivated(superModel, application);
        when(slobrokMonitor.registeredInSlobrok("config.id")).thenReturn(false);
        assertEquals(ServiceStatus.DOWN, getStatus("topleveldispatch"));
    }

    @Test
    public void testGetStatus_NotChecked() {
        assertEquals(ServiceStatus.NOT_CHECKED, getStatus("slobrok"));
        verify(slobrokMonitor, times(0)).registeredInSlobrok(any());
    }

    private ServiceStatus getStatus(String serviceType) {
        return slobrokMonitorManager.getStatus(
                application.getApplicationId(),
                new ServiceType(serviceType),
                new ConfigId("config.id"));
    }

    @Test
    public void testLookup() {
        assertEquals(
                Optional.of("config.id"),
                findSlobrokServiceName("topleveldispatch", "config.id"));

        assertEquals(
                Optional.empty(),
                findSlobrokServiceName("adminserver", "config.id"));
    }

    private Optional<String> findSlobrokServiceName(String serviceType, String configId) {
        return slobrokMonitorManager.findSlobrokServiceName(
                new ServiceType(serviceType),
                new ConfigId(configId));
    }
}
