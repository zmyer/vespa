// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.jdisc.metric;

import com.yahoo.jdisc.Metric;
import com.yahoo.jdisc.statistics.ActiveContainerMetrics;
import org.junit.Test;

import java.time.Duration;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author bjorncs 
 */
public class MetricUpdaterTest {
    
    @Test
    public void metrics_are_updated_in_scheduler_cycle() throws InterruptedException {
        Metric metric = mock(Metric.class);
        ActiveContainerMetrics activeContainerMetrics = mock(ActiveContainerMetrics.class);
        new MetricUpdater(new MockScheduler(), metric, activeContainerMetrics);
        verify(activeContainerMetrics, times(1)).emitMetrics(any());
        verify(metric, times(8)).set(anyString(), any(), any());
    }

    private static class MockScheduler implements MetricUpdater.Scheduler {
        @Override
        public void schedule(Runnable runnable, Duration frequency) {
            runnable.run();
        }
        @Override
        public void cancel() {}
    }

}
