// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.orchestrator;

import com.yahoo.vespa.applicationmodel.ConfigId;
import com.yahoo.vespa.applicationmodel.ServiceCluster;
import com.yahoo.vespa.applicationmodel.ServiceInstance;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for creating test setups.
 *
 * @author bakksjo
 */
public class TestUtil {
    @SafeVarargs
    public static Set<ServiceInstance> makeServiceInstanceSet(
            final ServiceInstance... serviceInstances) {
        return new HashSet<>(Arrays.asList(serviceInstances));
    }

    @SafeVarargs
    public static Set<ServiceCluster> makeServiceClusterSet(
            final ServiceCluster... serviceClusters) {
        return new HashSet<>(Arrays.asList(serviceClusters));
    }

    public static ConfigId storageNodeConfigId(String contentClusterName, int index) {
        return new ConfigId(contentClusterName + "/storage/" + index);
    }

    public static ConfigId clusterControllerConfigId(String contentClusterName, int index) {
        return new ConfigId(contentClusterName + "/standalone/" + contentClusterName + "-controllers/" + index);
    }
}
