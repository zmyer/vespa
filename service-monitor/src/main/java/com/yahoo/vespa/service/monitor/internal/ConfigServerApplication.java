// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.service.monitor.internal;

import com.yahoo.vespa.applicationmodel.ApplicationInstance;
import com.yahoo.vespa.applicationmodel.ApplicationInstanceId;
import com.yahoo.vespa.applicationmodel.ClusterId;
import com.yahoo.vespa.applicationmodel.ConfigId;
import com.yahoo.vespa.applicationmodel.HostName;
import com.yahoo.vespa.applicationmodel.ServiceCluster;
import com.yahoo.vespa.applicationmodel.ServiceInstance;
import com.yahoo.vespa.applicationmodel.ServiceStatus;
import com.yahoo.vespa.applicationmodel.ServiceType;
import com.yahoo.vespa.applicationmodel.TenantId;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A service/application model of the config server with health status.
 */
public class ConfigServerApplication {
    public static final ClusterId CLUSTER_ID = new ClusterId("zone-config-servers");
    public static final ServiceType SERVICE_TYPE = new ServiceType("configserver");
    public static final TenantId TENANT_ID = new TenantId("hosted-vespa");
    public static final ApplicationInstanceId APPLICATION_INSTANCE_ID = new ApplicationInstanceId("zone-config-servers");
    public static final String CONFIG_ID_PREFIX = "configid.";

    ApplicationInstance toApplicationInstance(List<String> hostnames) {
        Set<ServiceInstance> serviceInstances = hostnames.stream()
                .map(hostname -> new ServiceInstance(
                        new ConfigId(CONFIG_ID_PREFIX + hostname),
                        new HostName(hostname),
                        ServiceStatus.NOT_CHECKED))
                .collect(Collectors.toSet());

        ServiceCluster serviceCluster = new ServiceCluster(
                CLUSTER_ID,
                SERVICE_TYPE,
                serviceInstances);

        Set<ServiceCluster> serviceClusters =
                Stream.of(serviceCluster).collect(Collectors.toSet());

        ApplicationInstance applicationInstance = new ApplicationInstance(
                TENANT_ID,
                APPLICATION_INSTANCE_ID,
                serviceClusters);

        // Fill back-references
        serviceCluster.setApplicationInstance(applicationInstance);
        for (ServiceInstance serviceInstance : serviceCluster.serviceInstances()) {
            serviceInstance.setServiceCluster(serviceCluster);
        }

        return applicationInstance;
    }
}
