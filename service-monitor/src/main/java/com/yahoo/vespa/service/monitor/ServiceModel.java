// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.service.monitor;

import com.yahoo.vespa.applicationmodel.ApplicationInstance;
import com.yahoo.vespa.applicationmodel.ApplicationInstanceReference;
import com.yahoo.vespa.applicationmodel.HostName;
import com.yahoo.vespa.applicationmodel.ServiceInstance;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The ServiceModel is almost a mirror of the SuperModel, except that it
 * also gives ServiceStatus on each service, and there may be
 * artificial applications like the config server "application".
 */
// @Immutable
public class ServiceModel {
    private final Map<ApplicationInstanceReference, ApplicationInstance> applications;

    public ServiceModel(Map<ApplicationInstanceReference, ApplicationInstance> applications) {
        this.applications = Collections.unmodifiableMap(applications);
    }

    public Map<ApplicationInstanceReference, ApplicationInstance> getAllApplicationInstances() {
        return applications;
    }

    public Optional<ApplicationInstance> getApplicationInstance(ApplicationInstanceReference reference) {
        return Optional.ofNullable(applications.get(reference));
    }

    public Map<HostName, List<ServiceInstance>> getServiceInstancesByHostName() {
        return applications.values().stream()
                .flatMap(application -> application.serviceClusters().stream())
                .flatMap(cluster -> cluster.serviceInstances().stream())
                .collect(Collectors.groupingBy(service -> service.hostName(), Collectors.toList()));
    }
}
