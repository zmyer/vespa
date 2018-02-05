// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.model;

import com.google.common.base.Joiner;
import com.yahoo.cloud.config.LbServicesConfig;
import com.yahoo.config.model.api.ApplicationInfo;
import com.yahoo.config.model.api.HostInfo;
import com.yahoo.config.model.api.ServiceInfo;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.TenantName;
import com.yahoo.config.provision.Zone;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Produces lb-services cfg
 *
 * @author vegardh
 * @since 5.9
 */
public class LbServicesProducer implements LbServicesConfig.Producer {

    private final Map<TenantName, Map<ApplicationId, ApplicationInfo>> models;
    private final Zone zone;

    public LbServicesProducer(Map<TenantName, Map<ApplicationId, ApplicationInfo>> models, Zone zone) {
        this.models = models;
        this.zone = zone;
    }

    @Override
    public void getConfig(LbServicesConfig.Builder builder) {
        models.keySet().stream()
                .sorted()
                .forEach(tenant -> {
            builder.tenants(tenant.value(), getTenantConfig(models.get(tenant)));
        });
    }

    private LbServicesConfig.Tenants.Builder getTenantConfig(Map<ApplicationId, ApplicationInfo> apps) {
        LbServicesConfig.Tenants.Builder tb = new LbServicesConfig.Tenants.Builder();
        apps.keySet().stream()
                .sorted()
                .forEach(applicationId -> {
            tb.applications(createLbAppIdKey(applicationId), getAppConfig(apps.get(applicationId)));
        });
        return tb;
    }

    private String createLbAppIdKey(ApplicationId applicationId) {
        return applicationId.application().value() + ":" + zone.environment().value() + ":" + zone.region().value() + ":" + applicationId.instance().value();
    }

    private LbServicesConfig.Tenants.Applications.Builder getAppConfig(ApplicationInfo app) {
        LbServicesConfig.Tenants.Applications.Builder ab = new LbServicesConfig.Tenants.Applications.Builder();
        ab.activeRotation(getActiveRotation(app));
        app.getModel().getHosts().stream()
                .sorted((a, b) -> a.getHostname().compareTo(b.getHostname()))
                .forEach(hostInfo -> {
            ab.hosts(hostInfo.getHostname(), getHostsConfig(hostInfo));
        });
        return ab;
    }

    private boolean getActiveRotation(ApplicationInfo app) {
        boolean activeRotation = false;
        for (HostInfo hostInfo : app.getModel().getHosts()) {
            final Optional<ServiceInfo> container = hostInfo.getServices().stream().filter(
                    serviceInfo -> serviceInfo.getServiceType().equals("container") ||
                            serviceInfo.getServiceType().equals("qrserver")).
                    findAny();
            if (container.isPresent()) {
                activeRotation |= Boolean.valueOf(container.get().getProperty("activeRotation").orElse("false"));
            }
        }
        return activeRotation;
    }

    private LbServicesConfig.Tenants.Applications.Hosts.Builder getHostsConfig(HostInfo hostInfo) {
        LbServicesConfig.Tenants.Applications.Hosts.Builder hb = new LbServicesConfig.Tenants.Applications.Hosts.Builder();
        hb.hostname(hostInfo.getHostname());
        hostInfo.getServices().stream()
                 .forEach(serviceInfo -> {
                     hb.services(serviceInfo.getServiceName(), getServiceConfig(serviceInfo));
                 });
        return hb;
    }

    private LbServicesConfig.Tenants.Applications.Hosts.Services.Builder getServiceConfig(ServiceInfo serviceInfo) {
        final List<String> endpointAliases = Stream.of(serviceInfo.getProperty("endpointaliases").orElse("").split(",")).
                filter(prop -> !"".equals(prop)).collect(Collectors.toList());
        endpointAliases.addAll(Stream.of(serviceInfo.getProperty("rotations").orElse("").split(",")).filter(prop -> !"".equals(prop)).collect(Collectors.toList()));
        Collections.sort(endpointAliases);

        LbServicesConfig.Tenants.Applications.Hosts.Services.Builder sb = new LbServicesConfig.Tenants.Applications.Hosts.Services.Builder()
                .type(serviceInfo.getServiceType())
                .clustertype(serviceInfo.getProperty("clustertype").orElse(""))
                .clustername(serviceInfo.getProperty("clustername").orElse(""))
                .configId(serviceInfo.getConfigId())
                .servicealiases(Stream.of(serviceInfo.getProperty("servicealiases").orElse("").split(",")).
                                filter(prop -> !"".equals(prop)).sorted((a, b) -> a.compareTo(b)).collect(Collectors.toList()))
                .endpointaliases(endpointAliases)
                .index(Integer.parseInt(serviceInfo.getProperty("index").orElse("999999")));
        serviceInfo.getPorts().stream()
                .forEach(portInfo -> {
                    LbServicesConfig.Tenants.Applications.Hosts.Services.Ports.Builder pb = new LbServicesConfig.Tenants.Applications.Hosts.Services.Ports.Builder()
                        .number(portInfo.getPort())
                        .tags(Joiner.on(" ").join(portInfo.getTags()));
                    sb.ports(pb);
                });
        return sb;
    }
}
