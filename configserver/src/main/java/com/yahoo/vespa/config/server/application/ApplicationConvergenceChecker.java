// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.yahoo.component.AbstractComponent;
import com.yahoo.config.model.api.HostInfo;
import com.yahoo.config.model.api.PortInfo;
import com.yahoo.config.model.api.ServiceInfo;
import com.yahoo.slime.Cursor;
import com.yahoo.vespa.config.server.http.JSONResponse;
import org.glassfish.jersey.client.proxy.WebResourceFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Checks for convergence of config generation for a given application.
 *
 * @author lulf
 * @author hmusum
 */
public class ApplicationConvergenceChecker extends AbstractComponent {
    private static final String statePath = "/state/v1/";
    private static final String configSubPath = "config";
    private final StateApiFactory stateApiFactory;
    private final Client client = ClientBuilder.newClient();

    private final static Set<String> serviceTypesToCheck = new HashSet<>(Arrays.asList(
            "container",
            "qrserver",
            "docprocservice",
            "searchnode",
            "storagenode",
            "distributor"
    ));

    @Inject
    public ApplicationConvergenceChecker() {
        this(ApplicationConvergenceChecker::createStateApi);
    }

    public ApplicationConvergenceChecker(StateApiFactory stateApiFactory) {
        this.stateApiFactory = stateApiFactory;
    }

    public ServiceListResponse serviceListToCheckForConfigConvergence(Application application, URI uri) {
        List<ServiceInfo> servicesToCheck = new ArrayList<>();
        application.getModel().getHosts()
                   .forEach(host -> host.getServices().stream()
                                        .filter(service -> serviceTypesToCheck.contains(service.getServiceType()))
                                        .forEach(service -> getStatePort(service).ifPresent(port -> servicesToCheck.add(service))));
        return new ServiceListResponse(200, servicesToCheck, uri, application.getApplicationGeneration());
    }

    public ServiceResponse serviceConvergenceCheck(Application application, String hostAndPortToCheck, URI uri) {
        Long wantedGeneration = application.getApplicationGeneration();
        try {
            if (! hostInApplication(application, hostAndPortToCheck))
                return ServiceResponse.createHostNotFoundInAppResponse(uri, hostAndPortToCheck, wantedGeneration);

            long currentGeneration = getServiceGeneration(URI.create("http://" + hostAndPortToCheck));
            boolean converged = currentGeneration >= wantedGeneration;
            return ServiceResponse.createOkResponse(uri, hostAndPortToCheck, wantedGeneration, currentGeneration, converged);
        } catch (ProcessingException e) { // e.g. if we cannot connect to the service to find generation
            return ServiceResponse.createNotFoundResponse(uri, hostAndPortToCheck, wantedGeneration, e.getMessage());
        } catch (Exception e) {
            return ServiceResponse.createErrorResponse(uri, hostAndPortToCheck, wantedGeneration, e.getMessage());
        }
    }

    @Override
    public void deconstruct() {
        client.close();
    }

    @Path(statePath)
    public interface StateApi {
        @Path(configSubPath)
        @GET
        JsonNode config();
    }

    public interface StateApiFactory {
        StateApi createStateApi(Client client, URI serviceUri);
    }

    private static Optional<Integer> getStatePort(ServiceInfo service) {
        return service.getPorts().stream()
                .filter(port -> port.getTags().contains("state"))
                .map(PortInfo::getPort)
                .findFirst();
    }

    private long generationFromContainerState(JsonNode state) {
        return state.get("config").get("generation").asLong();
    }

    private static StateApi createStateApi(Client client, URI uri) {
        WebTarget target = client.target(uri);
        return WebResourceFactory.newResource(StateApi.class, target);
    }

    private long getServiceGeneration(URI serviceUri) {
        StateApi state = stateApiFactory.createStateApi(client, serviceUri);
        return generationFromContainerState(state.config());
    }

    private boolean hostInApplication(Application application, String hostPort) throws IOException {
        for (HostInfo host : application.getModel().getHosts()) {
            if (hostPort.startsWith(host.getHostname())) {
                for (ServiceInfo service : host.getServices()) {
                    for (PortInfo port : service.getPorts()) {
                        if (hostPort.equals(host.getHostname() + ":" + port.getPort())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    static class ServiceListResponse extends JSONResponse {
        final Cursor debug;

        // Pre-condition: servicesToCheck has a state port
        private ServiceListResponse(int status, List<ServiceInfo> servicesToCheck, URI uri, Long wantedGeneration) {
            super(status);
            Cursor serviceArray = object.setArray("services");
            for (ServiceInfo s : servicesToCheck) {
                Cursor service = serviceArray.addObject();
                String hostName = s.getHostName();
                int statePort = getStatePort(s).get();
                service.setString("host", hostName);
                service.setLong("port", statePort);
                service.setString("type", s.getServiceType());
                service.setString("url", uri.toString() + "/" + hostName + ":" + statePort);
            }
            debug = object.setObject("debug");
            object.setString("url", uri.toString());
            debug.setLong("wantedGeneration", wantedGeneration);
        }
    }

    static class ServiceResponse extends JSONResponse {
        final Cursor debug;

        private ServiceResponse(int status, URI uri, String hostname, Long wantedGeneration) {
            super(status);
            debug = object.setObject("debug");
            object.setString("url", uri.toString());
            debug.setString("host", hostname);
            debug.setLong("wantedGeneration", wantedGeneration);
        }

        static ServiceResponse createOkResponse(URI uri, String hostname, Long wantedGeneration, Long currentGeneration, boolean converged) {
            ServiceResponse serviceResponse = new ServiceResponse(200, uri, hostname, wantedGeneration);
            serviceResponse.object.setBool("converged", converged);
            serviceResponse.debug.setLong("currentGeneration", currentGeneration);
            return serviceResponse;
        }

        static ServiceResponse createHostNotFoundInAppResponse(URI uri, String hostname, Long wantedGeneration) {
            ServiceResponse serviceResponse = new ServiceResponse(410, uri, hostname, wantedGeneration);
            serviceResponse.debug.setString("problem", "Host:port (service) no longer part of application, refetch list of services.");
            return serviceResponse;
        }

        static ServiceResponse createErrorResponse(URI uri, String hostname, Long wantedGeneration, String error) {
            ServiceResponse serviceResponse = new ServiceResponse(500, uri, hostname, wantedGeneration);
            serviceResponse.object.setString("error", error);
            return serviceResponse;
        }

        static ServiceResponse createNotFoundResponse(URI uri, String hostname, Long wantedGeneration, String error) {
            ServiceResponse serviceResponse = new ServiceResponse(404, uri, hostname, wantedGeneration);
            serviceResponse.object.setString("error", error);
            return serviceResponse;
        }
    }

}
