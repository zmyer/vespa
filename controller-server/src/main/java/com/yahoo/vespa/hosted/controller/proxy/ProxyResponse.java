// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.proxy;

import com.yahoo.container.jdisc.HttpResponse;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Response class that also rewrites URL from config server.
 *
 * @author Haakon Dybdahl
 */
public class ProxyResponse  extends HttpResponse {

    private final String bodyResponseRewritten;
    private final String contentType;

    public ProxyResponse(
            ProxyRequest controllerRequest,
            String bodyResponse,
            int statusResponse,
            Optional<URI> configServer,
            String contentType) {
        super(statusResponse);
        this.contentType = contentType;

        if (! configServer.isPresent() || controllerRequest.getControllerPrefix().isEmpty()) {
            bodyResponseRewritten = bodyResponse;
            return;
        }

        final String configServerPrefix;
        final String controllerRequestPrefix;
        try {
            configServerPrefix = new URIBuilder()
                    .setScheme(configServer.get().getScheme())
                    .setHost(configServer.get().getHost())
                    .setPort(configServer.get().getPort())
                    .build().toString();
            controllerRequestPrefix = new URIBuilder()
                    .setScheme(controllerRequest.getScheme())
                    // controller prefix is more than host, so it is a bit hackish, but verified by tests.
                    .setHost(controllerRequest.getControllerPrefix())
                    .build().toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        bodyResponseRewritten = bodyResponse.replace(configServerPrefix, controllerRequestPrefix);
    }

    @Override
    public void render(OutputStream stream) throws IOException {
        stream.write(bodyResponseRewritten.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getContentType() { return contentType; }
}
