// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.hosted.controller.rotation.RotationId;

import java.net.URI;

/**
 * Represents an application's global rotation.
 *
 * @author mpolden
 */
public class ApplicationRotation {

    // TODO: TLS: Remove all non-secure stuff when all traffic is on HTTPS.
    public static final String DNS_SUFFIX = "global.vespa.yahooapis.com";
    private static final int port = 4080;
    private static final int securePort = 4443;

    private final URI url;
    private final URI secureUrl;
    private final RotationId id;

    public ApplicationRotation(ApplicationId application, RotationId id) {
        this.url = URI.create(String.format("http://%s.%s.%s:%d/",
                                            sanitize(application.application().value()),
                                            sanitize(application.tenant().value()),
                                            DNS_SUFFIX,
                                            port));
        this.secureUrl = URI.create(String.format("https://%s--%s.%s:%d/",
                                                  sanitize(application.application().value()),
                                                  sanitize(application.tenant().value()),
                                                  DNS_SUFFIX,
                                                  securePort));
        this.id = id;
    }

    /** ID of the rotation */
    public RotationId id() {
        return id;
    }

    /** URL to this rotation */
    public URI url() {
        return url;
    }

    /** HTTPS URL to this rotation */
    public URI secureUrl() {
        return secureUrl;
    }

    /** DNS name for this rotation */
    public String dnsName() {
        return url.getHost();
    }

    /** DNS name for this rotation */
    public String secureDnsName() {
        return secureUrl.getHost();
    }

    /** Sanitize by translating '_' to '-' as the former is not allowed in a DNS name */
    private static String sanitize(String s) {
        return s.replace('_', '-');
    }

}
