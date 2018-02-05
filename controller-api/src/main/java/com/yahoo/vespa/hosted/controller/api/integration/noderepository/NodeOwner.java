// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.noderepository;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author mpolden
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeOwner {

    @JsonProperty
    public String tenant;
    @JsonProperty
    public String application;
    @JsonProperty
    public String instance;

    public NodeOwner() {}

    public String getTenant() {
        return tenant;
    }

    public String getApplication() {
        return application;
    }

    public String getInstance() {
        return instance;
    }
}
