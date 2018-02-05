// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.model.container.xml;

import com.yahoo.config.model.ConfigModelContext;
import com.yahoo.vespa.model.container.ContainerModel;
import com.yahoo.vespa.model.container.configserver.ConfigserverCluster;
import com.yahoo.vespa.model.container.configserver.option.CloudConfigOptions;
import org.w3c.dom.Element;

/**
 * Builds the config model for the standalone config server.
 *
 * @author Ulf Lilleengen
 */
public class ConfigServerContainerModelBuilder extends ContainerModelBuilder {

    private final CloudConfigOptions options;

    public ConfigServerContainerModelBuilder(CloudConfigOptions options) {
        super(true, Networking.enable);
        this.options = options;
    }

    @Override
    public void doBuild(ContainerModel model, Element spec, ConfigModelContext modelContext) {
        ConfigserverCluster cluster = new ConfigserverCluster(modelContext.getParentProducer(), "configserver",
                                                              options);
        super.doBuild(model, spec, modelContext.withParent(cluster));
        cluster.setContainerCluster(model.getCluster());
    }

}
