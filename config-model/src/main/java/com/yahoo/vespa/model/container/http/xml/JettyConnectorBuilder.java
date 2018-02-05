// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.model.container.http.xml;

import com.yahoo.config.model.builder.xml.XmlHelper;
import com.yahoo.config.model.producer.AbstractConfigProducer;
import com.yahoo.text.XML;
import com.yahoo.vespa.model.builder.xml.dom.VespaDomBuilder;
import com.yahoo.vespa.model.container.http.ConnectorFactory;
import org.w3c.dom.Element;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Einar M R Rosenvinge
 */
public class JettyConnectorBuilder extends VespaDomBuilder.DomConfigProducerBuilder<ConnectorFactory>  {
    private static final Logger log = Logger.getLogger(JettyConnectorBuilder.class.getName());

    @Override
    protected ConnectorFactory doBuild(AbstractConfigProducer ancestor, Element serverSpec) {
        String name = XmlHelper.getIdString(serverSpec);
        int port = HttpBuilder.readPort(serverSpec, ancestor.getRoot().getDeployState());

        Element legacyServerConfig = XML.getChild(serverSpec, "config");
        if (legacyServerConfig != null) {
            String configName = legacyServerConfig.getAttribute("name");
            if (configName.equals("container.jdisc.config.http-server")) {
                ancestor.deployLogger().log(Level.WARNING, "The config 'container.jdisc.config.http-server' is deprecated and will be removed in a later version of Vespa."
                        + " Please use 'jdisc.http.connector' instead, see http://docs.vespa.ai/documentation/jdisc/http-server-and-filters.html#configuring-jetty-server");
            } else {
                legacyServerConfig = null;
            }
        }
        Element sslKeystoreConfigurator = XML.getChild(serverSpec, "ssl-keystore-configurator");
        Element sslTruststoreConfigurator = XML.getChild(serverSpec, "ssl-truststore-configurator");
        return new ConnectorFactory(name, port, legacyServerConfig, sslKeystoreConfigurator, sslTruststoreConfigurator);
    }

}
