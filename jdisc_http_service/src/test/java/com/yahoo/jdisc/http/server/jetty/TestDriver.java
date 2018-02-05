// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.jdisc.http.server.jetty;

import com.google.inject.Key;
import com.google.inject.Module;
import com.yahoo.jdisc.application.ContainerBuilder;
import com.yahoo.jdisc.handler.RequestHandler;
import com.yahoo.jdisc.http.ConnectorConfig;
import com.yahoo.jdisc.http.SslContextFactory;
import com.yahoo.jdisc.http.JksKeyStore;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Paths;

import static com.google.inject.name.Names.named;

/**
 * This class is based on the class by the same name in the jdisc_http_service module.
 * It provides functionality for setting up a jdisc container with an HTTP server and handlers.
 *
 * @author <a href="mailto:simon@yahoo-inc.com">Simon Thoresen Hult</a>
 * @author bakksjo
 */
public class TestDriver {

    private final com.yahoo.jdisc.test.TestDriver driver;
    private final JettyHttpServer server;
    private final SimpleHttpClient client;

    private TestDriver(com.yahoo.jdisc.test.TestDriver driver, JettyHttpServer server, SimpleHttpClient client)
            throws IOException {
        this.driver = driver;
        this.server = server;
        this.client = client;
    }

    public static TestDriver newInstance(Class<? extends JettyHttpServer> serverClass,
                                         RequestHandler requestHandler,
                                         Module testConfig) throws IOException {
        com.yahoo.jdisc.test.TestDriver driver =
                com.yahoo.jdisc.test.TestDriver.newSimpleApplicationInstance(testConfig);
        ContainerBuilder builder = driver.newContainerBuilder();
        JettyHttpServer server = builder.getInstance(serverClass);
        builder.serverProviders().install(server);
        builder.serverBindings().bind("*://*/*", requestHandler);
        driver.activateContainer(builder);
        server.start();

        SimpleHttpClient client = new SimpleHttpClient(newSslContext(builder), server.getListenPort(), false);
        return new TestDriver(driver, server, client);
    }

    public boolean close() throws IOException {
        server.close();
        server.release();
        return driver.close();
    }

    public JettyHttpServer server() { return server; }

    public SimpleHttpClient client() { return client; }

    public SimpleHttpClient newClient() throws IOException { return newClient(false); }

    public SimpleHttpClient newClient(final boolean useCompression) throws IOException {
        return new SimpleHttpClient(newSslContext(), server.getListenPort(), useCompression);
    }

    public SSLContext newSslContext() {
        return newSslContext(driver.newContainerBuilder());
    }

    private static SSLContext newSslContext(final ContainerBuilder builder) {
        ConnectorConfig.Ssl sslConfig = builder.getInstance(ConnectorConfig.class).ssl();
        if (!sslConfig.enabled()) return null;

        JksKeyStore keyStore = new JksKeyStore(
                Paths.get(sslConfig.keyStorePath()),
                builder.getInstance(Key.get(String.class, named("keyStorePassword"))));
        return SslContextFactory.newInstanceFromTrustStore(keyStore).getServerSSLContext();
    }

}
