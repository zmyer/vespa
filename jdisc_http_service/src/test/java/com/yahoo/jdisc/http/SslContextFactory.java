// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.jdisc.http;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Charles Kim
 */
public class SslContextFactory {

    private static final Logger log = Logger.getLogger(SslContextFactory.class.getName());
    private static final String DEFAULT_ALGORITHM = "SunX509";
    private static final String DEFAULT_PROTOCOL = "TLS";
    private final SSLContext sslContext;

    private SslContextFactory(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public SSLContext getServerSSLContext() {
        return this.sslContext;
    }

    public static SslContextFactory newInstanceFromTrustStore(JksKeyStore trustStore) {
        return newInstance(DEFAULT_ALGORITHM, DEFAULT_PROTOCOL, null, trustStore);
    }

    public static SslContextFactory newInstance(JksKeyStore trustStore, JksKeyStore keyStore) {
        return newInstance(DEFAULT_ALGORITHM, DEFAULT_PROTOCOL, keyStore, trustStore);
    }

    public static SslContextFactory newInstance(String sslAlgorithm, String sslProtocol,
                                                JksKeyStore keyStore, JksKeyStore trustStore) {
        log.fine("Configuring SSLContext...");
        log.fine("Using " + sslAlgorithm + " algorithm.");
        try {
            SSLContext sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(
                    keyStore == null ? null : getKeyManagers(keyStore, sslAlgorithm),
                    trustStore == null ? null : getTrustManagers(trustStore, sslAlgorithm),
                    null);
            return new SslContextFactory(sslContext);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Got exception creating SSLContext.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Used for the key store, which contains the SSL cert and private key.
     */
    public static javax.net.ssl.KeyManager[] getKeyManagers(JksKeyStore keyStore,
                                                            String sslAlgorithm) throws Exception {

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(sslAlgorithm);
        String keyStorePassword = keyStore.getKeyStorePassword();
        keyManagerFactory.init(
                keyStore.loadJavaKeyStore(),
                keyStorePassword != null ? keyStorePassword.toCharArray() : null);
        log.fine("KeyManagerFactory initialized with keystore");
        return keyManagerFactory.getKeyManagers();
    }

    /**
     * Used for the trust store, which contains certificates from other parties that you expect to communicate with,
     * or from Certificate Authorities that you trust to identify other parties.
     */
    public static javax.net.ssl.TrustManager[] getTrustManagers(JksKeyStore trustStore,
                                                                String sslAlgorithm)
            throws Exception {

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(sslAlgorithm);
        trustManagerFactory.init(trustStore.loadJavaKeyStore());
        log.fine("TrustManagerFactory initialized with truststore.");
        return trustManagerFactory.getTrustManagers();
    }

}
