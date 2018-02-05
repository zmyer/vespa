// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config;

import com.yahoo.config.subscription.ConfigSourceSet;
import com.yahoo.jrt.Supervisor;
import com.yahoo.jrt.Transport;
import com.yahoo.log.LogLevel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * A pool of JRT connections to a config source (either a config server or a config proxy).
 * The current connection is chosen randomly when calling {#link #setNewCurrentConnection}
 * (since the connection is chosen randomly, it might end up using the same connection again,
 * and it will always do so if there is only one source).
 * The current connection is available with {@link #getCurrent()}.
 * When calling {@link #setError(Connection, int)}, {#link #setNewCurrentConnection} will always be called.
 *
 * @author Gunnar Gauslaa Bergem
 * @author hmusum
 */
public class JRTConnectionPool implements ConnectionPool {

    private static final Logger log = Logger.getLogger(JRTConnectionPool.class.getName());

    private final Supervisor supervisor = new Supervisor(new Transport());
    private final Map<String, JRTConnection> connections = new LinkedHashMap<>();

    // The config sources used by this connection pool.
    private ConfigSourceSet sourceSet = null;

    // The current connection used by this connection pool.
    private volatile JRTConnection currentConnection;

    public JRTConnectionPool(ConfigSourceSet sourceSet) {
        addSources(sourceSet);
    }

    public JRTConnectionPool(List<String> addresses) {
        this(new ConfigSourceSet(addresses));
    }

    public void addSources(ConfigSourceSet sourceSet) {
        this.sourceSet = sourceSet;
        synchronized (connections) {
            for (String address : sourceSet.getSources()) {
                connections.put(address, new JRTConnection(address, supervisor));
            }
        }
        setNewCurrentConnection();
    }

    /**
     * Returns the current JRTConnection instance
     *
     * @return a JRTConnection
     */
    public synchronized JRTConnection getCurrent() {
        return currentConnection;
    }

    /**
     * Returns and set the current JRTConnection instance by randomly choosing
     * from the available sources (this means that you might end up using
     * the same connection).
     *
     * @return a JRTConnection
     */
    public synchronized JRTConnection setNewCurrentConnection() {
        List<JRTConnection> sources = getSources();
        currentConnection = sources.get(ThreadLocalRandom.current().nextInt(0, sources.size()));
        if (log.isLoggable(LogLevel.DEBUG)) {
            log.log(LogLevel.DEBUG, "Choosing new connection: " + currentConnection);
        }
        return currentConnection;
    }

    List<JRTConnection> getSources() {
        List<JRTConnection> ret = new ArrayList<>();
        synchronized (connections) {
            for (JRTConnection source : connections.values()) {
                ret.add(source);
            }
        }
        return ret;
    }

    ConfigSourceSet getSourceSet() {
        return sourceSet;
    }

    @Override
    public void setError(Connection connection, int errorCode) {
        connection.setError(errorCode);
        setNewCurrentConnection();
    }

    public JRTConnectionPool updateSources(List<String> addresses) {
        ConfigSourceSet newSources = new ConfigSourceSet(addresses);
        return updateSources(newSources);
    }

    public JRTConnectionPool updateSources(ConfigSourceSet sourceSet) {
        synchronized (connections) {
            for (JRTConnection conn : connections.values()) {
                conn.getTarget().close();
            }
            connections.clear();
            addSources(sourceSet);
        }
        return this;
    }

    public String getAllSourceAddresses() {
        StringBuilder sb = new StringBuilder();
        synchronized (connections) {
            for (JRTConnection conn : connections.values()) {
                sb.append(conn.getAddress());
                sb.append(",");
            }
        }
        // Remove trailing ","
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        synchronized (connections) {
            for (JRTConnection conn : connections.values()) {
                sb.append(conn.toString());
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public void close() {
        supervisor.transport().shutdown().join();
    }

    @Override
    public int getSize() {
        synchronized (connections) {
            return connections.size();
        }
    }

    @Override
    public Supervisor getSupervisor() {
        return supervisor;
    }

}
