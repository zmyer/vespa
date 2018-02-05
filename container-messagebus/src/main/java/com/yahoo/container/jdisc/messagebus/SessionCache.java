// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.jdisc.messagebus;

import com.yahoo.component.AbstractComponent;
import com.yahoo.config.subscription.ConfigGetter;
import com.yahoo.container.jdisc.ContainerMbusConfig;
import com.yahoo.document.DocumentTypeManager;
import com.yahoo.document.DocumentUtil;
import com.yahoo.documentapi.messagebus.loadtypes.LoadTypeSet;
import com.yahoo.documentapi.messagebus.protocol.DocumentProtocol;
import com.yahoo.jdisc.ReferencedResource;
import com.yahoo.jdisc.References;
import com.yahoo.jdisc.ResourceReference;
import com.yahoo.jdisc.SharedResource;
import com.yahoo.log.LogLevel;
import com.yahoo.messagebus.*;
import com.yahoo.messagebus.network.Identity;
import com.yahoo.messagebus.network.rpc.RPCNetworkParams;
import com.yahoo.messagebus.shared.SharedDestinationSession;
import com.yahoo.messagebus.shared.SharedIntermediateSession;
import com.yahoo.messagebus.shared.SharedMessageBus;
import com.yahoo.messagebus.shared.SharedSourceSession;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Class to encapsulate access to slobrok sessions.
 *
 * @author <a href="mailto:steinar@yahoo-inc.com">Steinar Knutsen</a>
 * @author <a href="mailto:einarmr@yahoo-inc.com">Einar Rosenvinge</a>
 */
public final class SessionCache extends AbstractComponent {

    private static final Logger log = Logger.getLogger(SessionCache.class.getName());

    //config
    private final String messagebusConfigId;
    private final String slobrokConfigId;
    private final String identity;
    private final String containerMbusConfigId;
    private final String documentManagerConfigId;
    private final String loadTypeConfigId;
    private final DocumentTypeManager documentTypeManager;

    // initialized in start()
    private ConfigAgent configAgent;
    private SharedMessageBus messageBus;

    private final Object intermediateLock = new Object();
    private final Map<String, SharedIntermediateSession> intermediates = new HashMap<>();
    private final IntermediateSessionCreator intermediatesCreator = new IntermediateSessionCreator();

    private final Object destinationLock = new Object();
    private final Map<String, SharedDestinationSession> destinations = new HashMap<>();
    private final DestinationSessionCreator destinationsCreator = new DestinationSessionCreator();

    private final Object sourceLock = new Object();
    private final Map<SourceSessionKey, SharedSourceSession> sources = new HashMap<>();
    private final SourceSessionCreator sourcesCreator = new SourceSessionCreator();

    public SessionCache(final String messagebusConfigId, final String slobrokConfigId, final String identity,
                        final String containerMbusConfigId, final String documentManagerConfigId,
                        final String loadTypeConfigId,
                        final DocumentTypeManager documentTypeManager) {
        this.messagebusConfigId = messagebusConfigId;
        this.slobrokConfigId = slobrokConfigId;
        this.identity = identity;
        this.containerMbusConfigId = containerMbusConfigId;
        this.documentManagerConfigId = documentManagerConfigId;
        this.loadTypeConfigId = loadTypeConfigId;
        this.documentTypeManager = documentTypeManager;
    }

    public SessionCache(final String identity) {
        this(identity, identity, identity, identity, identity, identity, new DocumentTypeManager());
    }

    private void start() {
        ContainerMbusConfig mbusConfig = ConfigGetter.getConfig(ContainerMbusConfig.class, containerMbusConfigId);
        if (documentManagerConfigId != null) {
            documentTypeManager.configure(documentManagerConfigId);
        }
        LoadTypeSet loadTypeSet = new LoadTypeSet(loadTypeConfigId);
        DocumentProtocol protocol = new DocumentProtocol(documentTypeManager, identity, loadTypeSet);
        messageBus = createSharedMessageBus(mbusConfig, slobrokConfigId, identity, protocol);
        // TODO: stop doing subscriptions to config when that is to be solved in slobrok as well
        configAgent = new ConfigAgent(messagebusConfigId, messageBus.messageBus());
        configAgent.subscribe();
    }

    private boolean isStarted() {
        return messageBus != null;
    }

    private static SharedMessageBus createSharedMessageBus(final ContainerMbusConfig mbusConfig,
                                                           final String slobrokConfigId, final String identity,
                                                           Protocol protocol) {
        final MessageBusParams mbusParams = new MessageBusParams().addProtocol(protocol);

        final int maxPendingSize = DocumentUtil
                .calculateMaxPendingSize(mbusConfig.maxConcurrentFactor(), mbusConfig.documentExpansionFactor(),
                        mbusConfig.containerCoreMemory());
        logSystemInfo(mbusConfig, maxPendingSize);

        mbusParams.setMaxPendingCount(mbusConfig.maxpendingcount());
        mbusParams.setMaxPendingSize(maxPendingSize);

        final RPCNetworkParams netParams = new RPCNetworkParams()
                .setSlobrokConfigId(slobrokConfigId)
                .setIdentity(new Identity(identity))
                .setListenPort(mbusConfig.port());
        return SharedMessageBus.newInstance(mbusParams, netParams);
    }

    private static void logSystemInfo(ContainerMbusConfig containerMbusConfig, long maxPendingSize) {
        log.log(LogLevel.DEBUG,
                "Running with maximum heap size of " + (Runtime.getRuntime().maxMemory() / 1024L / 1024L) + " MB");
        log.log(LogLevel.CONFIG,
                "Amount of memory reserved for container core: " + containerMbusConfig.containerCoreMemory() + " MB.");
        log.log(LogLevel.CONFIG,
                "Running with document expansion factor " + containerMbusConfig.documentExpansionFactor() + "");

        String msgLimit =
                (containerMbusConfig.maxpendingcount() == 0) ? "unlimited" : "" + containerMbusConfig.maxpendingcount();
        log.log(LogLevel.CONFIG, ("Starting message bus with max " + msgLimit + " pending messages and max " +
                (((double) (maxPendingSize / 1024L)) / 1024.0d) + " pending megabytes."));
    }

    public ReferencedResource<SharedIntermediateSession> retainIntermediate(final IntermediateSessionParams p) {
        synchronized (this) {
            if (!isStarted()) {
                start();
            }
        }
        return intermediatesCreator.retain(intermediateLock, intermediates, p);
    }

    public ReferencedResource<SharedDestinationSession> retainDestination(final DestinationSessionParams p) {
        synchronized (this) {
            if (!isStarted()) {
                start();
            }
        }
        return destinationsCreator.retain(destinationLock, destinations, p);
    }

    public ReferencedResource<SharedSourceSession> retainSource(final SourceSessionParams p) {
        synchronized (this) {
            if (!isStarted()) {
                start();
            }
        }
        return sourcesCreator.retain(sourceLock, sources, p);
    }

    private abstract class SessionCreator<PARAMS, KEY, SESSION extends SharedResource> {
        abstract SESSION create(PARAMS p);

        abstract KEY buildKey(PARAMS p);

        abstract void logReuse(SESSION session);

        ReferencedResource<SESSION> retain(final Object lock, final Map<KEY, SESSION> registry, final PARAMS p) {
            SESSION session;
            ResourceReference sessionReference;
            final KEY key = buildKey(p);
            // this lock is held for a horribly long time, but I see no way of
            // making it slimmer
            synchronized (lock) {
                session = registry.get(key);
                if (session == null) {
                    session = createAndStore(registry, p, key);
                    sessionReference = References.fromResource(session);
                } else {
                    try {
                        sessionReference = session.refer();
                        logReuse(session);
                    } catch (final IllegalStateException e) {
                        session = createAndStore(registry, p, key);
                        sessionReference = References.fromResource(session);
                    }
                }
            }
            return new ReferencedResource<>(session, sessionReference);
        }

        SESSION createAndStore(final Map<KEY, SESSION> registry, final PARAMS p, final KEY key) {
            SESSION session = create(p);
            registry.put(key, session);
            return session;
        }

    }

    private class DestinationSessionCreator
            extends SessionCreator<DestinationSessionParams, String, SharedDestinationSession> {
        @Override
        SharedDestinationSession create(final DestinationSessionParams p) {
            log.log(LogLevel.DEBUG, "Creating new destination session " + p.getName() + "");
            return messageBus.newDestinationSession(p);
        }

        @Override
        String buildKey(final DestinationSessionParams p) {
            return p.getName();
        }

        @Override
        void logReuse(final SharedDestinationSession session) {
            log.log(LogLevel.DEBUG, "Reusing destination session " + session.name() + "");
        }
    }

    private class SourceSessionCreator
            extends SessionCreator<SourceSessionParams, SourceSessionKey, SharedSourceSession> {

        @Override
        SharedSourceSession create(final SourceSessionParams p) {
            log.log(LogLevel.DEBUG, "Creating new source session.");
            return messageBus.newSourceSession(p);
        }

        @Override
        SourceSessionKey buildKey(final SourceSessionParams p) {
            return new SourceSessionKey(p);
        }

        @Override
        void logReuse(final SharedSourceSession session) {
            log.log(LogLevel.DEBUG, "Reusing source session.");
        }
    }

    private class IntermediateSessionCreator
            extends SessionCreator<IntermediateSessionParams, String, SharedIntermediateSession> {

        @Override
        SharedIntermediateSession create(final IntermediateSessionParams p) {
            log.log(LogLevel.DEBUG, "Creating new intermediate session " + p.getName() + "");
            return messageBus.newIntermediateSession(p);
        }

        @Override
        String buildKey(final IntermediateSessionParams p) {
            return p.getName();
        }

        @Override
        void logReuse(final SharedIntermediateSession session) {
            log.log(LogLevel.DEBUG, "Reusing intermediate session " + session.name() + "");
        }
    }

    static class ThrottlePolicySignature {
        @Override
        public int hashCode() {
            return getClass().hashCode();
        }
    }

    static class StaticThrottlePolicySignature extends
            ThrottlePolicySignature {
        private final int maxPendingCount;
        private final long maxPendingSize;

        StaticThrottlePolicySignature(final StaticThrottlePolicy policy) {
            maxPendingCount = policy.getMaxPendingCount();
            maxPendingSize = policy.getMaxPendingSize();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + maxPendingCount;
            result = prime * result
                    + (int) (maxPendingSize ^ (maxPendingSize >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final StaticThrottlePolicySignature other = (StaticThrottlePolicySignature) obj;
            if (maxPendingCount != other.maxPendingCount) {
                return false;
            }
            if (maxPendingSize != other.maxPendingSize) {
                return false;
            }
            return true;
        }

    }

    static class DynamicThrottlePolicySignature extends
            ThrottlePolicySignature {
        private final int maxPending;
        private final double maxWindowSize;
        private final double minWindowSize;
        private final double windowSizeBackoff;
        private final double windowSizeIncrement;

        DynamicThrottlePolicySignature(final DynamicThrottlePolicy policy) {
            maxPending = policy.getMaxPendingCount();
            maxWindowSize = policy.getMaxWindowSize();
            minWindowSize = policy.getMinWindowSize();
            windowSizeBackoff = policy.getWindowSizeBackOff();
            windowSizeIncrement = policy.getWindowSizeIncrement();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + maxPending;
            long temp;
            temp = Double.doubleToLongBits(maxWindowSize);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(minWindowSize);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(windowSizeBackoff);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(windowSizeIncrement);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DynamicThrottlePolicySignature other = (DynamicThrottlePolicySignature) obj;
            if (maxPending != other.maxPending) {
                return false;
            }
            if (Double.doubleToLongBits(maxWindowSize) != Double
                    .doubleToLongBits(other.maxWindowSize)) {
                return false;
            }
            if (Double.doubleToLongBits(minWindowSize) != Double
                    .doubleToLongBits(other.minWindowSize)) {
                return false;
            }
            if (Double.doubleToLongBits(windowSizeBackoff) != Double
                    .doubleToLongBits(other.windowSizeBackoff)) {
                return false;
            }
            if (Double.doubleToLongBits(windowSizeIncrement) != Double
                    .doubleToLongBits(other.windowSizeIncrement)) {
                return false;
            }
            return true;
        }

    }

    static class UnknownThrottlePolicySignature extends
            ThrottlePolicySignature {
        private final ThrottlePolicy policy;

        UnknownThrottlePolicySignature(final ThrottlePolicy policy) {
            this.policy = policy;
        }

        @Override
        public boolean equals(final Object other) {
            if (other == null) {
                return false;
            }
            if (other.getClass() != getClass()) {
                return false;
            }
            return ((UnknownThrottlePolicySignature) other).policy == policy;
        }
    }

    static class SourceSessionKey {
        private final double timeout;
        private final ThrottlePolicySignature policy;

        SourceSessionKey(final SourceSessionParams p) {
            timeout = p.getTimeout();
            policy = createSignature(p.getThrottlePolicy());
        }

        private static ThrottlePolicySignature createSignature(
                final ThrottlePolicy policy) {
            final Class<?> policyClass = policy.getClass();
            if (policyClass == DynamicThrottlePolicy.class) {
                return new DynamicThrottlePolicySignature(
                        (DynamicThrottlePolicy) policy);
            } else if (policyClass == StaticThrottlePolicy.class) {
                return new StaticThrottlePolicySignature(
                        (StaticThrottlePolicy) policy);
            } else {
                return new UnknownThrottlePolicySignature(policy);
            }
        }

        @Override
        public String toString() {
            return "SourceSessionKey{" +
                   "timeout=" + timeout +
                   ", policy=" + policy +
                   '}';
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((policy == null) ? 0 : policy.hashCode());
            long temp;
            temp = Double.doubleToLongBits(timeout);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final SourceSessionKey other = (SourceSessionKey) obj;
            if (policy == null) {
                if (other.policy != null) {
                    return false;
                }
            } else if (!policy.equals(other.policy)) {
                return false;
            }
            if (Double.doubleToLongBits(timeout) != Double
                    .doubleToLongBits(other.timeout)) {
                return false;
            }
            return true;
        }
    }
}
