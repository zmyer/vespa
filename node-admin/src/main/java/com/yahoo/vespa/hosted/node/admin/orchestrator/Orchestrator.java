// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.orchestrator;

import java.util.List;

/**
 * Abstraction for communicating with Orchestrator.
 *
 * @author bakksjo
 */
public interface Orchestrator {
    /**
     * Invokes orchestrator suspend of a host.
     * @throws OrchestratorException if suspend was denied.
     */
    void suspend(String hostName);

    /**
     * Invokes orchestrator resume of a host.
     * @throws OrchestratorException if resume was denied.
     */
    void resume(String hostName);

    /**
     * Invokes orchestrator suspend hosts.
     * @throws OrchestratorException if batch suspend was denied.
     */
    void suspend(String parentHostName, List<String> hostNames);
}
