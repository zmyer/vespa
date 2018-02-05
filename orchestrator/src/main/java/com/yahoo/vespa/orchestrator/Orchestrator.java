// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.orchestrator;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.applicationmodel.HostName;
import com.yahoo.vespa.orchestrator.model.NodeGroup;
import com.yahoo.vespa.orchestrator.policy.BatchHostStateChangeDeniedException;
import com.yahoo.vespa.orchestrator.policy.HostStateChangeDeniedException;
import com.yahoo.vespa.orchestrator.status.ApplicationInstanceStatus;
import com.yahoo.vespa.orchestrator.status.HostStatus;

import java.util.List;
import java.util.Set;

/**
 * The orchestrator is used to coordinate the need of vespa services to restart or
 * disconnect from normal operations for debugging or maintenance. We are not coordinating on the service
 * level but rather on the granularity of host and application instance.
 *
 * (A host will have multiple services and an application will have multiple hots)
 *
 * A policy decides how many hosts can go down at the same time based on which services that runs
 * on the hosts, the redundancy in the system, coverage requirements and potentially more
 * (see policies for details).
 *
 * An application level suspend - enables all services to go down at the same time and bypasses the
 * host level state and the host level policies.
 * This is used for parallel upgrade and larger maintenance tasks.
 *
 * @author smorgrav
 */
public interface Orchestrator {

    /**
     * Get orchestrator information related to a host.
     * @throws HostNameNotFoundException
     */
    Host getHost(HostName hostName) throws HostNameNotFoundException;

    /**
     * Get the status of a given node. If no state is recorded
     * then this will return the status 'No Remarks'
     *
     * @param hostName The FQDN which are used in the noderepo.
     * @return The enum describing the current state.
     * @throws HostNameNotFoundException if hostName is unrecognized (in node repo)
     */
    HostStatus getNodeStatus(HostName hostName) throws HostNameNotFoundException;

    void setNodeStatus(HostName hostName, HostStatus state) throws OrchestrationException;

    /**
     * Resume normal operation for this host.
     *
     * @param hostName The FQDN
     * @throws HostStateChangeDeniedException if the request cannot be meet due to policy constraints.
     * @throws HostNameNotFoundException if the hostName is not recognized in the system (node repo)
     */
    void resume(HostName hostName) throws HostStateChangeDeniedException, HostNameNotFoundException;

    /**
     * Suspend normal operations for this host.
     *
     * @param hostName The FQDN
     * @throws HostStateChangeDeniedException if the request cannot be meet due to policy constraints.
     * @throws HostNameNotFoundException if the hostName is not recognized in the system (node repo)
     */
    void suspend(HostName hostName) throws HostStateChangeDeniedException, HostNameNotFoundException;

    /**
     * Acquire permission to remove a node permanently from the application, or otherwise throw
     * {@link OrchestrationException}.
     */
    void acquirePermissionToRemove(HostName hostName) throws OrchestrationException;

    /**
     * Suspend normal operations for a group of nodes in the same application.
     *
     * @param nodeGroup The group of nodes in an application.
     * @throws HostStateChangeDeniedException if the request cannot be meet due to policy constraints.
     * @throws HostNameNotFoundException if any hostnames in the node group is not recognized
     */
    void suspendGroup(NodeGroup nodeGroup) throws HostStateChangeDeniedException, HostNameNotFoundException;

    /**
     * Suspend several hosts. On failure, all hosts are resumed before exiting the method with an exception.
     */
    void suspendAll(HostName parentHostname, List<HostName> hostNames)
            throws BatchInternalErrorException, BatchHostStateChangeDeniedException, BatchHostNameNotFoundException;

    /**
     * Get the orchestrator status of the application instance.
     *
     * @param appId Identifier of the application to check
     * @return The enum describing the current state.
     */
    ApplicationInstanceStatus getApplicationInstanceStatus(ApplicationId appId) throws ApplicationIdNotFoundException;

    /**
     * Returns all application instances that are suspended. The intention is to use this
     * for visualization, informational and debugging purposes.
     *
     * @return A Map between the application instance and its status.
     */
    Set<ApplicationId> getAllSuspendedApplications();

    /**
     * Resume normal orchestration for hosts belonging to this application.
     *
     * @param appId Identifier of the application to resume
     */
    void resume(ApplicationId appId) throws ApplicationStateChangeDeniedException, ApplicationIdNotFoundException;


    /**
     * Suspend an application:  All hosts will allow suspension in parallel.
     * CAUTION:  Only use this if the application is not in service.
     *
     * @param appId Identifier of the application to resume
     */
    void suspend(ApplicationId appId) throws ApplicationStateChangeDeniedException, ApplicationIdNotFoundException;
}
