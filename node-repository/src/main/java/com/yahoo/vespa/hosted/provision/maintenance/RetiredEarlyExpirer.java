// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.collections.ListMap;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.Deployer;
import com.yahoo.config.provision.Deployment;
import com.yahoo.vespa.applicationmodel.HostName;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.orchestrator.OrchestrationException;
import com.yahoo.vespa.orchestrator.Orchestrator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Maintenance job which deactivates retired nodes, if given permission by orchestrator.
 *
 * @author hakon
 */
// TODO: This should be consolidated with RetiredExpirer. The only difference between this and RetiredExpirer is that
// this runs more often by default and asks orchestrator for permission to retire nodes.
public class RetiredEarlyExpirer extends Maintainer {

    private final Deployer deployer;
    private final Orchestrator orchestrator;

    public RetiredEarlyExpirer(NodeRepository nodeRepository,
                               Duration interval,
                               JobControl jobControl,
                               Deployer deployer,
                               Orchestrator orchestrator) {
        super(nodeRepository, interval, jobControl);
        this.deployer = deployer;
        this.orchestrator = orchestrator;
    }

    @Override
    protected void maintain() {
        List<Node> activeNodes = nodeRepository().getNodes(Node.State.active);

        ListMap<ApplicationId, Node> retiredNodesByApplication = new ListMap<>();
        for (Node node : activeNodes) {
            if (node.allocation().isPresent() && node.allocation().get().membership().retired()) {
                retiredNodesByApplication.put(node.allocation().get().owner(), node);
            }
        }

        for (Map.Entry<ApplicationId, List<Node>> entry : retiredNodesByApplication.entrySet()) {
            ApplicationId application = entry.getKey();
            List<Node> retiredNodes = entry.getValue();

            try {
                Optional<Deployment> deployment = deployer.deployFromLocalActive(application);
                if ( ! deployment.isPresent()) continue; // this will be done at another config server

                List<Node> nodesToRemove = new ArrayList<>();
                for (Node node : retiredNodes) {
                    if (canRemove(node)) {
                        nodesToRemove.add(node);
                    }
                }
                
                if (nodesToRemove.isEmpty()) {
                    continue;
                }

                nodeRepository().setRemovable(application, nodesToRemove);

                deployment.get().activate();

                String nodeList = nodesToRemove.stream().map(Node::hostname).collect(Collectors.joining(", "));
                log.info("Redeployed " + application + " to deactivate retired nodes: " +  nodeList);
            } catch (RuntimeException e) {
                String nodeList = retiredNodes.stream().map(Node::hostname).collect(Collectors.joining(", "));
                log.log(Level.WARNING, "Exception trying to deactivate retired nodes from " + application
                        + ": " + nodeList, e);
            }
        }
    }

    /** Returns whether orchestrator permits given node to be removed */
    private boolean canRemove(Node node) {
        try {
            orchestrator.acquirePermissionToRemove(new HostName(node.hostname()));
            return true;
        } catch (OrchestrationException e) {
            log.info("Did not get permission to remove retired " + node + ": " + e.getMessage());
            return false;
        }
    }

}
