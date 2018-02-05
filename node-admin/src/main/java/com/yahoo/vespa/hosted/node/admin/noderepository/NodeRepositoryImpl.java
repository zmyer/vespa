// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.noderepository;

import com.yahoo.vespa.hosted.node.admin.ContainerAclSpec;
import com.yahoo.vespa.hosted.node.admin.ContainerNodeSpec;
import com.yahoo.vespa.hosted.dockerapi.ContainerName;
import com.yahoo.vespa.hosted.dockerapi.DockerImage;
import com.yahoo.vespa.hosted.node.admin.nodeagent.NodeAttributes;
import com.yahoo.vespa.hosted.node.admin.noderepository.bindings.GetAclResponse;
import com.yahoo.vespa.hosted.node.admin.noderepository.bindings.GetNodesResponse;
import com.yahoo.vespa.hosted.node.admin.noderepository.bindings.NodeMessageResponse;
import com.yahoo.vespa.hosted.node.admin.noderepository.bindings.UpdateNodeAttributesRequestBody;
import com.yahoo.vespa.hosted.node.admin.noderepository.bindings.UpdateNodeAttributesResponse;
import com.yahoo.vespa.hosted.node.admin.util.ConfigServerHttpRequestExecutor;
import com.yahoo.vespa.hosted.node.admin.util.HttpException;
import com.yahoo.vespa.hosted.node.admin.util.PrefixLogger;
import com.yahoo.vespa.hosted.provision.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author stiankri, dybis
 */
public class NodeRepositoryImpl implements NodeRepository {
    private static final PrefixLogger NODE_ADMIN_LOGGER = PrefixLogger.getNodeAdminLogger(NodeRepositoryImpl.class);

    private final ConfigServerHttpRequestExecutor requestExecutor;

    public NodeRepositoryImpl(ConfigServerHttpRequestExecutor requestExecutor) {
        this.requestExecutor = requestExecutor;
    }

    @Override
    public List<ContainerNodeSpec> getContainersToRun(String baseHostName) throws IOException {
        try {
            final GetNodesResponse nodesForHost = requestExecutor.get(
                    "/nodes/v2/node/?parentHost=" + baseHostName + "&recursive=true",
                    GetNodesResponse.class);

            if (nodesForHost.nodes == null) {
                throw new IOException("Response didn't contain nodes element");
            }
            List<ContainerNodeSpec> nodes = new ArrayList<>(nodesForHost.nodes.size());
            for (GetNodesResponse.Node node : nodesForHost.nodes) {
                ContainerNodeSpec nodeSpec;
                try {
                    nodeSpec = createContainerNodeSpec(node);
                } catch (IllegalArgumentException | NullPointerException e) {
                    NODE_ADMIN_LOGGER.warning("Bad node received from node repo when requesting children of the "
                            + baseHostName + " host: " + node, e);
                    continue;
                }
                nodes.add(nodeSpec);
            }
            return nodes;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Optional<ContainerNodeSpec> getContainerNodeSpec(String hostName) {
        try {
            GetNodesResponse.Node nodeResponse = requestExecutor.get("/nodes/v2/node/" + hostName,
                                                                     GetNodesResponse.Node.class);
            if (nodeResponse == null) {
                return Optional.empty();
            }
            return Optional.of(createContainerNodeSpec(nodeResponse));
        } catch (HttpException.NotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<ContainerAclSpec> getContainerAclSpecs(String hostName) {
        try {
            final String path = String.format("/nodes/v2/acl/%s?children=true", hostName);
            final GetAclResponse response = requestExecutor.get(path, GetAclResponse.class);
            return response.trustedNodes.stream()
                    .map(node -> new ContainerAclSpec(
                            node.hostname, node.ipAddress, ContainerName.fromHostname(node.trustedBy)))
                    .collect(Collectors.toList());
        } catch (HttpException.NotFoundException e) {
            return Collections.emptyList();
        }
    }

    private static ContainerNodeSpec createContainerNodeSpec(GetNodesResponse.Node node)
            throws IllegalArgumentException, NullPointerException {
        Objects.requireNonNull(node.nodeState, "Unknown node state");
        Node.State nodeState = Node.State.valueOf(node.nodeState);
        if (nodeState == Node.State.active) {
            Objects.requireNonNull(node.wantedVespaVersion, "Unknown vespa version for active node");
            Objects.requireNonNull(node.wantedDockerImage, "Unknown docker image for active node");
            Objects.requireNonNull(node.wantedRestartGeneration, "Unknown wantedRestartGeneration for active node");
            Objects.requireNonNull(node.currentRestartGeneration, "Unknown currentRestartGeneration for active node");
        }

        String hostName = Objects.requireNonNull(node.hostname, "hostname is null");

        ContainerNodeSpec.Owner owner = null;
        if (node.owner != null) {
            owner = new ContainerNodeSpec.Owner(node.owner.tenant, node.owner.application, node.owner.instance);
        }

        ContainerNodeSpec.Membership membership = null;
        if (node.membership != null) {
            membership = new ContainerNodeSpec.Membership(node.membership.clusterType, node.membership.clusterId,
                    node.membership.group, node.membership.index, node.membership.retired);
        }

        return new ContainerNodeSpec(
                hostName,
                Optional.ofNullable(node.wantedDockerImage).map(DockerImage::new),
                Optional.ofNullable(node.currentDockerImage).map(DockerImage::new),
                nodeState,
                node.nodeType,
                node.nodeFlavor,
                node.nodeCanonicalFlavor,
                Optional.ofNullable(node.wantedVespaVersion),
                Optional.ofNullable(node.vespaVersion),
                Optional.ofNullable(owner),
                Optional.ofNullable(membership),
                Optional.ofNullable(node.wantedRestartGeneration),
                Optional.ofNullable(node.currentRestartGeneration),
                Optional.ofNullable(node.wantedRebootGeneration),
                Optional.ofNullable(node.currentRestartGeneration),
                node.minCpuCores,
                node.minMainMemoryAvailableGb,
                node.minDiskAvailableGb,
                node.fastDisk,
                node.ipAddresses,
                Optional.ofNullable(node.hardwareDivergence));
    }

    @Override
    public void updateNodeAttributes(final String hostName, final NodeAttributes nodeAttributes) {
        UpdateNodeAttributesResponse response = requestExecutor.patch(
                "/nodes/v2/node/" + hostName,
                new UpdateNodeAttributesRequestBody(nodeAttributes),
                UpdateNodeAttributesResponse.class);

        if (response.errorCode == null || response.errorCode.isEmpty()) {
            return;
        }
        throw new RuntimeException("Unexpected message " + response.message + " " + response.errorCode);
    }

    @Override
    public void markAsDirty(String hostName) {
        // This will never happen once the new allocation scheme is rolled out.
        markNodeToState(hostName, Node.State.dirty.name());
    }

    @Override
    public void markNodeAvailableForNewAllocation(final String hostName) {
        // TODO replace with call to delete node when everything has been migrated to dynamic docker allocation
        markNodeToState(hostName, "availablefornewallocations");
    }

    private void markNodeToState(String hostName, String state) {
        NodeMessageResponse response = requestExecutor.put(
                "/nodes/v2/state/" + state + "/" + hostName,
                Optional.empty(), /* body */
                NodeMessageResponse.class);
        NODE_ADMIN_LOGGER.info(response.message);

        if (response.errorCode == null || response.errorCode.isEmpty()) {
            return;
        }
        throw new RuntimeException("Unexpected message " + response.message + " " + response.errorCode);
    }
}
