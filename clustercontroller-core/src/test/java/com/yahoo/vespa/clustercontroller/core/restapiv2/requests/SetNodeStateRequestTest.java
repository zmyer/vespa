// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.clustercontroller.core.restapiv2.requests;

import com.yahoo.vdslib.state.ClusterState;
import com.yahoo.vdslib.state.Node;
import com.yahoo.vdslib.state.NodeState;
import com.yahoo.vdslib.state.NodeType;
import com.yahoo.vdslib.state.State;
import com.yahoo.vespa.clustercontroller.core.ContentCluster;
import com.yahoo.vespa.clustercontroller.core.NodeInfo;
import com.yahoo.vespa.clustercontroller.core.NodeStateChangeChecker;
import com.yahoo.vespa.clustercontroller.core.listeners.NodeStateOrHostInfoChangeHandler;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.errors.StateRestApiException;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.requests.SetUnitStateRequest;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.response.SetResponse;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.response.UnitState;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetNodeStateRequestTest {
    public static final String REASON = "operator";
    ContentCluster cluster = mock(ContentCluster.class);
    SetUnitStateRequest.Condition condition = SetUnitStateRequest.Condition.SAFE;
    Map<String, UnitState> newStates = new HashMap<>();
    UnitState unitState = mock(UnitState.class);
    private final int NODE_INDEX = 2;
    Node storageNode = new Node(NodeType.STORAGE, NODE_INDEX);
    NodeStateOrHostInfoChangeHandler stateListener = mock(NodeStateOrHostInfoChangeHandler.class);
    ClusterState currentClusterState = mock(ClusterState.class);

    @Before
    public void setUp() {
        newStates.put("user", unitState);
    }

    @Test
    public void testUpToMaintenance() throws StateRestApiException {
        testSetStateRequest(
                "maintenance",
                State.UP, State.UP,
                NodeStateChangeChecker.Result.allowSettingOfWantedState(),
                Optional.of(State.MAINTENANCE), Optional.of(State.DOWN));
    }

    @Test
    public void testUpToDown() throws StateRestApiException {
        testSetStateRequest(
                "down",
                State.UP, State.UP,
                NodeStateChangeChecker.Result.allowSettingOfWantedState(),
                Optional.of(State.DOWN), Optional.of(State.DOWN));
    }

    @Test
    public void testMaintenanceToUp() throws StateRestApiException {
        testSetStateRequest(
                "up",
                State.MAINTENANCE, State.DOWN,
                NodeStateChangeChecker.Result.allowSettingOfWantedState(),
                Optional.of(State.UP), Optional.of(State.UP));
    }

    @Test
    public void testDownToUp() throws StateRestApiException {
        testSetStateRequest(
                "up",
                State.DOWN, State.DOWN,
                NodeStateChangeChecker.Result.allowSettingOfWantedState(),
                Optional.of(State.UP), Optional.of(State.UP));
    }

    @Test
    public void testOnlyStorageInMaintenaceToMaintenance() throws StateRestApiException {
        testSetStateRequest(
                "maintenance",
                State.MAINTENANCE, State.UP,
                NodeStateChangeChecker.Result.createAlreadySet(),
                Optional.empty(), Optional.of(State.DOWN));
    }

    @Test
    public void testNoOpMaintenaceToMaintenance() throws StateRestApiException {
        testSetStateRequest(
                "maintenance",
                State.MAINTENANCE, State.DOWN,
                NodeStateChangeChecker.Result.createAlreadySet(),
                Optional.empty(), Optional.empty());
    }

    private void testSetStateRequest(
            String wantedStateString,
            State storageWantedState,
            State distributorWantedState,
            NodeStateChangeChecker.Result result,
            Optional<State> expectedNewStorageWantedState,
            Optional<State> expectedNewDistributorWantedState) throws StateRestApiException {
        when(cluster.hasConfiguredNode(NODE_INDEX)).thenReturn(true);

        NodeInfo storageNodeInfo = mock(NodeInfo.class);
        when(cluster.getNodeInfo(storageNode)).thenReturn(storageNodeInfo);
        NodeState storageNodeState = new NodeState(NodeType.STORAGE, storageWantedState);
        when(storageNodeInfo.getUserWantedState()).thenReturn(storageNodeState);

        when(unitState.getId()).thenReturn(wantedStateString);
        when(unitState.getReason()).thenReturn(REASON);

        when(cluster.calculateEffectOfNewState(any(), any(), any(), any(), any())).thenReturn(result);

        when(storageNodeInfo.isStorage()).thenReturn(storageNode.getType() == NodeType.STORAGE);
        when(storageNodeInfo.getNodeIndex()).thenReturn(storageNode.getIndex());

        NodeInfo distributorNodeInfo = mock(NodeInfo.class);
        Node distributorNode = new Node(NodeType.DISTRIBUTOR, NODE_INDEX);
        when(cluster.getNodeInfo(distributorNode)).thenReturn(distributorNodeInfo);

        NodeState distributorNodeState = new NodeState(distributorNode.getType(), distributorWantedState);
        when(distributorNodeInfo.getUserWantedState()).thenReturn(distributorNodeState);

        setWantedState();

        if (expectedNewStorageWantedState.isPresent()) {
            NodeState expectedNewStorageNodeState =
                    new NodeState(NodeType.STORAGE, expectedNewStorageWantedState.get());
            verify(storageNodeInfo).setWantedState(expectedNewStorageNodeState);
            verify(stateListener).handleNewWantedNodeState(storageNodeInfo, expectedNewStorageNodeState);
        }

        if (expectedNewDistributorWantedState.isPresent()) {
            NodeState expectedNewDistributorNodeState =
                    new NodeState(NodeType.DISTRIBUTOR, expectedNewDistributorWantedState.get());
            verify(distributorNodeInfo).setWantedState(expectedNewDistributorNodeState);
            verify(stateListener).handleNewWantedNodeState(distributorNodeInfo, expectedNewDistributorNodeState);
        }
    }

    private SetResponse setWantedState() throws StateRestApiException {
        return SetNodeStateRequest.setWantedState(
                cluster,
                condition,
                newStates,
                storageNode,
                stateListener,
                currentClusterState);
    }
}