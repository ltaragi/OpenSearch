/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.STRICT;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.NONE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationAllocationIT extends MigrationBaseTestCase {

    public static final String TEST_INDEX = "test_index";
    public static final String NAME = "remote_store_migration";

    private static final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
    private Client client;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /* Adding the following mock plugins:
        - InternalSettingsPlugin : To override default intervals of retention lease and global ckp sync
        - MockFsRepositoryPlugin and MockTransportService.TestPlugin: To ensure remote interactions are not no-op and retention leases are properly propagated
         */
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class, MockFsRepositoryPlugin.class, MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    // tests for primary shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testAllocateNewPrimaryShardForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> verify expected decision for allocating a new primary shard on a non-remote node");
        prepareIndexWithoutReplica(Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, true, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "[remote_store migration_direction]: primary shard copy can not be allocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation on non-remote node");
        attemptAllocation(Optional.empty());

        logger.info(" --> verify non-allocation of primary shard on non-remote node");
        assertNonAllocation(true);

        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info(" --> verify expected decision for allocating a new primary shard on a remote node");
        prepareDecisions();
        decision = getDecisionForTargetNode(remoteNode, true, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: primary shard copy can be allocated to a remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation on remote node");
        attemptAllocation(Optional.empty());
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(true, Optional.of(remoteNode));
    }

    // tests for replica shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testNewReplicaShardAllocationIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        String nonRemoteNodeName1 = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);

        logger.info(" --> allocate primary shard on non-remote node");
        prepareIndexWithAllocatedPrimary(nonRemoteNode1, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard for remote node");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode, false, true, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can not be allocated to a remote node since primary shard copy is not yet migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation of replica shard on remote node");
        attemptAllocation(Optional.empty());

        logger.info(" --> verify non-allocation of replica shard");
        assertNonAllocation(false);

        logger.info(" --> add another non-remote node");
        addRemote = false;
        String nonRemoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);

        logger.info(" --> verify expected decision for replica shard for the other non-remote node");
        prepareDecisions();
        decision = getDecisionForTargetNode(nonRemoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        logger.info(" --> attempt allocation of replica shard on the other non-remote node");
        attemptAllocation(Optional.empty());
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        assertAllocation(false, Optional.of(nonRemoteNode2));
    }

    public void testNewReplicaShardAllocationIfPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED.mode);

        logger.info(" --> add remote and non-remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> allocate primary shard on remote node");
        prepareIndexWithAllocatedPrimary(remoteNode1, Optional.empty());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be allocated to a remote node since primary shard copy has been migrated to remote",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );

        decision = getDecisionForTargetNode(nonRemoteNode, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals(
            "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node",
            decision.getExplanation().toLowerCase(Locale.ROOT)
        );
    }

    // test for STRICT mode

    public void testAlwaysAllocateNewShardForStrictMode() throws Exception {
        boolean isRemoteCluster = randomBoolean();
        boolean isReplicaAllocation = randomBoolean();

        logger.info(" --> initialize cluster and add nodes");
        initializeCluster(isRemoteCluster);
        String nodeName1 = internalCluster().startNode();
        String nodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode node1 = assertNodeInCluster(nodeName1);
        DiscoveryNode node2 = assertNodeInCluster(nodeName2);

        logger.info(" --> verify expected decision for allocating a new shard on a non-remote node");
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(node1, Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE.direction);

        assertEquals(
            (isRemoteCluster ? "true" : null),
            client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index(TEST_INDEX)
                .getSettings()
                .get(SETTING_REMOTE_STORE_ENABLED)
        );

        prepareDecisions();

        Decision decision = getDecisionForTargetNode(
            isReplicaAllocation ? node2 : randomFrom(node1, node2),
            !isReplicaAllocation,
            true,
            false
        );
        assertEquals(Decision.Type.YES, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can be allocated to a %s node for strict compatibility mode",
            (isReplicaAllocation ? "replica" : "primary"),
            (isRemoteCluster ? "remote" : "non-remote")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> attempt allocation");
        attemptAllocation(Optional.empty());
        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(!isReplicaAllocation, !isReplicaAllocation ? Optional.empty() : Optional.of(node2));
    }

    // test for remote store backed index

    public void testDontAllocateToNonRemoteNodeForRemoteStoreBackedIndex() throws Exception {
        logger.info(" --> initialize cluster with remote master node");
        initializeCluster(true);

        logger.info(" --> add remote and non-remote nodes");
        String remoteNodeName = internalCluster().startNode();
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);

        boolean isReplicaAllocation = randomBoolean();

        logger.info(" --> verify expected decision for allocating a new shard on a non-remote node");
        if (isReplicaAllocation) {
            prepareIndexWithAllocatedPrimary(remoteNode, Optional.empty());
        } else {
            prepareIndexWithoutReplica(Optional.empty());
        }

        assertEquals(
            "true",
            client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index(TEST_INDEX)
                .getSettings()
                .get(SETTING_REMOTE_STORE_ENABLED)
        );

        setDirection(REMOTE_STORE.direction);
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, !isReplicaAllocation, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String expectedReason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertEquals(expectedReason, decision.getExplanation().toLowerCase(Locale.ROOT));

        logger.info(" --> attempt allocation of shard on non-remote node");
        attemptAllocation(Optional.of(nonRemoteNodeName));

        logger.info(" --> verify non-allocation of shard");
        assertNonAllocation(!isReplicaAllocation);
    }

    public void testTrial() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add non-remote nodes");
        String nonRemoteNodeName1 = internalCluster().startDataOnlyNode();
        String nonRemoteNodeName2 = internalCluster().startDataOnlyNode();
        String nonRemoteNodeName3 = internalCluster().startDataOnlyNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);
        DiscoveryNode nonRemoteNode3 = assertNodeInCluster(nonRemoteNodeName3);

        // Initially: Primary and 2 Replicas on non-remote nodes
        logger.info(" --> allocate primaries and replicas");
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
        createIndex(TEST_INDEX, indexSettings);
        ensureGreen(TEST_INDEX);

        // To start migration
        setClusterMode(MIXED.mode);
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> add remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startDataOnlyNode();
        String remoteNodeName2 = internalCluster().startDataOnlyNode();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);

        logger.info("---> Starting doc ingestion in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(TEST_INDEX);
        asyncIndexingService.startIndexing();

        String primaryShardNodeName = primaryNodeName(TEST_INDEX);
        logger.info(" --> migrate primary shard copy from {} to remote enabled node {}", primaryShardNodeName, remoteNodeName1);
        attemptRelocation(primaryShardNodeName, remoteNodeName1);
        ensureGreen(TEST_INDEX);

        String replicaShardNodeName1 = replicaNodeName(TEST_INDEX);
        logger.info(" --> migrate replica shard copy from {} to remote enabled node {}", replicaShardNodeName1, remoteNodeName2);
        attemptRelocation(replicaShardNodeName1, remoteNodeName2);
        ensureGreen(TEST_INDEX);

        logger.info("---> Stop remote store enabled node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName1));
        ensureStableCluster(5);
        ensureYellow(TEST_INDEX);
//
//        ShardRouting primaryShardRouting0 = getShardRouting(true);
//        String currentPrimaryNodeId0 = primaryShardRouting0.currentNodeId();
//        assertEquals(remoteNode1.getId(), currentPrimaryNodeId0);
//
//        logger.info("--> kill the remote node of the current primary");
//        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName1));
//        ensureStableCluster(5);

    }

    // test for primary promotion preferences

    // test for primary promotion preferences

    public void testPreferReplicaOnRemoteNodeForPrimaryPromotion() throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster(false);

        logger.info(" --> add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        String nonRemoteNodeName1 = internalCluster().startNode();
        String nonRemoteNodeName2 = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        String remoteNodeName3 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        DiscoveryNode remoteNode3 = assertNodeInCluster(remoteNodeName3);

        // Desired state: 1 primary, 4 replicas
        // Primary and 2 Replicas on remote nodes, 2 replicas on non-remote nodes
        logger.info(" --> allocate primaries and replicas");
        prepareIndexWithAllocatedPrimary(TEST_INDEX, 1, 4, remoteNodeName1);
        assertAllocation(true, Optional.of(remoteNode1));
        ensureYellowAndNoInitializingShards(TEST_INDEX);
        attemptAllocation(Optional.of(remoteNodeName2));
        ensureYellowAndNoInitializingShards(TEST_INDEX);
        attemptAllocation(Optional.of(remoteNodeName3));
        ensureYellowAndNoInitializingShards(TEST_INDEX);
        attemptAllocation(Optional.of(nonRemoteNodeName1));
        ensureYellowAndNoInitializingShards(TEST_INDEX);
        attemptAllocation(Optional.of(nonRemoteNodeName2));
        ensureGreen(TEST_INDEX);

        setDirection(REMOTE_STORE.direction);

        ShardRouting primaryShardRouting0 = getShardRouting(true);
        String currentPrimaryNodeId0 = primaryShardRouting0.currentNodeId();
        assertEquals(remoteNode1.getId(), currentPrimaryNodeId0);

        logger.info("--> kill the remote node of the current primary");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName1));
        ensureStableCluster(5);

        logger.info("--> verify new primary is on one of the remote nodes");
        ShardRouting primaryShardRouting1 = getShardRouting(true);
        String currentPrimaryNodeId1 = primaryShardRouting1.currentNodeId();
        assertTrue(currentPrimaryNodeId1.equals(remoteNode2.getId()) || currentPrimaryNodeId1.equals(remoteNode3.getId()));
        assertNotEquals(primaryShardRouting0, primaryShardRouting1);

        logger.info("--> again kill the remote node of the current primary");
        String nodeNameToKill = currentPrimaryNodeId1.equals(remoteNode2.getId()) ? remoteNodeName2 : remoteNodeName3;
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNameToKill));
        ensureStableCluster(4);

        logger.info("--> verify new primary is on one of the remote nodes");
        ShardRouting primaryShardRouting2 = getShardRouting(true);
        String currentPrimaryNodeId2 = primaryShardRouting2.currentNodeId();
        assertTrue(currentPrimaryNodeId2.equals(remoteNode2.getId()) || currentPrimaryNodeId2.equals(remoteNode3.getId()));
        assertNotEquals(primaryShardRouting1, primaryShardRouting2);
        assertNotEquals(currentPrimaryNodeId1, currentPrimaryNodeId2);

        logger.info("--> again kill the remote node of the current primary");
        nodeNameToKill = currentPrimaryNodeId2.equals(remoteNode2.getId()) ? remoteNodeName2 : remoteNodeName3;
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNameToKill));
        ensureStableCluster(3);

        logger.info("--> verify new primary is on one of the non-remote nodes");
        ShardRouting primaryShardRouting3 = getShardRouting(true);
        String currentPrimaryNodeId3 = primaryShardRouting3.currentNodeId();
        assertTrue(currentPrimaryNodeId3.equals(nonRemoteNode1.getId()) || currentPrimaryNodeId3.equals(nonRemoteNode2.getId()));
        assertNotEquals(primaryShardRouting2, primaryShardRouting3);
        assertNotEquals(currentPrimaryNodeId2, currentPrimaryNodeId3);
    }

    // bootstrap a cluster
    public void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startNodes(1);
        client = internalCluster().client();
        setClusterMode(STRICT.mode);
        setDirection(NONE.direction);
    }

    // set the compatibility mode of cluster [strict, mixed]
    public static void setClusterMode(String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster [remote_store, docrep, none]
    public static void setDirection(String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
    public static DiscoveryNode assertNodeInCluster(String nodeName) {
        Map<String, DiscoveryNode> nodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes().getNodes();
        DiscoveryNode discoveryNode = null;
        for (Map.Entry<String, DiscoveryNode> entry : nodes.entrySet()) {
            DiscoveryNode node = entry.getValue();
            if (node.getName().equals(nodeName)) {
                discoveryNode = node;
                break;
            }
        }
        assertNotNull(discoveryNode);
        return discoveryNode;
    }

    // returns a comma-separated list of node names excluding `except`
    public static String allNodesExcept(String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = internalCluster().client().admin().cluster().prepareState().get().getState().nodes();
        for (DiscoveryNode node : allNodes) {
            if (node.getName().equals(except) == false) {
                exclude.append(node.getName()).append(",");
            }
        }
        return exclude.toString();
    }

    // obtain decision for allocation/relocation of a shard to a given node
    private Decision getDecisionForTargetNode(
        DiscoveryNode targetNode,
        boolean isPrimary,
        boolean includeYesDecisions,
        boolean isRelocation
    ) {
        ClusterAllocationExplanation explanation = client.admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex(TEST_INDEX)
            .setShard(0)
            .setPrimary(isPrimary)
            .setIncludeYesDecisions(includeYesDecisions)
            .get()
            .getExplanation();

        Decision requiredDecision = null;
        List<NodeAllocationResult> nodeAllocationResults;
        if (isRelocation) {
            MoveDecision moveDecision = explanation.getShardAllocationDecision().getMoveDecision();
            nodeAllocationResults = moveDecision.getNodeDecisions();
        } else {
            AllocateUnassignedDecision allocateUnassignedDecision = explanation.getShardAllocationDecision().getAllocateDecision();
            nodeAllocationResults = allocateUnassignedDecision.getNodeDecisions();
        }

        for (NodeAllocationResult nodeAllocationResult : nodeAllocationResults) {
            if (nodeAllocationResult.getNode().equals(targetNode)) {
                for (Decision decision : nodeAllocationResult.getCanAllocateDecision().getDecisions()) {
                    if (decision.label().equals(NAME)) {
                        requiredDecision = decision;
                        break;
                    }
                }
            }
        }

        assertNotNull(requiredDecision);
        return requiredDecision;
    }

    // create a new test index
    public static void prepareIndexWithoutReplica(Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    public void prepareIndexWithAllocatedPrimary(DiscoveryNode primaryShardNode, Optional<String> name) {
        String indexName = name.orElse(TEST_INDEX);
        prepareIndexWithAllocatedPrimary(indexName, 1, 1, primaryShardNode.getName());

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        assertAllocation(true, Optional.of(primaryShardNode));

        logger.info(" --> verify non-allocation of replica shard");
        assertNonAllocation(false);
    }

    public void prepareIndexWithAllocatedPrimary(String indexName, int shardCount, int replicaCount, String nodeName) {
        client.admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", shardCount)
                    .put("index.number_of_replicas", replicaCount)
                    .put("index.routing.allocation.include._name", nodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nodeName))
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();
    }

    // get allocation and relocation decisions for all nodes
    private void prepareDecisions() {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", allNodesExcept(null)))
            .execute()
            .actionGet();
    }

    // attempt allocating the shard copy to targetNode
    private void attemptAllocation(Optional<String> targetNodeName) {
        String nodeName = targetNodeName.orElse(null);
        Settings.Builder settingsBuilder;
        if (nodeName != null) {
            settingsBuilder = Settings.builder()
                .put("index.routing.allocation.include._name", nodeName)
                .put("index.routing.allocation.exclude._name", allNodesExcept(nodeName));
        } else {
            String clusterManagerNodeName = client.admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getNodes()
                .getClusterManagerNode()
                .getName();
            // to allocate freely among all nodes other than cluster-manager node
            settingsBuilder = Settings.builder()
                .put("index.routing.allocation.include._name", allNodesExcept(clusterManagerNodeName))
                .put("index.routing.allocation.exclude._name", clusterManagerNodeName);
        }
        client.admin().indices().prepareUpdateSettings(TEST_INDEX).setSettings(settingsBuilder).execute().actionGet();
    }

    // attempt relocating the shard copy from currentNode to targetNode
    private ClusterRerouteResponse attemptRelocation(String currentNodeName, String targetNodeName) {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.enable", "none")
                    .put("index.routing.allocation.include._name", targetNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(targetNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        ClusterRerouteResponse rerouteResponse = client.admin()
            .cluster()
            .prepareReroute()
            .setExplain(true)
            .add(new MoveAllocationCommand(TEST_INDEX, 0, currentNodeName, targetNodeName))
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        return rerouteResponse;
    }

    private ShardRouting getShardRouting(boolean isPrimary) {
        IndexShardRoutingTable table = client.admin()
            .cluster()
            .prepareState()
            .execute()
            .actionGet()
            .getState()
            .getRoutingTable()
            .index(TEST_INDEX)
            .shard(0);
        return (isPrimary ? table.primaryShard() : table.replicaShards().get(0));
    }

    // verify that shard does not exist at targetNode
    private void assertNonAllocation(boolean isPrimary) {
        if (isPrimary) {
            ensureRed(TEST_INDEX);
        } else {
            ensureYellowAndNoInitializingShards(TEST_INDEX);
        }
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertFalse(shardRouting.active());
        assertNull(shardRouting.currentNodeId());
        assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
    }

    // verify that shard exists at targetNode
    private void assertAllocation(boolean isPrimary, Optional<DiscoveryNode> targetNode) {
        ShardRouting shardRouting = getShardRouting(isPrimary);
        assertTrue(shardRouting.active());
        assertNotNull(shardRouting.currentNodeId());
        DiscoveryNode node = targetNode.orElse(null);
        if (node != null) {
            assertEquals(shardRouting.currentNodeId(), node.getId());
        }
    }

}
