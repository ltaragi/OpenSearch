/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreShardAllocationIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_index";

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;

    static boolean addRemote = false;
    private String cmNodeName = null;
    private final ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
    private  Client client;

    protected Settings nodeSettings (int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote_store_enabled node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        } else {
            logger.info("Adding non_remote_store_enabled node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        }
    }

    @Override
    protected Settings featureFlagSettings () {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    private void initializeCluster () {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        cmNodeName = cmNodes.get(0);
        client = internalCluster().client();
    }

    private void setClusterMode (String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    private void setDirection (String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(DIRECTION_SETTING.getKey(), direction));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    private DiscoveryNode assertNodeInCluster (String nodeName) {
        Map<String, DiscoveryNode> nodes = client.admin().cluster().prepareState().get().getState().nodes().getNodes();
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

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add non-remote node");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate primary shard");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName)
            )
            .execute()
            .actionGet();

        logger.info(" --> verify non-allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertFalse(primaryShardRouting.active());
        assertNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.state(), ShardRoutingState.UNASSIGNED);

        logger.info("New primary shard can not be allocated to a non-remote node for remote_store direction");
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode("mixed");

        logger.info(" --> add remote node");
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate primary shard");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName)
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertTrue(primaryShardRouting.active());
        assertNotNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.currentNodeId(), remoteNode.getId());

        logger.info("--> New primary shard can only be allocated to a remote node for remote_store direction");
    }

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode("mixed");

        logger.info(" --> add remote and non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on non-remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + remoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertTrue(primaryShardRouting.active());
        assertNotNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.currentNodeId(), nonRemoteNode.getId());

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertFalse(replicaShardRouting.active());
        assertNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.state(), ShardRoutingState.UNASSIGNED);

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate replica shard on remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + nonRemoteNodeName)
            )
            .execute()
            .actionGet();

        logger.info(" --> verify non-allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertFalse(replicaShardRouting.active());
        assertNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.state(), ShardRoutingState.UNASSIGNED);
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode("mixed");

        logger.info(" --> add remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        assertTrue(remoteNode1.isRemoteStoreNode());
        assertTrue(remoteNode2.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate primary shard on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName1)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + remoteNodeName2)
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertTrue(primaryShardRouting.active());
        assertNotNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.currentNodeId(), remoteNode1.getId());

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertFalse(replicaShardRouting.active());
        assertNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.state(), ShardRoutingState.UNASSIGNED);

        logger.info(" --> allocate replica shard on the other remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName2)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + remoteNodeName1)
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertTrue(replicaShardRouting.active());
        assertNotNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.currentNodeId(), remoteNode2.getId());
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName1 = internalCluster().startNode();
        String nonRemoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode1 = assertNodeInCluster(nonRemoteNodeName1);
        DiscoveryNode nonRemoteNode2 = assertNodeInCluster(nonRemoteNodeName2);
        assertFalse(nonRemoteNode1.isRemoteStoreNode());
        assertFalse(nonRemoteNode2.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on non-remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName1)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + nonRemoteNodeName2)
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertTrue(primaryShardRouting.active());
        assertNotNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.currentNodeId(), nonRemoteNode1.getId());

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertFalse(replicaShardRouting.active());
        assertNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.state(), ShardRoutingState.UNASSIGNED);

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate replica shard on the other non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName2)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + nonRemoteNodeName1)
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shad");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertTrue(replicaShardRouting.active());
        assertNotNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.currentNodeId(), nonRemoteNode2.getId());
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode("mixed");

        logger.info(" --> add remote and non-remote nodes");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection("remote_store");

        logger.info(" --> allocate primary on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + nonRemoteNodeName)
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertTrue(primaryShardRouting.active());
        assertNotNull(primaryShardRouting.currentNodeId());
        assertEquals(primaryShardRouting.currentNodeId(), remoteNode.getId());

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertFalse(replicaShardRouting.active());
        assertNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.state(), ShardRoutingState.UNASSIGNED);

        logger.info(" --> allocate replica shard on non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", cmNodeName + "," + remoteNodeName)
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertTrue(replicaShardRouting.active());
        assertNotNull(replicaShardRouting.currentNodeId());
        assertEquals(replicaShardRouting.currentNodeId(), nonRemoteNode.getId());
    }


}
