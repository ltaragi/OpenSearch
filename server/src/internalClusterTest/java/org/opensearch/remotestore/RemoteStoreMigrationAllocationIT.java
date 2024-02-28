/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.*;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationAllocationIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_index";

    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String NONE_DIRECTION = "none";

    private final static String STRICT_MODE = "strict";
    private final static String MIXED_MODE = "mixed";


    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected static final String NAME = "remote_store_migration";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;

    static boolean addRemote = false;
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

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalOrPrivateSettingsPlugin.class);
    }


    // tests for primary shard copy allocation with REMOTE_STORE direction

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
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isMixedMode = randomBoolean();
        if (isMixedMode) {
            setClusterMode(MIXED_MODE);
        }

        logger.info(" --> verify expected decision for allocating a new primary shard on a non-remote node");
        boolean isRemoteStoreEnabledIndex = randomBoolean();
        prepareIndex(1, 0, getRemoteStoreEnabledIndexSettingsBuilder(isRemoteStoreEnabledIndex));
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        String reason = "[REMOTE_STORE migration_direction]: primary shard copy can not be allocated to a non_remote_store node";
        if (!isMixedMode) {
            //  strict mode
            reason = reason.concat(" because target node cannot be non-remote in STRICT mode");
        }
        else if (isRemoteStoreEnabledIndex) {
            reason = reason.concat(" for remote_store_enabled index");
        }
        assertEquals(reason, decision.getExplanation());

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        ensureRed(TEST_INDEX);

        logger.info(" --> verify non-allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertNonAllocation(primaryShardRouting);
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote node");
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isStrictMode = randomBoolean();
        if (isStrictMode) {
            setClusterMode(STRICT_MODE);
        }

        logger.info(" --> verify expected decision for allocating a new primary shard on a remote node");
        boolean isRemoteStoreEnabledIndex = randomBoolean();
        prepareIndex(1, 0, getRemoteStoreEnabledIndexSettingsBuilder(isRemoteStoreEnabledIndex));
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        Decision decision = getDecisionForTargetNode(remoteNode, true, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: primary shard copy can be allocated to a remote_store node", decision.getExplanation());

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode);
    }


    // tests for replica shard copy allocation with REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

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
                getRemoteStoreEnabledIndexSettingsBuilder(false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, nonRemoteNode);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        if (isRemoteStoreEnabledIndex) {
            setRemoteStoreEnabledIndex();
        }
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        boolean isStrictMode = randomBoolean();
        if (isStrictMode) {
            setClusterMode(STRICT_MODE);
        }

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode, false, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a remote_store node since primary shard copy is not yet migrated to remote", decision.getExplanation());

        logger.info(" --> attempt allocation of replica shard on remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .execute()
            .actionGet();

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify non-allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

        logger.info(" --> add remote nodes");
        addRemote = true;
        String remoteNodeName1 = internalCluster().startNode();
        String remoteNodeName2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode remoteNode1 = assertNodeInCluster(remoteNodeName1);
        DiscoveryNode remoteNode2 = assertNodeInCluster(remoteNodeName2);
        assertTrue(remoteNode1.isRemoteStoreNode());
        assertTrue(remoteNode2.isRemoteStoreNode());

        logger.info(" --> allocate primary shard on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                getRemoteStoreEnabledIndexSettingsBuilder(false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName1)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName1))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode1);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        if (isRemoteStoreEnabledIndex) {
            setRemoteStoreEnabledIndex();
        }
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        boolean isStrictMode = randomBoolean();
        if (isStrictMode) {
            setClusterMode(STRICT_MODE);
        }

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode2, false, true, false);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a remote_store node since primary shard copy has been migrated to remote", decision.getExplanation());

        logger.info(" --> attempt allocation of replica shard the other remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", remoteNodeName2)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName2))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of replica shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertAllocation(replicaShardRouting, remoteNode2);
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
                getRemoteStoreEnabledIndexSettingsBuilder(false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName1)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName1))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, nonRemoteNode1);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        if (isRemoteStoreEnabledIndex) {
            setRemoteStoreEnabledIndex();
        }
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        boolean isMixedMode = randomBoolean();
        if (isMixedMode) {
            setClusterMode(MIXED_MODE);
        }

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode2, false, true, false);
        Decision.Type type = Decision.Type.YES;
        String reason = "[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a non_remote_store node";
        if (!isMixedMode) {
            type = Decision.Type.NO;
            reason = "[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a non_remote_store node because target node cannot be non-remote in STRICT mode";
        }
        else if (isRemoteStoreEnabledIndex) {
            type = Decision.Type.NO;
            reason = "[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a non_remote_store node for remote_store_enabled index";
        }
        assertEquals(type, decision.type());
        assertEquals(reason, decision.getExplanation());

        logger.info(" --> allocate replica shard on the other non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName2)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName2))
            )
            .execute()
            .actionGet();

        if (!isMixedMode || isRemoteStoreEnabledIndex) {
            ensureYellowAndNoInitializingShards(TEST_INDEX);

            logger.info(" --> verify non-allocation of replica shard");
            routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
            replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
            assertNonAllocation(replicaShardRouting);
        }
        else {
            ensureGreen(TEST_INDEX);

            logger.info(" --> verify allocation of replica shard");
            routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
            replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
            assertAllocation(replicaShardRouting, nonRemoteNode2);
        }
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () throws Exception {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> set mixed cluster compatibility mode");
        setClusterMode(MIXED_MODE);

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

        logger.info(" --> allocate primary on remote node");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                getRemoteStoreEnabledIndexSettingsBuilder(false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .setWaitForActiveShards(ActiveShardCount.ONE)
            .execute()
            .actionGet();

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode);

        logger.info(" --> verify non-allocation of replica shard");
        ShardRouting replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
        assertNonAllocation(replicaShardRouting);

        logger.info(" --> set remote_store direction");
        setDirection(REMOTE_STORE_DIRECTION);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        if (isRemoteStoreEnabledIndex) {
            setRemoteStoreEnabledIndex();
        }
        assertEquals((isRemoteStoreEnabledIndex ? "true" : null), client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        boolean isStrictMode = randomBoolean();
        if (isStrictMode) {
            setClusterMode(STRICT_MODE);
        }

        logger.info(" --> verify expected decision for replica shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, false, true, false);

        Decision.Type type = Decision.Type.YES;
        String reason = "[REMOTE_STORE migration_direction]: replica shard copy can be allocated to a non_remote_store node";
        if (isStrictMode) {
            type = Decision.Type.NO;
            reason = "[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a non_remote_store node because target node cannot be non-remote in STRICT mode";
        }
        else if (isRemoteStoreEnabledIndex) {
            type = Decision.Type.NO;
            reason = "[REMOTE_STORE migration_direction]: replica shard copy can not be allocated to a non_remote_store node for remote_store_enabled index";
        }
        assertEquals(type, decision.type());
        assertEquals(reason, decision.getExplanation());

        logger.info(" --> allocate replica shard on non-remote node");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        if (isStrictMode || isRemoteStoreEnabledIndex) {
            ensureYellowAndNoInitializingShards(TEST_INDEX);

            logger.info(" --> verify non-allocation of replica shard");
            routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
            replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
            assertNonAllocation(replicaShardRouting);
        }
        else {
            ensureGreen(TEST_INDEX);

            logger.info(" --> verify allocation of replica shard");
            routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
            replicaShardRouting = routingTable.index(TEST_INDEX).shard(0).replicaShards().get(0);
            assertAllocation(replicaShardRouting, nonRemoteNode);
        }
    }


    // remote store enabled index no decisions

    public void testReturnNoForRelocationOfRemoteStoreEnabledIndexFromNonRemoteNode () {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add nodes");
        setClusterMode(MIXED_MODE);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> allocate primary on nonRemoteNode");
        Settings.Builder remoteStoreEnabledIndexSettingsBuilder = getRemoteStoreEnabledIndexSettingsBuilder(false);
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                remoteStoreEnabledIndexSettingsBuilder
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, nonRemoteNode);

        logger.info(" --> set remote_store_enabled index");
        setRemoteStoreEnabledIndex();
        setDirection(REMOTE_STORE_DIRECTION);

        logger.info(" --> verify expected decision for relocating the primary shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(remoteNode, true, false, true);
        assertEquals(Decision.Type.NO, decision.type());
        String reason = "[REMOTE_STORE migration_direction]: primary shard copy can not be relocated to a remote_store node because remote_store_enabled index found on a non remote node";
        assertEquals(reason, decision.getExplanation());

        logger.info(" --> attempt relocation of primary shard");
        ClusterRerouteResponse rerouteResponse = relocateShard(nonRemoteNodeName, remoteNodeName);
        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify non-relocation of primary shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertNonRelocation(primaryShardRouting, rerouteResponse, nonRemoteNode, remoteNode, reason);
    }

    public void testDontAllocateToNonRemoteNodeForRemoteStoreEnabledIndex () {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add non-remote node");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        logger.info(" --> set none direction");
        setDirection(NONE_DIRECTION);
        setClusterMode(MIXED_MODE);

        logger.info(" --> verify expected decision for allocating a new primary shard on a non-remote node");
        prepareIndex(1, 0, getRemoteStoreEnabledIndexSettingsBuilder(true));
        assertEquals("true", client.admin().cluster().prepareState().execute()
            .actionGet().getState().getMetadata().index(TEST_INDEX).getSettings().get(SETTING_REMOTE_STORE_ENABLED));

        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, false, false);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals("[NONE migration_direction]: primary shard copy can not be allocated to a non_remote_store/docrep node for remote_store_enabled index", decision.getExplanation());

        logger.info(" --> attempt allocation");
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", nonRemoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(nonRemoteNodeName))
            )
            .execute()
            .actionGet();

        ensureRed(TEST_INDEX);

        logger.info(" --> verify non-allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertNonAllocation(primaryShardRouting);
    }

    public void testDontRelocateToNonRemoteNodeForRemoteStoreEnabledIndex () {
        logger.info(" --> initialize cluster");
        initializeCluster();

        logger.info(" --> add nodes");
        setClusterMode(MIXED_MODE);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        DiscoveryNode nonRemoteNode = assertNodeInCluster(nonRemoteNodeName);
        DiscoveryNode remoteNode = assertNodeInCluster(remoteNodeName);
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        assertTrue(remoteNode.isRemoteStoreNode());

        logger.info(" --> allocate primary on remoteNode");
        Settings.Builder remoteStoreEnabledIndexSettingsBuilder = getRemoteStoreEnabledIndexSettingsBuilder(false);
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                remoteStoreEnabledIndexSettingsBuilder
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.include._name", remoteNodeName)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(remoteNodeName))
            )
            .execute()
            .actionGet();

        ensureGreen(TEST_INDEX);

        logger.info(" --> verify allocation of primary shard");
        RoutingTable routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        ShardRouting primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertAllocation(primaryShardRouting, remoteNode);

        logger.info(" --> set remote_store_enabled index");
        setRemoteStoreEnabledIndex();
        setDirection(NONE_DIRECTION);

        logger.info(" --> verify expected decision for relocating the primary shard");
        prepareDecisions();
        Decision decision = getDecisionForTargetNode(nonRemoteNode, true, false, true);
        assertEquals(Decision.Type.NO, decision.type());
        String reason = "[NONE migration_direction]: primary shard copy can not be relocated to a non_remote_store/docrep node for remote_store_enabled index";
        assertEquals(reason, decision.getExplanation());

        logger.info(" --> attempt relocation of primary shard");
        ClusterRerouteResponse rerouteResponse = relocateShard(remoteNodeName, nonRemoteNodeName);
        ensureYellowAndNoInitializingShards(TEST_INDEX);

        logger.info(" --> verify non-relocation of primary shard");
        routingTable = client.admin().cluster().prepareState().execute().actionGet().getState().getRoutingTable();
        primaryShardRouting = routingTable.index(TEST_INDEX).shard(0).primaryShard();
        assertNonRelocation(primaryShardRouting, rerouteResponse, remoteNode, nonRemoteNode, reason);
    }


    // bootstrap a cluster
    private void initializeCluster () {
        addRemote = false;
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startNodes(1);
        client = internalCluster().client();
        setClusterMode(STRICT_MODE);
        setDirection(NONE_DIRECTION);
    }

    // set the compatibility mode of cluster [strict, mixed]
    private void setClusterMode (String mode) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // set the migration direction for cluster [remote_store, docrep, none]
    private void setDirection (String direction) {
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), direction));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    // verify that the given nodeName exists in cluster
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

    // returns a comma-separated list of node names excluding `except`
    private String allNodesExcept (String except) {
        StringBuilder exclude = new StringBuilder();
        DiscoveryNodes allNodes = client.admin().cluster().prepareState().get().getState().nodes();
        for (DiscoveryNode node: allNodes) {
            if (!(node.getName().equals(except))) {
                exclude.append(node.getName()).append(",");
            }
        }
        return exclude.toString();
    }

    // obtain decision for allocation/relocation of a shard to a given node
    private Decision getDecisionForTargetNode (DiscoveryNode targetNode, boolean isPrimary, boolean includeYesDecisions, boolean isRelocation) {
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
        }
        else {
            AllocateUnassignedDecision allocateUnassignedDecision = explanation.getShardAllocationDecision().getAllocateDecision();
            nodeAllocationResults = allocateUnassignedDecision.getNodeDecisions();
        }

        for (NodeAllocationResult nodeAllocationResult : nodeAllocationResults) {
            if (nodeAllocationResult.getNode().equals(targetNode)) {
                for (Decision decision: nodeAllocationResult.getCanAllocateDecision().getDecisions()) {
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
    private void prepareIndex (int shardCount, int replicaCount, Settings.Builder customSettingsBuilder) {
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                customSettingsBuilder
                    .put("index.number_of_shards", shardCount)
                    .put("index.number_of_replicas", replicaCount)
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    // get allocation and relocation decisions for all nodes
    private void prepareDecisions () {
        client.admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude._name", allNodesExcept(null))
            )
            .execute()
            .actionGet();
    }

    // attempt relocating the shard copy at currentNode to targetNode
    private ClusterRerouteResponse relocateShard (String currentNodeName, String targetNodeName) {
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

    // verify that shard does not exist at targetNode
    private void assertNonAllocation (ShardRouting shardRouting) {
        assertFalse(shardRouting.active());
        assertNull(shardRouting.currentNodeId());
        assertEquals(ShardRoutingState.UNASSIGNED, shardRouting.state());
    }

    // verify that shard exists at targetNode
    private void assertAllocation (ShardRouting shardRouting, DiscoveryNode targetNode) {
        assertTrue(shardRouting.active());
        assertNotNull(shardRouting.currentNodeId());
        assertEquals(shardRouting.currentNodeId(), targetNode.getId());
    }

    // verify that shard did not get relocated to the targetNode
    private void assertNonRelocation (ShardRouting shardRouting, ClusterRerouteResponse rerouteResponse, DiscoveryNode currentNode, DiscoveryNode targetNode, String reason) {
        Decision.Type decisionType = rerouteResponse.getExplanations().explanations().get(0).decisions().type();
        List<Decision> relocationDecisions = rerouteResponse.getExplanations().explanations().get(0).decisions().getDecisions();
        for (Decision dec: relocationDecisions) {
            if (dec.type().equals(Decision.Type.NO)) {
                // only one NO decision
                assertEquals(reason, dec.getExplanation());
            }
        }
        ShardRouting expectedShardRouting = rerouteResponse.getState().getRoutingTable().index(TEST_INDEX).shard(0).shards()
            .stream().filter(sr -> sr.allocationId().equals(shardRouting.allocationId())).findFirst().orElse(null);
        assertNotNull(expectedShardRouting);
        assertEquals(Decision.Type.NO, decisionType);
        assertNotNull(expectedShardRouting.currentNodeId());
        assertEquals(currentNode.getId(), expectedShardRouting.currentNodeId());
        assertNotEquals(targetNode.getId(), expectedShardRouting.currentNodeId());
    }

    // index settings builder for remote store enabled index
    private Settings.Builder getRemoteStoreEnabledIndexSettingsBuilder (boolean isRemoteStoreEnabledIndex) {
        Settings.Builder builder = Settings.builder()
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        if (isRemoteStoreEnabledIndex) {
            builder
                .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, REPOSITORY_NAME)
                .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_2_NAME)
                .put(SETTING_REMOTE_STORE_ENABLED, true);
        }
        return builder;
    }

    // to update index settings post intialization
    private void updatePrivateOrInternalIndexSetting (String settingKey, String value) {
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request(TEST_INDEX, settingKey, value)
        ).actionGet();
        final GetSettingsResponse responseAfterUpdate = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertEquals(value, responseAfterUpdate.getSetting(TEST_INDEX, settingKey));
        assertEquals(value, client.admin().indices().prepareGetSettings(TEST_INDEX).get().getSetting(TEST_INDEX, settingKey));
    }

    // to set index as remote_store_enabled post its intialization
    public void setRemoteStoreEnabledIndex () {
        updatePrivateOrInternalIndexSetting(SETTING_REMOTE_STORE_ENABLED, "true");
        updatePrivateOrInternalIndexSetting(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, REPOSITORY_NAME);
        updatePrivateOrInternalIndexSetting(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_2_NAME);
    }

}
