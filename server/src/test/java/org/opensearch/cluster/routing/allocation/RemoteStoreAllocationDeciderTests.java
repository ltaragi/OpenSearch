/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.*;
import org.opensearch.cluster.routing.allocation.decider.RemoteStoreAllocationDecider;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.decider.*;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.remotestore.RemoteStoreNodeService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.DIRECTION_SETTING;
import static org.hamcrest.core.Is.is;

public class RemoteStoreAllocationDeciderTests extends OpenSearchAllocationTestCase {
    private final Logger logger = LogManager.getLogger(RemoteStoreAllocationDeciderTests.class);
    private final String TEST_INDEX = "TEST_INDEX";

    private final Settings directionEnabledNodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();

    private final Settings remoteStoreDirectionSettings = Settings.builder()
        .put(DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
        .build();

    private final ClusterSettings remoteStoreDirectionClusterSettings = new ClusterSettings(remoteStoreDirectionSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    private ClusterState getInitialClusterState(Settings settings) {
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings)
            .put(IndexMetadata.builder(TEST_INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(TEST_INDEX)).build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
    }

    private DiscoveryNode getNonRemoteNode () {
        return new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public DiscoveryNode getRemoteNode () {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_VALUE");
        return new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }
    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        ClusterState clusterState = getInitialClusterState(remoteStoreDirectionSettings);
        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(nonRemoteNode)
                    .localNodeId(nonRemoteNode.getId())
                    .build()
            )
            .build();

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

        RemoteStoreAllocationDecider remoteStoreClusterAllocationDecider = new RemoteStoreAllocationDecider(remoteStoreDirectionSettings, remoteStoreDirectionClusterSettings);

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreClusterAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreClusterAllocationDecider.canAllocate(primaryShardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(decision.getExplanation(), is("for REMOTE_STORE direction, new primary shards can not be allocated to non-remote nodes"));
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForRemoteStoreDirection () {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        ClusterState clusterState = getInitialClusterState(remoteStoreDirectionSettings);

        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(remoteNode)
                    .localNodeId(remoteNode.getId())
                    .build()
            )
            .build();

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        RemoteStoreAllocationDecider remoteStoreClusterAllocationDecider = new RemoteStoreAllocationDecider(remoteStoreDirectionSettings, remoteStoreDirectionClusterSettings);

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreClusterAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreClusterAllocationDecider.canAllocate(primaryShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation(), is("for REMOTE_STORE direction, allocation of a primary shard is allowed on a remote_store_enabled node"));
    }

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection () {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());


        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT).put(remoteStoreDirectionSettings).put(directionEnabledNodeSettings))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId)
                            .addShard(
                                // primary on non-remote node
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    nonRemoteNode.getId(),
                                    true,
                                    ShardRoutingState.STARTED
                                )
                            )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        RemoteStoreAllocationDecider remoteStoreClusterAllocationDecider = new RemoteStoreAllocationDecider(remoteStoreDirectionSettings, remoteStoreDirectionClusterSettings);

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreClusterAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreClusterAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(decision.getExplanation(), is("can not allocate replica shard on a remote node when primary shard is not already active on some remote node"));

    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection () {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode remoteNode1 = getRemoteNode();
        assertTrue(remoteNode1.isRemoteStoreNode());
        DiscoveryNode remoteNode2 = getRemoteNode();
        assertTrue(remoteNode2.isRemoteStoreNode());


        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT).put(remoteStoreDirectionSettings).put(directionEnabledNodeSettings))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId)
                            .addShard(
                                // primary on remote node
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    remoteNode1.getId(),
                                    true,
                                    ShardRoutingState.STARTED
                                )
                            )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        RemoteStoreAllocationDecider remoteStoreClusterAllocationDecider = new RemoteStoreAllocationDecider(remoteStoreDirectionSettings, remoteStoreDirectionClusterSettings);

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreClusterAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreClusterAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation(), is("for REMOTE_STORE direction, allocation of a replica shard is allowed on a remote_store_enabled node"));
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeForRemoteStoreDirection () {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        assertFalse(nonRemoteNode1.isRemoteStoreNode());
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();
        assertFalse(nonRemoteNode2.isRemoteStoreNode());


        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT).put(remoteStoreDirectionSettings).put(directionEnabledNodeSettings))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId)
                            .addShard(
                                // primary shard on non-remote node
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    nonRemoteNode1.getId(),
                                    true,
                                    ShardRoutingState.STARTED
                                )
                            )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());

        RemoteStoreAllocationDecider remoteStoreClusterAllocationDecider = new RemoteStoreAllocationDecider(remoteStoreDirectionSettings, remoteStoreDirectionClusterSettings);

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreClusterAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreClusterAllocationDecider.canAllocate(replicaShardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation(), is("for REMOTE_STORE direction, allocation of a replica shard is allowed on a non_remote_store_enabled node"));
    }

}
