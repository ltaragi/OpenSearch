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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * An allocation decider to oversee shard allocation or relocation to remote-store enabled
 * nodes. If Direction is set as "REMOTE_STORE", new primary shards can only go to remote-store enabled
 * nodes. Replica shards can go to remote nodes only if corresponding primary also exists on a remote node.
 *
 * @opensearch.internal
 */
public class RemoteStoreAllocationDecider extends AllocationDecider {
    public static final String NAME = "remote_store_enabled_cluster";

    private Direction direction;

    public RemoteStoreAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.direction = RemoteStoreNodeService.DIRECTION_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(RemoteStoreNodeService.DIRECTION_SETTING, this::setDirection);
    }

    private void setDirection (Direction direction) {
        this.direction = direction;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        DiscoveryNode targetNode = node.node();
        if (!shardRouting.assignedToNode()){
            if (shardRouting.primary()) {
                if (direction.equals(Direction.REMOTE_STORE) && !targetNode.isRemoteStoreNode()) {
                    return allocation.decision(Decision.NO, NAME,
                        "for REMOTE_STORE direction, new primary shards can not be allocated to non-remote nodes");
                }
            }
            else {
                ShardRouting primaryShardRouting = allocation.routingNodes().activePrimary(shardRouting.shardId());
                if (primaryShardRouting == null) {
                    // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                    return allocation.decision(Decision.NO, NAME, "no active primary shard yet");
                }

                DiscoveryNode primaryShardNode = allocation.routingNodes()
                    .stream()
                    .filter(nd -> nd.nodeId().equals(primaryShardRouting.currentNodeId()))
                    .findFirst().get().node();

                if (direction.equals(Direction.REMOTE_STORE)) {
                    if (!primaryShardNode.isRemoteStoreNode() && targetNode.isRemoteStoreNode()) {
                        return allocation.decision(Decision.NO, NAME,
                            "can not allocate replica shard on a remote node when primary shard is not already active on some remote node");
                    }
                }
            }
            return allocation.decision(
                Decision.YES,
                NAME,
                "for %s direction, allocation of a %s shard is allowed on a %s",
                direction,
                (shardRouting.primary() ? "primary" : "replica"),
                isRemoteStoreEnabled(targetNode)
            );
        }
        return allocation.decision(Decision.YES, NAME, "it is a relocation, returning YES for now");
    }

    private static String isRemoteStoreEnabled (DiscoveryNode node) {
        return (node.isRemoteStoreNode() ? "remote_store_enabled" : "non_remote_store_enabled") + " node";
    }

}
