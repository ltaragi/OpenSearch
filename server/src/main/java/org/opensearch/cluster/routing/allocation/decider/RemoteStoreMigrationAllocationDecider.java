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

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;
import org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * An allocation decider to oversee shard allocation or relocation to facilitate remote-store migration:
 * - For "REMOTE_STORE" migration direction:
 *      - New primary shards can only be allocated to a remote node
 *      - New replica shards can be allocated to a remote node iff the primary has been migrated/allocated to a remote node
 * - For STRICT compatibility mode, the migration direction has to be same as type of target node
 * - For remote_store_enabled indices, relocation or allocation/relocation can only be towards a remote node
 *
 * @opensearch.internal
 */
public class RemoteStoreMigrationAllocationDecider extends AllocationDecider {

    public static final String NAME = "remote_store_migration";

    private Direction migrationDirection;
    private CompatibilityMode compatibilityMode;
    private boolean remoteStoreEnabledIndex = false;

    public RemoteStoreMigrationAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.migrationDirection = RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING.get(settings);
        this.compatibilityMode = RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(settings);
        this.remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings);
        if (IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(settings)) {
            remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings);
        }
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING,
            this::setMigrationDirection
        );
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
            this::setCompatibilityMode
        );
    }

    // listen for changes in migration direction of cluster
    private void setMigrationDirection(Direction migrationDirection) {
        this.migrationDirection = migrationDirection;
    }

    // listen for changes in compatibility mode of cluster
    private void setCompatibilityMode (CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        DiscoveryNode targetNode = node.node();
        String reason = checkStrictModeNoDecisions(shardRouting, targetNode, allocation);
        if (reason != null) {
            return allocation.decision(Decision.NO, NAME, reason);
        }

        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(indexMetadata.getSettings())) {
            remoteStoreEnabledIndex = IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings());
        }
        reason = checkIndexMetadataDecisions(shardRouting, targetNode, allocation);
        if (reason != null) {
            return allocation.decision(Decision.NO, NAME, reason);
        }

        if (shardRouting.primary()) {
            if (!shardRouting.assignedToNode()) {
                return newPrimaryAllocation(allocation, targetNode);
            }
            return allocation.decision(Decision.YES, NAME, "relocation of primary shard copy");
        }
        else {
            ShardRouting primaryShardRouting = allocation.routingNodes().activePrimary(shardRouting.shardId());
            if (primaryShardRouting == null) {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.NO, NAME, "cannot allocate replica as no active primary shard yet");
            }

            if (!shardRouting.assignedToNode()) {
                return newReplicaAllocation(allocation, targetNode, primaryShardRouting);
            }
            return allocation.decision(Decision.YES, NAME, "relocation of replica shard copy");
        }
    }

    // handle scenarios for allocation of a new shard's primary copy
    private Decision newPrimaryAllocation(RoutingAllocation allocation, DiscoveryNode targetNode) {
        if (migrationDirection.equals(Direction.REMOTE_STORE)) {
            if (!targetNode.isRemoteStoreNode()) {
                return allocation.decision(Decision.NO, NAME,
                    getReason(false, true, true, targetNode, ""));
            }
            return allocation.decision(Decision.YES, NAME,
                getReason(true, true, true, targetNode, ""));
        }
        return allocation.decision(Decision.YES, NAME, "allocation of primary shard copy for non remote_store direction");
    }

    // handle scenarios for allocation of a new shard's replica copy
    private Decision newReplicaAllocation(RoutingAllocation allocation, DiscoveryNode targetNode, ShardRouting primaryShardRouting) {
        if (migrationDirection.equals(Direction.REMOTE_STORE)) {
            DiscoveryNode primaryShardNode = allocation.routingNodes()
                .stream()
                .filter(nd -> nd.nodeId().equals(primaryShardRouting.currentNodeId()))
                .findFirst().get().node();
            if (targetNode.isRemoteStoreNode()) {
                if (!primaryShardNode.isRemoteStoreNode()) {
                    return allocation.decision(Decision.NO, NAME,
                        getReason(false, true, false, targetNode, " since primary shard copy is not yet migrated to remote"));

                }
                return allocation.decision(Decision.YES, NAME,
                    getReason(true, true, false, targetNode, " since primary shard copy has been migrated to remote"));
            }
            return allocation.decision(Decision.YES, NAME,
                getReason(true, true, false, targetNode, ""));
        }
        return allocation.decision(Decision.YES, NAME, "allocation of replica shard copy for non remote_store direction");
    }

    private String checkStrictModeNoDecisions (ShardRouting shardRouting, DiscoveryNode targetNode, RoutingAllocation allocation) {
        if (compatibilityMode.equals(CompatibilityMode.STRICT)) {
            if (migrationDirection.equals(Direction.REMOTE_STORE) && !targetNode.isRemoteStoreNode()) {
                return getReason(false, !shardRouting.assignedToNode(), shardRouting.primary(), targetNode,
                    " because target node cannot be non-remote in STRICT mode");
            }
        }
        return null;
    }

    private String checkIndexMetadataDecisions (ShardRouting shardRouting, DiscoveryNode targetNode, RoutingAllocation allocation) {
        // mismatched current node type with index metadata
        if (shardRouting.assignedToNode()) {
            DiscoveryNode currentNode = allocation.nodes().getNodes().get(shardRouting.currentNodeId());
            if (remoteStoreEnabledIndex && !currentNode.isRemoteStoreNode()) {
                // other checks should prevent this case
                return getReason(false, false, shardRouting.primary(), targetNode,
                    " because remote_store_enabled index found on a non remote node");
            }
        }
        // allocations and relocations must be towards a remote node
        if (remoteStoreEnabledIndex && !migrationDirection.equals(Direction.DOCREP)) {
            if (!targetNode.isRemoteStoreNode()) {
                return getReason(false, !shardRouting.assignedToNode(), shardRouting.primary(), targetNode,
                    " for remote_store_enabled index");
            }
        }
        return null;
    }

    // get given node's type
    private String getNodeType(DiscoveryNode node) {
        if(migrationDirection.equals(Direction.REMOTE_STORE)) {
            return(node.isRemoteStoreNode() ? "remote_store" : "non_remote_store");
        }
        return(node.isRemoteStoreNode() ? "remote_store/non_docrep" : "non_remote_store/docrep");
    }

    // get descriptive reason for the decision
    private String getReason(boolean isYes, boolean isNewShard, boolean isPrimary, DiscoveryNode targetNode, String details) {
        return String.format(
            "[%s migration_direction]: %s shard copy %s be %s to a %s node%s",
            migrationDirection,
            (isPrimary ? "primary" : "replica"),
            (isYes ? "can" : "can not"),
            (isNewShard ? "allocated" : "relocated"),
            getNodeType(targetNode),
            details
        );
    }

}
