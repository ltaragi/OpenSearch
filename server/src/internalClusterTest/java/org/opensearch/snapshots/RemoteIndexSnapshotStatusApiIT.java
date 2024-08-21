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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.snapshots.SnapshotsService.MAX_SHARDS_ALLOWED_IN_STATUS_API;
import static org.opensearch.snapshots.SnapshotsService.SHALLOW_SNAPSHOT_V2;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexSnapshotStatusApiIT extends AbstractSnapshotIntegTestCase {

    protected Path absolutePath;
    final String remoteStoreRepoName = "remote-store-repo-name";

    @Before
    public void setup() {
        absolutePath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .put(remoteStoreClusterSettings(remoteStoreRepoName, absolutePath))
            .build();
    }

    public void testStatusAPICallForShallowCopySnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used for the test");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final String remoteStoreEnabledIndexName = "remote-index";
        final String remoteStoreEnabledIndexName1 = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        createIndex(remoteStoreEnabledIndexName1, remoteStoreEnabledIndexSettings);
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(remoteStoreEnabledIndexName, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(remoteStoreEnabledIndexName1, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);

        assertBusy(() -> {
            // although no. of shards in snapshot (3) is greater than the max value allowed in a status api call, the request does not fail
            // TODO: currently this gives snapshot not found at repository issue --> discuss what to do here
            SnapshotStatus snapshotsStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(snapshotRepoName)
                .setSnapshots(snapshot)
                .setIndices(remoteStoreEnabledIndexName1)
                .get()
                .getSnapshots()
                .get(0);
            logger.info("*** snapshotsStatus = {}", snapshotsStatus);
        });
    }

    public void testStatusAPIStatsForBackToBackShallowSnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used for the test");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(remoteStoreEnabledIndexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        createFullSnapshot(snapshotRepoName, "test-snap-1");
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 1);

        SnapshotStatus snapshotStatus = getSnapshotStatus(snapshotRepoName, "test-snap-1");
        assertThat(snapshotStatus.getState(), is(SnapshotsInProgress.State.SUCCESS));

        SnapshotIndexShardStatus shallowSnapshotShardState = stateFirstShard(snapshotStatus, remoteStoreEnabledIndexName);
        assertThat(shallowSnapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
        final int totalFileCount = shallowSnapshotShardState.getStats().getTotalFileCount();
        final long totalSize = shallowSnapshotShardState.getStats().getTotalSize();
        final int incrementalFileCount = shallowSnapshotShardState.getStats().getIncrementalFileCount();
        final long incrementalSize = shallowSnapshotShardState.getStats().getIncrementalSize();

        createFullSnapshot(snapshotRepoName, "test-snap-2");
        assert (getLockFilesInRemoteStore(remoteStoreEnabledIndexName, remoteStoreRepoName).length == 2);

        snapshotStatus = getSnapshotStatus(snapshotRepoName, "test-snap-2");
        assertThat(snapshotStatus.getState(), is(SnapshotsInProgress.State.SUCCESS));
        shallowSnapshotShardState = stateFirstShard(snapshotStatus, remoteStoreEnabledIndexName);
        assertThat(shallowSnapshotShardState.getStats().getTotalFileCount(), equalTo(totalFileCount));
        assertThat(shallowSnapshotShardState.getStats().getTotalSize(), equalTo(totalSize));
        assertThat(shallowSnapshotShardState.getStats().getIncrementalFileCount(), equalTo(incrementalFileCount));
        assertThat(shallowSnapshotShardState.getStats().getIncrementalSize(), equalTo(incrementalSize));
    }

    public void testStatusAPICallInProgressShallowSnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used for the test");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy().put("block_on_data", true));

        final String remoteStoreEnabledIndexName = "remote-index-1";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreEnabledIndexName, remoteStoreEnabledIndexSettings);
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(remoteStoreEnabledIndexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot(snapshotRepoName, "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        awaitNumberOfSnapshotsInProgress(1);
        assertEquals(
            SnapshotsInProgress.State.STARTED,
            client().admin()
                .cluster()
                .prepareSnapshotStatus(snapshotRepoName)
                .setSnapshots("test-snap")
                .get()
                .getSnapshots()
                .get(0)
                .getState()
        );

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes(snapshotRepoName);

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testStatusAPICallForShallowV2Snapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used for the test");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final String index1 = "remote-index-1";
        final String index2 = "remote-index-2";
        final String index3 = "remote-index-3";
        final Settings remoteStoreEnabledIndexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(index1, remoteStoreEnabledIndexSettings);
        createIndex(index2, remoteStoreEnabledIndexSettings);
        createIndex(index3, remoteStoreEnabledIndexSettings);
        ensureGreen();

        logger.info("Indexing some data");
        for (int i = 0; i < 50; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        logger.info("*** triggering refresh from ");
        refresh();

        logger.info("Set SNAPSHOT_V2 as true and MAX_SHARDS_ALLOWED_IN_STATUS_API to a low value");
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey(), 2).put(SHALLOW_SNAPSHOT_V2.getKey(), true)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        final String snapshot = "snapshot";
        SnapshotInfo snapshotInfo = createFullSnapshot(snapshotRepoName, snapshot);
        assertTrue(snapshotInfo.getPinnedTimestamp() > 0); // to assert creation of a shallow v2 snapshot
        logger.info(
            "*** primarySize = {}",
            client().admin().indices().prepareStats().execute().actionGet().getPrimaries().getStore().size()
        );
        ClusterStatsIndices clusterStatsIndices = client().admin().cluster().prepareClusterStats().get().getIndicesStats();
        logger.info("*** clusterStatsIndices = {}", clusterStatsIndices.toString());
        logger.info("*** clusterStatsIndices.getStore().sizeInBytes() = {}", clusterStatsIndices.getStore().sizeInBytes());

        logger.info("Indexing some data");
        for (int i = 0; i < 50; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        logger.info("*** triggering refresh from ");
        refresh();

        final String snapshot1 = "snapshot1";
        SnapshotInfo snapshotInfo1 = createFullSnapshot(snapshotRepoName, snapshot1);
        assertTrue(snapshotInfo.getPinnedTimestamp() > 0); // to assert creation of a shallow v2 snapshot
        logger.info(
            "*** primarySize1 = {}",
            client().admin().indices().prepareStats().execute().actionGet().getPrimaries().getStore().size()
        );
        ClusterStatsIndices clusterStatsIndices1 = client().admin().cluster().prepareClusterStats().get().getIndicesStats();
        logger.info("*** clusterStatsIndices1 = {}", clusterStatsIndices.toString());
        logger.info("*** clusterStatsIndices.getStore().sizeInBytes()1 = {}", clusterStatsIndices.getStore().sizeInBytes());

        logger.info("Reset SNAPSHOT_V2 and MAX_SHARDS_ALLOWED_IN_STATUS_API to default values");
        updateSettingsRequest.persistentSettings(
            Settings.builder().putNull(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey()).putNull(SHALLOW_SNAPSHOT_V2.getKey())
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    private static SnapshotIndexShardStatus stateFirstShard(SnapshotStatus snapshotStatus, String indexName) {
        return snapshotStatus.getIndices().get(indexName).getShards().get(0);
    }

    private static SnapshotStatus getSnapshotStatus(String repoName, String snapshotName) {
        try {
            return client().admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotName).get().getSnapshots().get(0);
        } catch (SnapshotMissingException e) {
            throw new AssertionError(e);
        }
    }
}
