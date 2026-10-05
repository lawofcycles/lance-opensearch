/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.nio.file.Path;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceIndicesStatsActionFilter} replaces the store size of every
 * shard of a Lance backed index with the shard's
 * {@code docs.totalSizeInBytes}, leaves the shards of other indexes as
 * they are, and removes the docs group it added to a request that did
 * not ask for it.
 */
public class LanceIndicesStatsActionFilterTests extends OpenSearchTestCase {

    private static final long TABLE_BYTES = 123_456_789L;
    private static final long DIRECTORY_BYTES = 384L;

    private final Index lance = new Index("lance", "lance-uuid");
    private final Index plain = new Index("plain", "plain-uuid");

    private Metadata metadata() {
        return Metadata.builder()
            .clusterUUID("cluster-1")
            .put(indexMetadata(lance, Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/tables/demo.lance").build()), false)
            .put(indexMetadata(plain, Settings.EMPTY), false)
            .build();
    }

    private static IndexMetadata indexMetadata(Index index, Settings extra) {
        return IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(extra)
            )
            .build();
    }

    /** A started primary of {@code index} whose docs and store groups carry the given values, as a data node would report them. */
    private ShardStats shardStats(Index index, DocsStats docs, StoreStats store) {
        ShardId shardId = new ShardId(index, 0);
        ShardRouting routing = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        Path shardDir = createTempDir().resolve(index.getUUID()).resolve("0");
        CommonStats stats = new CommonStats(CommonStatsFlags.NONE);
        stats.docs = docs;
        stats.store = store;
        return new ShardStats.Builder().shardRouting(routing)
            .shardPath(new ShardPath(false, shardDir, shardDir, shardId))
            .commonStats(stats)
            .build();
    }

    private static IndicesStatsResponse response(ShardStats... shards) {
        return new IndicesStatsResponse(shards, shards.length, shards.length, 0, List.of());
    }

    private static StoreStats directoryStore() {
        return new StoreStats.Builder().sizeInBytes(DIRECTORY_BYTES).reservedSize(7L).build();
    }

    private static DocsStats docs(long count, long deleted, long totalSizeInBytes) {
        return new DocsStats.Builder().count(count).deleted(deleted).totalSizeInBytes(totalSizeInBytes).build();
    }

    public void testLanceShardStoreBecomesTheDocsTotalAndKeepsReservedBytes() {
        IndicesStatsResponse response = response(
            shardStats(lance, docs(14L, 2L, TABLE_BYTES), directoryStore()),
            shardStats(plain, docs(3L, 0L, 900L), directoryStore())
        );

        LanceIndicesStatsActionFilter.withTableStoreSizes(response, metadata(), false);

        CommonStats lanceStats = response.getAt(0).getStats();
        assertEquals(TABLE_BYTES, lanceStats.store.getSizeInBytes());
        assertEquals(7L, lanceStats.store.getReservedSize().getBytes());
        assertEquals("docs stay as the engine reported them", 14L, lanceStats.docs.getCount());
        assertEquals(2L, lanceStats.docs.getDeleted());

        CommonStats plainStats = response.getAt(1).getStats();
        assertEquals("a shard of an index that is not Lance backed is untouched", DIRECTORY_BYTES, plainStats.store.getSizeInBytes());
        assertEquals(3L, plainStats.docs.getCount());

        assertEquals(
            "the index level totals are built from the edited shards",
            TABLE_BYTES,
            response.getIndex("lance").getPrimaries().getStore().getSizeInBytes()
        );
        assertEquals(TABLE_BYTES + DIRECTORY_BYTES, response.getTotal().getStore().getSizeInBytes());
    }

    public void testDocsAddedForTheStoreValueAreRemovedAgain() {
        IndicesStatsResponse response = response(
            shardStats(lance, docs(14L, 2L, TABLE_BYTES), directoryStore()),
            shardStats(plain, docs(3L, 0L, 900L), directoryStore())
        );

        LanceIndicesStatsActionFilter.withTableStoreSizes(response, metadata(), true);

        assertEquals(TABLE_BYTES, response.getAt(0).getStats().store.getSizeInBytes());
        assertNull("the caller did not ask for docs", response.getAt(0).getStats().docs);
        assertEquals(DIRECTORY_BYTES, response.getAt(1).getStats().store.getSizeInBytes());
        assertNull(response.getAt(1).getStats().docs);
    }

    public void testShardWithoutDocsOrStoreIsLeftAlone() {
        // A request without the store group never reaches the edit; a
        // shard whose docs the data node did not report (a failed shard
        // answers with no groups) keeps the store it has.
        IndicesStatsResponse response = response(shardStats(lance, null, directoryStore()), shardStats(lance, docs(1L, 0L, 5L), null));

        LanceIndicesStatsActionFilter.withTableStoreSizes(response, metadata(), false);

        assertEquals(DIRECTORY_BYTES, response.getAt(0).getStats().store.getSizeInBytes());
        assertNull(response.getAt(1).getStats().store);
    }

    public void testIndexUnknownToTheClusterStateIsLeftAlone() {
        // The response names an index by name and uuid; one that was
        // deleted and recreated under the same name since the request
        // was routed does not match the metadata and is not edited.
        Index recreated = new Index("lance", "other-uuid");
        IndicesStatsResponse response = response(shardStats(recreated, docs(14L, 2L, TABLE_BYTES), directoryStore()));

        LanceIndicesStatsActionFilter.withTableStoreSizes(response, metadata(), false);

        assertEquals(DIRECTORY_BYTES, response.getAt(0).getStats().store.getSizeInBytes());
    }
}
