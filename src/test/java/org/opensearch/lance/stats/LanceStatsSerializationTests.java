/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.RootAllocator;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Wire and JSON shape of the stats transport classes, and the collector's
 * reading of a live {@link LanceWarmCache}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceStatsSerializationTests extends OpenSearchTestCase {

    private static LanceNodeStats sample() {
        return new LanceNodeStats(
            true,
            3,
            1,
            7L,
            4L,
            12L,
            4096L,
            65536L,
            9,
            30L,
            5L,
            2L,
            1L,
            2048L,
            3L,
            5000L,
            900L,
            17_179_869_183L,
            2,
            8_589_934_591L,
            1_000_000,
            "metadata",
            List.of(
                new LanceWarmUpStatus(
                    "perf",
                    "s3://bucket/perf.lance",
                    8L,
                    "metadata",
                    "done",
                    1_700_000_000_000L,
                    3.456d,
                    List.of(
                        new LanceWarmUpStatus.IndexEntry("rating_idx", "BTree", "rating", "done", 0.4d, ""),
                        new LanceWarmUpStatus.IndexEntry("body_idx", "Inverted", "body", "failed", 1.25d, "boom")
                    )
                )
            ),
            List.of(
                new LanceNodeStats.IndexReaderStats("big", 3_000_000_000L, 2_000_000_000L, true),
                new LanceNodeStats.IndexReaderStats("small", 120L, 120L, false)
            )
        );
    }

    public void testNodeStatsRoundTrip() throws Exception {
        LanceNodeStats original = sample();
        LanceNodeStats restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceNodeStats(in);
            }
        }
        assertEquals(original, restored);
        assertEquals(original.hashCode(), restored.hashCode());
    }

    public void testNodeStatsXContentShape() throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            sample().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals(
                "{\"snapshots\":{\"enabled\":true,\"count\":3,\"retired\":1,\"dataset_open_count\":7,\"snapshot_build_count\":4,"
                    + "\"snapshot_hit_count\":12},"
                    + "\"column_store\":{\"bytes\":4096,\"limit_bytes\":65536,\"entries\":9,\"hits\":30,\"loads\":5,\"evictions\":2,"
                    + "\"budget_misses\":1,\"heap_fallback_bytes\":2048,\"heap_fallback_rejections\":3},"
                    + "\"native_memory\":{\"estimated_bytes\":5000,\"session_bytes\":900,\"column_store_bytes\":4096,"
                    + "\"index_cache_capacity\":17179869183,\"index_cache_shards\":2,\"index_cache_shard_share\":8589934591},"
                    + "\"fts\":{\"subset_probe_limit\":1000000},"
                    + "\"warm_up\":{\"mode\":\"metadata\",\"tables\":[{\"index\":\"perf\",\"table\":\"s3://bucket/perf.lance\","
                    + "\"version\":8,\"mode\":\"metadata\",\"state\":\"done\",\"started_at\":\"2023-11-14T22:13:20Z\",\"seconds\":3.46,"
                    + "\"indexes\":[{\"name\":\"rating_idx\",\"type\":\"BTree\",\"column\":\"rating\",\"state\":\"done\",\"seconds\":0.4},"
                    + "{\"name\":\"body_idx\",\"type\":\"Inverted\",\"column\":\"body\",\"state\":\"failed\",\"seconds\":1.25,"
                    + "\"detail\":\"boom\"}]}]},"
                    + "\"indices\":{\"big\":{\"rows\":3000000000,\"shard_reader_rows\":2000000000,\"lucene_bound_exceeded\":true},"
                    + "\"small\":{\"rows\":120,\"shard_reader_rows\":120,\"lucene_bound_exceeded\":false}}}",
                builder.toString()
            );
        }
    }

    public void testResponseRoundTripAndShape() throws Exception {
        DiscoveryNode node = new DiscoveryNode(
            "node-1",
            "node-1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        LanceStatsResponse original = new LanceStatsResponse(
            new ClusterName("lance"),
            List.of(new LanceStatsNodeResponse(node, sample())),
            Collections.emptyList()
        );
        LanceStatsResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceStatsResponse(in);
            }
        }
        assertEquals("lance", restored.getClusterName().value());
        assertEquals(1, restored.getNodes().size());
        assertEquals("node-1", restored.getNodes().get(0).getNode().getId());
        assertEquals(sample(), restored.getNodes().get(0).stats());
        assertTrue(restored.failures().isEmpty());

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            restored.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertTrue(json, json.startsWith("{\"nodes\":{\"node-1\":{\"name\":\"node-1\",\"snapshots\":{"));
            assertTrue(json, json.contains("\"fts\":{\"subset_probe_limit\":1000000},\"warm_up\":{\"mode\":\"metadata\""));
            assertTrue(json, json.endsWith("\"lucene_bound_exceeded\":false}}}}}"));
        }
    }

    public void testRequestRoundTripKeepsNodeIds() throws Exception {
        LanceStatsRequest original = new LanceStatsRequest("a", "b");
        LanceStatsRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceStatsRequest(in);
            }
        }
        assertArrayEquals(new String[] { "a", "b" }, restored.nodesIds());
        assertNull(restored.validate());
    }

    public void testCollectorWithoutACacheReportsZeroCacheFigures() {
        LanceNodeStats stats = new LanceStatsCollector(null, () -> 42L, () -> null).collect();
        assertFalse(stats.cacheEnabled());
        assertEquals(0, stats.snapshotCount());
        assertEquals(0L, stats.columnStoreBytes());
        assertEquals(0L, stats.columnStoreLimitBytes());
        assertEquals(42L, stats.sessionBytes());
        assertEquals(0L, stats.indexCacheCapacityBytes());
        assertEquals(0, stats.indexCacheShards());
        assertEquals(0L, stats.indexCacheShardShareBytes());
        assertEquals(LanceFtsQuery.subsetProbeLimit(), stats.ftsSubsetProbeLimit());
        assertEquals("none", stats.warmUpMode());
        assertTrue(stats.warmUps().isEmpty());
    }

    public void testCollectorReadsTheWarmCache() throws Exception {
        String uri = LanceTableFactory.writeHintFixtureTable(createTempDir(), "stats-" + getTestName(), 2, 50);
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache cache = new LanceWarmCache(allocator, 1024L * 1024, 8, true)
        ) {
            IndexCacheSizing sizing = NativeMemoryLimit.sizeIndexCache(19L << 30, 16);
            LanceStatsCollector collector = new LanceStatsCollector(cache, () -> 0L, () -> sizing);
            LanceNodeStats empty = collector.collect();
            assertTrue(empty.cacheEnabled());
            assertEquals(0, empty.snapshotCount());
            assertEquals(0L, empty.snapshotBuildCount());
            assertEquals(1024L * 1024, empty.columnStoreLimitBytes());
            assertEquals((16L << 30) - 1, empty.indexCacheCapacityBytes());
            assertEquals(2, empty.indexCacheShards());
            assertEquals((8L << 30) - 1, empty.indexCacheShardShareBytes());

            try (
                LanceWarmCache.Lease lease = cache.acquire(
                    "uuid",
                    uri,
                    StorageOptions.empty(),
                    Optional.empty(),
                    "",
                    LancePrimaryKeyType.NONE,
                    Collections.emptyMap()
                )
            ) {
                LanceNodeStats held = collector.collect();
                assertEquals(1, held.snapshotCount());
                assertEquals(0, held.retiredSnapshotCount());
                assertEquals(1L, held.snapshotBuildCount());
                assertEquals(1L, held.datasetOpenCount());
                assertEquals(0L, held.snapshotHitCount());

                cache.retire("uuid", lease.snapshot().version() + 1);
                assertEquals("a retired snapshot a lease still holds is counted", 1, collector.collect().retiredSnapshotCount());
            }
            LanceNodeStats released = collector.collect();
            assertEquals("the retired snapshot closed with its last lease", 0, released.snapshotCount());
            assertEquals(0, released.retiredSnapshotCount());
        }
    }
}
