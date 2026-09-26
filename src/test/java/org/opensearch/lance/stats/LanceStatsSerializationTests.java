/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.RootAllocator;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.ScanAdmission;
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
            admissionRejections(7L, 1L),
            832L,
            "fts",
            "warm_up",
            6_442_450_944L,
            268_435_456L,
            "fts:s3://bucket/perf.lance:body",
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
                new LanceNodeStats.IndexReaderStats(
                    "big",
                    3_000_000_000L,
                    2_000_000_000L,
                    0L,
                    true,
                    Map.of(),
                    List.of(new LanceMappingMeta.RenamedField("ts", "event_ts", 1))
                ),
                new LanceNodeStats.IndexReaderStats("small", 120L, 120L, 14L, false, Map.of("rating", List.of("BTree", "Bitmap")))
            ),
            List.of(new LanceNodeStats.LocalCloneStats("cloned", 4321L, 9L)),
            5,
            123L,
            2,
            4L,
            planRefinements(2L, 0L, 1L),
            planExecuted(7L, 4L),
            9L,
            new LanceNodeStats.FreshnessStats(
                2,
                40L,
                3L,
                1L,
                2L,
                1L,
                0L,
                1_700_000_000_000L,
                Map.of("perf", "Mapper for [body] conflicts with existing mapper")
            )
        ).withRequestCache(new LanceNodeStats.RequestCacheStats(true, 2048L, 1_048_576L, 3, 11L, 4L, 1L, 2L, 6L));
    }

    /** The admission rejections in the gate's key order, one per kind. */
    private static Map<String, Long> admissionRejections(long fts, long filterScan) {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("fts", fts);
        counts.put("scalar_index", 0L);
        counts.put("vector_index", 0L);
        counts.put("filter_scan", filterScan);
        counts.put("aggregate_scan", 0L);
        counts.put("column_load", 0L);
        return counts;
    }

    /** The executed counters in the collector's key order. */
    private static Map<String, Long> planExecuted(long pushedScan, long lucene) {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("pushed_scan", pushedScan);
        counts.put("lucene", lucene);
        return counts;
    }

    /** The refinement counters in the collector's key order, one per reason. */
    private static Map<String, Long> planRefinements(long securityWrapper, long sortFieldType, long aggregateResolution) {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("security_wrapper", securityWrapper);
        counts.put("sort_field_type", sortFieldType);
        counts.put("aggregate_resolution", aggregateResolution);
        return counts;
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

    public void testFreshnessStatsTakeANullMapAsNoRefusedUpdates() {
        LanceNodeStats.FreshnessStats stats = new LanceNodeStats.FreshnessStats(1, 2L, 3L, 4L, 5L, 6L, 7L, 8L, null);
        assertEquals(Map.of(), stats.mappingErrors());
        assertEquals(stats, new LanceNodeStats.FreshnessStats(1, 2L, 3L, 4L, 5L, 6L, 7L, 8L));
        assertEquals(Map.of(), stats.withMappingErrors(null).mappingErrors());
    }

    public void testNodeStatsXContentShape() throws Exception {
        ScanAdmission.setEnabled(true);
        ScanAdmission.setHeadroom(ScanAdmission.DEFAULT_HEADROOM);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            sample().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals(
                "{\"snapshots\":{\"enabled\":true,\"count\":3,\"retired\":1,\"dataset_open_count\":7,\"snapshot_build_count\":4,"
                    + "\"snapshot_hit_count\":12},"
                    + "\"column_store\":{\"bytes\":4096,\"limit_bytes\":65536,\"entries\":9,\"hits\":30,\"loads\":5,\"evictions\":2,"
                    + "\"budget_misses\":1,\"heap_fallback_bytes\":2048,\"heap_fallback_rejections\":3},"
                    + "\"request_cache\":{\"enabled\":true,\"size_bytes\":2048,\"limit_bytes\":1048576,\"entries\":3,\"hits\":11,"
                    + "\"misses\":4,\"evictions\":1,\"invalidations\":2,\"skipped\":6},"
                    + "\"native_memory\":{\"estimated_bytes\":5000,\"session_bytes\":900,\"column_store_bytes\":4096,"
                    + "\"index_cache_capacity\":17179869183,\"index_cache_shards\":2,\"index_cache_shard_share\":8589934591},"
                    + "\"fts\":{\"subset_probe_limit\":1000000},"
                    + "\"admission\":{\"enabled\":true,\"headroom_bytes\":8589934592,\"available_bytes\":6442450944,"
                    + "\"retained_bytes\":268435456,\"retained_scope\":\"fts:s3://bucket/perf.lance:body\","
                    + "\"last_estimate_bytes\":832,\"last_kind\":\"fts\",\"last_source\":\"warm_up\","
                    + "\"rejections\":{\"fts\":7,\"scalar_index\":0,\"vector_index\":0,\"filter_scan\":1,\"aggregate_scan\":0,"
                    + "\"column_load\":0}},"
                    + "\"warm_up\":{\"mode\":\"metadata\",\"tables\":[{\"index\":\"perf\",\"table\":\"s3://bucket/perf.lance\","
                    + "\"version\":8,\"mode\":\"metadata\",\"state\":\"done\",\"started_at\":\"2023-11-14T22:13:20Z\",\"seconds\":3.46,"
                    + "\"indexes\":[{\"name\":\"rating_idx\",\"type\":\"BTree\",\"column\":\"rating\",\"state\":\"done\",\"seconds\":0.4},"
                    + "{\"name\":\"body_idx\",\"type\":\"Inverted\",\"column\":\"body\",\"state\":\"failed\",\"seconds\":1.25,"
                    + "\"detail\":\"boom\"}]}]},"
                    + "\"plan\":{\"statistics\":{\"tables\":5,\"collect_millis_total\":123,\"pending\":2,\"planned_without\":4},"
                    + "\"refinements\":{\"security_wrapper\":2,\"sort_field_type\":0,\"aggregate_resolution\":1},"
                    + "\"executed\":{\"pushed_scan\":7,\"lucene\":4},\"pruned\":{\"fragments\":9}},"
                    + "\"freshness\":{\"tracked\":2,\"checks\":40,\"moves\":3,\"mapping_updates\":1,\"mapping_unchanged\":2,"
                    + "\"rebuilds\":1,\"failures\":0,\"last_check_millis\":1700000000000,"
                    + "\"mapping_errors\":{\"perf\":\"Mapper for [body] conflicts with existing mapper\"}},"
                    + "\"indices\":{\"big\":{\"rows\":3000000000,\"shard_reader_rows\":2000000000,\"nested_docs\":0,"
                    + "\"lucene_bound_exceeded\":true,\"index_types\":{},"
                    + "\"renamed_fields\":[{\"from\":\"ts\",\"to\":\"event_ts\",\"lance_field_id\":1}]},"
                    + "\"small\":{\"rows\":120,\"shard_reader_rows\":120,\"nested_docs\":14,\"lucene_bound_exceeded\":false,"
                    + "\"index_types\":{\"rating\":[\"BTree\",\"Bitmap\"]}}},"
                    + "\"local_clones\":{\"cloned\":{\"local_clone_bytes\":4321,\"source_version\":9}}}",
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
            assertTrue(json, json.contains("\"fts\":{\"subset_probe_limit\":1000000},\"admission\":{\"enabled\":"));
            assertTrue(
                json,
                json.contains(
                    "\"rejections\":{\"fts\":7,\"scalar_index\":0,\"vector_index\":0,\"filter_scan\":1,\"aggregate_scan\":0,"
                        + "\"column_load\":0}},\"warm_up\":{\"mode\":\"metadata\""
                )
            );
            assertTrue(json, json.endsWith("\"local_clones\":{\"cloned\":{\"local_clone_bytes\":4321,\"source_version\":9}}}}}"));
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

    public void testNodeStatsStreamOpensWithTheWireVersionAndCarriesTheLaterFieldsAsBlocks() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceNodeStats.WIRE_VERSION, in.readVInt());
            }
        }
        // The stream a version 8 data node would return: today's fields
        // and one optional block this coordinator steps over.
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            sample(),
            WireVersionTestSupport.NO_PRELUDE,
            false,
            o -> o.writeVLong(77L)
        );
        try (StreamInput in = newer.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("warm_up", restored.admissionLastSource());
            assertEquals(2, restored.planStatisticsPending());
            assertEquals(4L, restored.planStatisticsPlannedWithout());
            assertEquals("fts:s3://bucket/perf.lance:body", restored.admissionRetainedScope());
            assertEquals(sample().freshness(), restored.freshness());
            assertEquals(sample().requestCache(), restored.requestCache());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }

    public void testMixedPluginVersionAVersion1CoordinatorReadsTodaysStatsWithoutTheLaterFields() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion1 = LanceNodeStats.read(in, 1);
                assertEquals("the counter the older coordinator does not know falls back to zero", 0L, asVersion1.planPrunedFragments());
                assertEquals("the source the older coordinator does not know falls back to none", "none", asVersion1.admissionLastSource());
                assertEquals("the progress counters it does not know fall back to zero", 0, asVersion1.planStatisticsPending());
                assertEquals(0L, asVersion1.planStatisticsPlannedWithout());
                assertEquals("the retained scope it does not know falls back to none", "none", asVersion1.admissionRetainedScope());
                assertEquals(
                    "the result cache it does not know reads as none",
                    LanceNodeStats.RequestCacheStats.NONE,
                    asVersion1.requestCache()
                );
                assertEquals(sample().planExecuted(), asVersion1.planExecuted());
                assertEquals(
                    "the freshness counters are base fields; the refused updates are not",
                    sample().freshness().withMappingErrors(Map.of()),
                    asVersion1.freshness()
                );
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion2CoordinatorReadsTodaysStatsWithoutTheSourceAndTheMappingErrors() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion2 = LanceNodeStats.read(in, 2);
                assertEquals("the block version 2 knows is read", 9L, asVersion2.planPrunedFragments());
                assertEquals("the block it does not know falls back", "none", asVersion2.admissionLastSource());
                assertEquals("fts", asVersion2.admissionLastKind());
                assertEquals(sample().freshness().withMappingErrors(Map.of()), asVersion2.freshness());
                assertEquals(0, asVersion2.planStatisticsPending());
                assertEquals(0L, asVersion2.planStatisticsPlannedWithout());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion3CoordinatorReadsTodaysStatsWithoutTheMappingErrors() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion3 = LanceNodeStats.read(in, 3);
                assertEquals(9L, asVersion3.planPrunedFragments());
                assertEquals("the block version 3 knows is read", "warm_up", asVersion3.admissionLastSource());
                assertEquals(
                    "the block it does not know is stepped over",
                    sample().freshness().withMappingErrors(Map.of()),
                    asVersion3.freshness()
                );
                assertTrue(asVersion3.freshness().mappingErrors().isEmpty());
                assertEquals(0, asVersion3.planStatisticsPending());
                assertEquals(0L, asVersion3.planStatisticsPlannedWithout());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion4CoordinatorReadsTodaysStatsWithoutTheProgressCounters() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion4 = LanceNodeStats.read(in, 4);
                assertEquals(9L, asVersion4.planPrunedFragments());
                assertEquals("warm_up", asVersion4.admissionLastSource());
                assertEquals("the blocks version 4 knows are read", sample().freshness(), asVersion4.freshness());
                assertEquals("the block it does not know is stepped over", 0, asVersion4.planStatisticsPending());
                assertEquals(0L, asVersion4.planStatisticsPlannedWithout());
                assertEquals("none", asVersion4.admissionRetainedScope());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion5CoordinatorReadsTodaysStatsWithoutTheRetainedScope() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion5 = LanceNodeStats.read(in, 5);
                assertEquals(9L, asVersion5.planPrunedFragments());
                assertEquals("warm_up", asVersion5.admissionLastSource());
                assertEquals(sample().freshness(), asVersion5.freshness());
                assertEquals("the blocks version 5 knows are read", 2, asVersion5.planStatisticsPending());
                assertEquals(4L, asVersion5.planStatisticsPlannedWithout());
                assertEquals("the retained bytes are a base field", 268_435_456L, asVersion5.admissionRetainedBytes());
                assertEquals("the block it does not know is stepped over", "none", asVersion5.admissionRetainedScope());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion6CoordinatorReadsTodaysStatsWithoutTheRequestCache() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sample().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNodeStats asVersion6 = LanceNodeStats.read(in, 6);
                assertEquals("the blocks version 6 knows are read", "fts:s3://bucket/perf.lance:body", asVersion6.admissionRetainedScope());
                assertEquals(2, asVersion6.planStatisticsPending());
                assertEquals(sample().freshness(), asVersion6.freshness());
                assertEquals(
                    "the block it does not know is stepped over",
                    LanceNodeStats.RequestCacheStats.NONE,
                    asVersion6.requestCache()
                );
                assertFalse(asVersion6.requestCache().enabled());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    /**
     * The bytes of today's stream as a data node at wire version
     * {@code marker} writes them: the base fields and the blocks up to
     * that version, the later blocks cut off.
     */
    private static BytesReference asWrittenByVersion(LanceNodeStats stats, int marker) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput(); BytesStreamOutput today = new BytesStreamOutput()) {
            stats.writeTo(today);
            // The blocks are the last thing written, each a flag, a
            // length prefix and the bytes of that length; their sizes are
            // measured by writing them again on their own. Version 2 is
            // the pruned counter, 3 the admission source, 4 the mapping
            // errors, 5 the statistics progress counters, 6 the retained
            // pool's scope, 7 the result cache, in that order.
            int trailing = 0;
            if (marker < 7) {
                trailing += blockSize(stats.requestCache());
            }
            if (marker < 6) {
                trailing += blockSize(o -> o.writeString(stats.admissionRetainedScope()));
            }
            if (marker < 5) {
                trailing += blockSize(o -> {
                    o.writeVInt(stats.planStatisticsPending());
                    o.writeVLong(stats.planStatisticsPlannedWithout());
                });
            }
            if (marker < 4) {
                trailing += blockSize(
                    o -> o.writeMap(stats.freshness().mappingErrors(), StreamOutput::writeString, StreamOutput::writeString)
                );
            }
            if (marker < 3) {
                trailing += blockSize(o -> o.writeString(stats.admissionLastSource()));
            }
            if (marker < 2) {
                trailing += blockSize(o -> o.writeVLong(stats.planPrunedFragments()));
            }
            try (StreamInput in = today.bytes().streamInput()) {
                assertEquals(LanceNodeStats.WIRE_VERSION, in.readVInt());
                byte[] rest = in.readAllBytes();
                assertTrue(rest.length > trailing);
                out.writeVInt(marker);
                out.writeBytes(rest, 0, rest.length - trailing);
            }
            return out.bytes();
        }
    }

    private static int blockSize(Writeable fields) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.writeBlock(out, false, fields);
            return out.bytes().length();
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion1NodesStats() throws Exception {
        // The stream a version 1 data node writes: today's base fields
        // and no block at all.
        BytesReference version1 = asWrittenByVersion(sample(), 1);
        try (StreamInput in = version1.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(0L, restored.planPrunedFragments());
            assertEquals("none", restored.admissionLastSource());
            assertEquals(0, restored.planStatisticsPending());
            assertEquals(0L, restored.planStatisticsPlannedWithout());
            assertEquals(sample().planExecuted(), restored.planExecuted());
            assertEquals(sample().freshness().withMappingErrors(Map.of()), restored.freshness());
            assertEquals(-1, in.read());
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion2NodesStats() throws Exception {
        // The stream a version 2 data node writes: the pruned counter
        // block only.
        BytesReference version2 = asWrittenByVersion(sample(), 2);
        try (StreamInput in = version2.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("none", restored.admissionLastSource());
            assertEquals(sample().freshness().withMappingErrors(Map.of()), restored.freshness());
            assertEquals(0, restored.planStatisticsPending());
            assertEquals(0L, restored.planStatisticsPlannedWithout());
            assertEquals(-1, in.read());
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion3NodesStats() throws Exception {
        // The stream a version 3 data node writes: the pruned counter and
        // the admission source blocks, no mapping error block.
        BytesReference version3 = asWrittenByVersion(sample(), 3);
        try (StreamInput in = version3.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("warm_up", restored.admissionLastSource());
            assertEquals("the mapping errors fall back to empty", sample().freshness().withMappingErrors(Map.of()), restored.freshness());
            assertEquals(0, restored.planStatisticsPending());
            assertEquals(0L, restored.planStatisticsPlannedWithout());
            assertEquals(-1, in.read());
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion4NodesStats() throws Exception {
        // The stream a version 4 data node writes: the pruned counter, the
        // admission source and the mapping error blocks, no progress block.
        BytesReference version4 = asWrittenByVersion(sample(), 4);
        try (StreamInput in = version4.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("warm_up", restored.admissionLastSource());
            assertEquals(sample().freshness(), restored.freshness());
            assertEquals("the progress counters fall back to zero", 0, restored.planStatisticsPending());
            assertEquals(0L, restored.planStatisticsPlannedWithout());
            assertEquals("none", restored.admissionRetainedScope());
            assertEquals(-1, in.read());
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion5NodesStats() throws Exception {
        // The stream a version 5 data node writes: every block up to the
        // progress counters, no retained scope block.
        BytesReference version5 = asWrittenByVersion(sample(), 5);
        try (StreamInput in = version5.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("warm_up", restored.admissionLastSource());
            assertEquals(sample().freshness(), restored.freshness());
            assertEquals(2, restored.planStatisticsPending());
            assertEquals(4L, restored.planStatisticsPlannedWithout());
            assertEquals(268_435_456L, restored.admissionRetainedBytes());
            assertEquals("the retained scope falls back to none", "none", restored.admissionRetainedScope());
            assertEquals(LanceNodeStats.RequestCacheStats.NONE, restored.requestCache());
            assertEquals(-1, in.read());
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion6NodesStats() throws Exception {
        // The stream a version 6 data node writes: every block up to the
        // retained scope, no result cache block.
        BytesReference version6 = asWrittenByVersion(sample(), 6);
        try (StreamInput in = version6.streamInput()) {
            LanceNodeStats restored = new LanceNodeStats(in);
            assertEquals(9L, restored.planPrunedFragments());
            assertEquals("warm_up", restored.admissionLastSource());
            assertEquals(sample().freshness(), restored.freshness());
            assertEquals(2, restored.planStatisticsPending());
            assertEquals("fts:s3://bucket/perf.lance:body", restored.admissionRetainedScope());
            assertEquals("the result cache falls back to none", LanceNodeStats.RequestCacheStats.NONE, restored.requestCache());
            assertEquals(-1, in.read());
        }
    }

    public void testNodeRequestStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        // The per node request carries only the marker, after the parent
        // task id its TransportRequest base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new LanceStatsNodeRequest().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                assertEquals(LanceStatsNodeRequest.WIRE_VERSION, in.readVInt());
                assertEquals("nothing follows the marker", -1, in.read());
            }
        }
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            new LanceStatsNodeRequest(),
            o -> TaskId.EMPTY_TASK_ID.writeTo(o),
            false,
            o -> o.writeBoolean(true)
        );
        try (StreamInput in = newer.streamInput()) {
            new LanceStatsNodeRequest(in);
            assertEquals("the reader consumed the block", -1, in.read());
        }
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
        assertTrue("a live host reports available memory: " + stats.admissionAvailableBytes(), stats.admissionAvailableBytes() > 0L);
        assertEquals("nothing was admitted, so nothing is retained", 0L, stats.admissionRetainedBytes());
        assertEquals(
            "every kind is reported, zero when it never refused",
            List.of("fts", "scalar_index", "vector_index", "filter_scan", "aggregate_scan", "column_load"),
            List.copyOf(stats.admissionRejections().keySet())
        );
        assertEquals("none", stats.warmUpMode());
        assertTrue(stats.warmUps().isEmpty());
        assertEquals(0, stats.planStatisticsTables());
        assertEquals(0L, stats.planStatisticsCollectMillisTotal());
        assertEquals(
            "every refinement reason is reported, zero when it never fired",
            List.of("security_wrapper", "sort_field_type", "aggregate_resolution", "column_store_warm"),
            List.copyOf(stats.planRefinements().keySet())
        );
        assertEquals(List.of("pushed_scan", "lucene"), List.copyOf(stats.planExecuted().keySet()));
        assertEquals("no cache, no figures", LanceNodeStats.RequestCacheStats.NONE, stats.requestCache());
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
                    LanceOverrides.EMPTY
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

    public void testCollectorReadsThePlanStatisticsCache() throws Exception {
        String uri = LanceTableFactory.writeHintFixtureTable(createTempDir(), "plan-stats-" + getTestName(), 2, 50);
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache cache = new LanceWarmCache(allocator, 1024L * 1024, 8, true)
        ) {
            LanceStatsCollector collector = new LanceStatsCollector(cache, () -> 0L, () -> null);
            LanceNodeStats empty = collector.collect();
            assertEquals(0, empty.planStatisticsTables());
            assertEquals(0L, empty.planStatisticsCollectMillisTotal());
            assertEquals(0, empty.planStatisticsPending());
            assertEquals(0L, empty.planStatisticsPlannedWithout());
            try (
                LanceWarmCache.Lease lease = cache.acquire(
                    "uuid",
                    uri,
                    StorageOptions.empty(),
                    Optional.empty(),
                    "",
                    LancePrimaryKeyType.NONE,
                    LanceOverrides.EMPTY
                )
            ) {
                // Building the snapshot collected the statistics of its
                // version (on this thread: the test cache has no pool).
                LanceNodeStats held = collector.collect();
                assertEquals(1, held.planStatisticsTables());
                assertTrue(
                    "one collection counts at least one millisecond: " + held.planStatisticsCollectMillisTotal(),
                    held.planStatisticsCollectMillisTotal() >= 1L
                );
                assertEquals("nothing is pending once the collection ran", 0, held.planStatisticsPending());
                assertEquals("a prefetch is not a plan without statistics", 0L, held.planStatisticsPlannedWithout());
                String tableUri = lease.snapshot().dataset().uri();
                long version = lease.snapshot().version();
                assertNotNull(
                    cache.tableStatistics()
                        .lookup(tableUri, version, () -> LanceRegistry.openDataset(uri, StorageOptions.empty(), Optional.of(version)))
                );
                assertEquals(
                    "a cache hit collects nothing",
                    held.planStatisticsCollectMillisTotal(),
                    collector.collect().planStatisticsCollectMillisTotal()
                );
                // A lookup of a version the cache does not hold is a plan
                // without statistics, counted as such (the collection it
                // starts fails here: there is no such version to open).
                assertNull(
                    cache.tableStatistics().lookup(tableUri, version + 1, () -> { throw new IllegalStateException("no such version"); })
                );
                assertEquals(1L, collector.collect().planStatisticsPlannedWithout());
                assertEquals(0, collector.collect().planStatisticsPending());
                cache.retire("uuid", version + 1);
            }
            assertEquals("the entry went with its snapshot", 0, collector.collect().planStatisticsTables());
        }
    }
}
