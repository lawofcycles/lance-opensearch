/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchModule;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.QueryRescoreMode;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the fragment-dispatch transport
 * classes. Guarantee that a request or response byte-for-byte
 * survives the wire so coordinator and node handlers can be
 * developed independently without breaking each other.
 */
public class LanceFragmentQuerySerializationTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry AGG_REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, java.util.Collections.emptyList()).getNamedWriteables()
    );

    public void testRequestRoundTrip() throws Exception {
        StorageOptions storage = StorageOptions.of(java.util.Map.of("region", "us-east-1"));
        // Aggregations flow across the wire as an
        // AggregatorFactories.Builder — the raw shape from
        // SearchSourceBuilder.aggregations(). Build one with two
        // metric aggregations so the round-trip exercises the
        // native OpenSearch serialisation.
        org.opensearch.search.aggregations.AggregatorFactories.Builder aggs =
            new org.opensearch.search.aggregations.AggregatorFactories.Builder().addAggregator(
                new org.opensearch.search.aggregations.metrics.SumAggregationBuilder("s").field("id")
            ).addAggregator(new org.opensearch.search.aggregations.metrics.MinAggregationBuilder("m").field("id"));
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "s3://bucket/tables/demo.lance",
            "demo",
            storage,
            /* pinnedVersion */ 7L,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_AGGREGATE, "id >= 2"),
            /* query */ null,
            /* postFilter */ null,
            /* sorts */ Collections.emptyList(),
            /* searchAfter */ null,
            5,
            aggs,
            List.of(0, 2, 4),
            /* trackScores */ true,
            /* trackTotalHitsUpTo */ 3
        );

        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, AGG_REGISTRY)
            ) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }

        assertEquals(original.tableUri(), restored.tableUri());
        assertEquals(original.indexName(), restored.indexName());
        assertEquals(original.plan(), restored.plan());
        assertEquals("id >= 2", restored.plan().filterSql());
        assertEquals(original.size(), restored.size());
        assertNotNull(restored.aggregations());
        assertEquals(2, restored.aggregations().getAggregatorFactories().size());
        assertEquals(original.fragmentIds(), restored.fragmentIds());
        assertEquals(original.storageOptions().asMap(), restored.storageOptions().asMap());
        assertEquals(7L, restored.pinnedVersion());
        assertEquals(Optional.of(7L), restored.pinnedVersionOrEmpty());
        assertEquals(original.trackScores(), restored.trackScores());
        assertEquals(3, restored.trackTotalHitsUpTo());
    }

    public void testRequestTrackTotalHitsSentinelsRoundTrip() throws Exception {
        // The two sentinels sit at the extremes of the int range
        // (-1 and Integer.MAX_VALUE); a variable-length encoding would
        // mangle the negative one, so the field is a plain int.
        for (int upTo : new int[] { SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE, 10_000 }) {
            LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
                "/tmp/table.lance",
                "demo",
                StorageOptions.empty(),
                /* pinnedVersion */ -1L,
                FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, null),
                /* query */ null,
                /* postFilter */ null,
                Collections.emptyList(),
                /* searchAfter */ null,
                0,
                /* aggregations */ null,
                Collections.emptyList(),
                /* trackScores */ false,
                upTo
            );
            LanceFragmentQueryRequest restored;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    restored = new LanceFragmentQueryRequest(in);
                }
            }
            assertEquals(upTo, restored.trackTotalHitsUpTo());
        }
    }

    public void testRequestWithNullFilterAndAllFragmentsRoundTrip() throws Exception {
        StorageOptions storage = StorageOptions.empty();
        LanceFragmentQueryRequest original = LanceFragmentQueryRequest.allFragments(
            "/tmp/table.lance",
            "demo",
            storage,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            /* query */ null,
            /* sorts */ Collections.emptyList(),
            10,
            /* aggregations */ null
        );

        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }

        assertEquals(original.indexName(), restored.indexName());
        assertEquals("allFragments follows the latest manifest", -1L, restored.pinnedVersion());
        assertTrue("-1 must expose as an empty pin", restored.pinnedVersionOrEmpty().isEmpty());
        assertNull(restored.plan().filterSql());
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, restored.plan().kind());
        assertTrue("empty fragmentIds is the all-fragments sentinel", restored.fragmentIds().isEmpty());
        assertNull("empty list must expose as null through the SDK helper", restored.fragmentIdsOrNull());
        assertEquals(original.size(), restored.size());
        assertNull(restored.aggregations());
        assertEquals("allFragments asks for an exact count", SearchContext.TRACK_TOTAL_HITS_ACCURATE, restored.trackTotalHitsUpTo());
    }

    public void testRequestKnobsAndProjectionRoundTrip() throws Exception {
        // min_score, terminate_after and the per hit projections travel
        // as the stock Writeable classes the coordinator copies from the
        // search body.
        HitProjection projection = new HitProjection(
            new FetchSourceContext(true, new String[] { "id", "ti*" }, new String[] { "body" }),
            StoredFieldsContext.fromList(List.of("id", "_source")),
            List.of(new FieldAndFormat("ts", "yyyy-MM-dd"), new FieldAndFormat("id", null)),
            List.of(new FieldAndFormat("ti*", null)),
            true
        );
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "/tmp/table.lance",
            "demo",
            StorageOptions.empty(),
            /* pinnedVersion */ -1L,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            /* query */ null,
            /* postFilter */ null,
            Collections.emptyList(),
            /* searchAfter */ null,
            5,
            /* aggregations */ null,
            Collections.emptyList(),
            /* trackScores */ false,
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            /* minScore */ 1.5f,
            /* terminateAfter */ 7,
            projection
        );
        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }
        assertEquals(1.5f, restored.minScore(), 0f);
        assertEquals(7, restored.terminateAfter());
        assertTrue(restored.hasCollectorKnobs());
        assertEquals(projection, restored.projection());
        assertTrue(restored.projection().fetchSource().fetchSource());
        assertArrayEquals(new String[] { "id", "ti*" }, restored.projection().fetchSource().includes());
        assertEquals(List.of("id", "_source"), restored.projection().storedFields().fieldNames());
        assertEquals("yyyy-MM-dd", restored.projection().docValueFields().get(0).format);
        assertEquals("ti*", restored.projection().fetchFields().get(0).field);
        assertTrue(restored.projection().explain());
    }

    public void testRequestWithoutKnobsOrProjectionRoundTrip() throws Exception {
        // The shorter constructor is a body without these elements:
        // no min_score, no terminate_after, the empty projection, and
        // nothing about them travels beyond the absent markers.
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "/tmp/table.lance",
            "demo",
            StorageOptions.empty(),
            -1L,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, null),
            null,
            null,
            Collections.emptyList(),
            null,
            0,
            null,
            Collections.emptyList(),
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }
        assertNull(restored.minScore());
        assertEquals(0, restored.terminateAfter());
        assertFalse(restored.hasCollectorKnobs());
        assertEquals(HitProjection.NONE, restored.projection());
        assertTrue(restored.projection().isEmpty());
    }

    public void testRequestRescoreAndCollapseRoundTrip() throws Exception {
        // The rescorers travel as named writeables (the search module
        // registers the query rescorer) and the collapse as the stock
        // Writeable, inner hits and the concurrency bound included.
        QueryRescorerBuilder first = new QueryRescorerBuilder(new TermQueryBuilder("category", "c1")).windowSize(25)
            .setQueryWeight(0.7f)
            .setRescoreQueryWeight(1.2f)
            .setScoreMode(QueryRescoreMode.Max);
        QueryRescorerBuilder second = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        CollapseBuilder collapse = new CollapseBuilder("category").setInnerHits(
            new InnerHitBuilder("top").setSize(2).addSort(new FieldSortBuilder("id").order(SortOrder.DESC))
        ).setMaxConcurrentGroupRequests(3);
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "/tmp/table.lance",
            "demo",
            StorageOptions.empty(),
            /* pinnedVersion */ -1L,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            /* query */ null,
            /* postFilter */ null,
            Collections.emptyList(),
            /* searchAfter */ null,
            5,
            /* aggregations */ null,
            Collections.emptyList(),
            /* trackScores */ false,
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            /* minScore */ null,
            /* terminateAfter */ 0,
            HitProjection.NONE,
            List.of(first, second),
            collapse
        );
        assertEquals("the first pass collects the largest window", 25, original.firstPassSize());
        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, AGG_REGISTRY)
            ) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }
        assertEquals(List.of(first, second), restored.rescores());
        assertEquals(Integer.valueOf(25), restored.rescores().get(0).windowSize());
        assertNull("a rescorer without a window keeps none on the wire", restored.rescores().get(1).windowSize());
        assertEquals(collapse, restored.collapse());
        assertEquals("category", restored.collapse().getField());
        assertEquals(1, restored.collapse().getInnerHits().size());
        assertEquals("top", restored.collapse().getInnerHits().get(0).getName());
        assertEquals(3, restored.collapse().getMaxConcurrentGroupRequests());
        assertEquals(25, restored.firstPassSize());
    }

    public void testRequestWithoutRescoreOrCollapseRoundTrip() throws Exception {
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "/tmp/table.lance",
            "demo",
            StorageOptions.empty(),
            -1L,
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
            null,
            null,
            Collections.emptyList(),
            null,
            5,
            null,
            Collections.emptyList(),
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }
        assertTrue(restored.rescores().isEmpty());
        assertNull(restored.collapse());
        assertEquals("without rescorers the first pass is the page", 5, restored.firstPassSize());
    }

    public void testResponseTerminatedEarlyRoundTrip() throws Exception {
        // terminated_early is absent (null) without terminate_after and
        // a boolean with it; all three states survive the wire.
        for (Boolean terminatedEarly : new Boolean[] { null, Boolean.FALSE, Boolean.TRUE }) {
            LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
                2L,
                false,
                1,
                List.of(),
                new long[0],
                null,
                terminatedEarly
            );
            LanceFragmentQueryResponse restored;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    restored = new LanceFragmentQueryResponse(in);
                }
            }
            assertEquals(terminatedEarly, restored.terminatedEarly());
        }
        assertNull(new LanceFragmentQueryResponse(0L, false, 1, List.of(), new long[0], null).terminatedEarly());
    }

    public void testResponseRoundTrip() throws Exception {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        hit.score(1.0f);
        hit.sourceRef(new BytesArray("{\"id\":3}"));

        // A real InternalSum so the round-trip exercises the aggregator's
        // own StreamInput / StreamOutput path rather than an empty
        // container.
        org.opensearch.search.aggregations.InternalAggregations aggregations = org.opensearch.search.aggregations.InternalAggregations.from(
            List.<org.opensearch.search.aggregations.InternalAggregation>of(
                new org.opensearch.search.aggregations.metrics.InternalSum(
                    "s",
                    3.0d,
                    org.opensearch.search.DocValueFormat.RAW,
                    java.util.Map.of()
                )
            )
        );

        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            1L,
            /* matchedIsLowerBound */ false,
            1,
            List.of(hit),
            new long[] { 3L },
            aggregations
        );

        LanceFragmentQueryResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (
                StreamInput raw = out.bytes().streamInput();
                NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, AGG_REGISTRY)
            ) {
                restored = new LanceFragmentQueryResponse(in);
            }
        }

        assertEquals(original.matched(), restored.matched());
        assertFalse(restored.matchedIsLowerBound());
        assertEquals(original.fragmentCount(), restored.fragmentCount());
        assertEquals(1, restored.hits().size());
        assertEquals("0-3", restored.hits().get(0).getId());
        assertEquals("{\"id\":3}", restored.hits().get(0).getSourceAsString());
        assertArrayEquals(new long[] { 3L }, restored.rowAddrs());
        assertNotNull(restored.aggregations());
        org.opensearch.search.aggregations.metrics.InternalSum restoredSum =
            (org.opensearch.search.aggregations.metrics.InternalSum) restored.aggregations().get("s");
        assertEquals(3.0d, restoredSum.getValue(), 0.0d);
    }

    public void testResponseRoundTripWithNoAggregations() throws Exception {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        hit.score(1.0f);

        // matched 10001 with the lower-bound flag is what an executor
        // reports after stopping a count at track_total_hits 10000 + 1.
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            10_001L,
            /* matchedIsLowerBound */ true,
            1,
            List.of(hit),
            new long[] { 3L },
            null
        );

        LanceFragmentQueryResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryResponse(in);
            }
        }

        assertEquals(original.matched(), restored.matched());
        assertTrue(restored.matchedIsLowerBound());
        assertEquals(1, restored.hits().size());
        assertArrayEquals(new long[] { 3L }, restored.rowAddrs());
        assertNull(restored.aggregations());
    }

    public void testResponseRejectsRowAddressCountMismatch() {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LanceFragmentQueryResponse(1L, false, 1, List.of(hit), new long[0], null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("0 entries for 1 hits"));
    }

    public void testRequestStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        LanceFragmentQueryRequest original = LanceFragmentQueryRequest.allFragments(
            "/tmp/table.lance",
            "demo",
            StorageOptions.empty(),
            FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, null),
            /* query */ null,
            Collections.emptyList(),
            0,
            /* aggregations */ null
        );
        // The marker is the first field after the parent task id the
        // ActionRequest base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                assertEquals(LanceFragmentQueryRequest.WIRE_VERSION, in.readVInt());
            }
        }
        // The stream a version 2 coordinator would write: today's fields
        // followed by one optional block this version does not know. The
        // request is read whole and the block is stepped over.
        Writeable prelude = o -> TaskId.EMPTY_TASK_ID.writeTo(o);
        BytesReference optional = WireVersionTestSupport.asNextVersion(original, prelude, false, o -> o.writeString("a version 2 hint"));
        try (StreamInput in = new NamedWriteableAwareStreamInput(optional.streamInput(), AGG_REGISTRY)) {
            LanceFragmentQueryRequest restored = new LanceFragmentQueryRequest(in);
            assertEquals(original.tableUri(), restored.tableUri());
            assertEquals(original.plan(), restored.plan());
            assertEquals("the reader consumed the block", -1, in.read());
        }
        // A critical block of a version this node does not know is
        // refused by name, before the request is executed.
        BytesReference critical = WireVersionTestSupport.asNextVersion(
            original,
            prelude,
            true,
            o -> o.writeString("a version 2 constraint")
        );
        try (StreamInput in = new NamedWriteableAwareStreamInput(critical.streamInput(), AGG_REGISTRY)) {
            IOException refused = expectThrows(IOException.class, () -> new LanceFragmentQueryRequest(in));
            assertEquals(
                "LanceFragmentQueryRequest wire version [2] adds fields in version [2] that this node's [1] cannot ignore: "
                    + "upgrade this node before sending it this message",
                refused.getMessage()
            );
        }
    }

    public void testResponseProfileRoundTrip() throws Exception {
        LanceFragmentQueryResponse.Profile profile = new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 8L);
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(2L, false, 1, List.of(), new long[0], null, null, profile);
        LanceFragmentQueryResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryResponse(in);
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
        assertEquals(profile, restored.profile());
        assertEquals(
            "a response built without timings reports none",
            LanceFragmentQueryResponse.Profile.NONE,
            new LanceFragmentQueryResponse(2L, false, 1, List.of(), new long[0], null).profile()
        );
        assertEquals(
            new LanceFragmentQueryResponse.Profile(13L, 5L, 5L, 50L, 10L, 10L),
            profile.plus(new LanceFragmentQueryResponse.Profile(1L, 2L, 1L, 3L, 1L, 2L))
        );
    }

    public void testMixedPluginVersionAVersion2CoordinatorReadsTodaysResponseWithoutTheTakeColumns() throws Exception {
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            2L,
            false,
            1,
            List.of(),
            new long[0],
            null,
            Boolean.TRUE,
            new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 8L)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceFragmentQueryResponse asVersion2 = LanceFragmentQueryResponse.read(in, 2);
                assertEquals(2L, asVersion2.matched());
                assertEquals(
                    "the timings block is read, the columns block it does not know is stepped over",
                    new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 0L),
                    asVersion2.profile()
                );
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion2NodesResponse() throws Exception {
        // The stream a version 2 data node writes: today's base fields
        // and the timings block, no columns block.
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            2L,
            false,
            1,
            List.of(),
            new long[0],
            null,
            null,
            new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 8L)
        );
        int columnsBlockBytes;
        try (BytesStreamOutput block = new BytesStreamOutput()) {
            WireVersion.writeBlock(block, false, o -> o.writeVLong(original.profile().takeColumns()));
            columnsBlockBytes = block.bytes().length();
        }
        try (BytesStreamOutput today = new BytesStreamOutput(); BytesStreamOutput version2 = new BytesStreamOutput()) {
            original.writeTo(today);
            try (StreamInput in = today.bytes().streamInput()) {
                assertEquals(LanceFragmentQueryResponse.WIRE_VERSION, in.readVInt());
                byte[] rest = in.readAllBytes();
                version2.writeVInt(2);
                version2.writeBytes(rest, 0, rest.length - columnsBlockBytes);
            }
            try (StreamInput in = version2.bytes().streamInput()) {
                LanceFragmentQueryResponse restored = new LanceFragmentQueryResponse(in);
                assertEquals(2L, restored.matched());
                assertEquals(
                    "the timings are read, the columns fall back to zero",
                    new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 0L),
                    restored.profile()
                );
                assertEquals(-1, in.read());
            }
        }
    }

    public void testMixedPluginVersionAVersion1CoordinatorReadsTodaysResponseWithoutTheProfile() throws Exception {
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            2L,
            false,
            1,
            List.of(),
            new long[0],
            null,
            Boolean.TRUE,
            new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 8L)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceFragmentQueryResponse asVersion1 = LanceFragmentQueryResponse.read(in, 1);
                assertEquals(2L, asVersion1.matched());
                assertEquals(Boolean.TRUE, asVersion1.terminatedEarly());
                assertEquals("the blocks it does not know are stepped over", LanceFragmentQueryResponse.Profile.NONE, asVersion1.profile());
                assertEquals("the reader consumed the blocks", -1, in.read());
            }
        }
    }

    public void testMixedPluginVersionTodaysCoordinatorReadsAVersion1NodesResponse() throws Exception {
        // The stream a version 1 data node writes: today's base fields
        // and no block at all.
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            2L,
            false,
            1,
            List.of(),
            new long[0],
            null,
            null,
            new LanceFragmentQueryResponse.Profile(12L, 3L, 4L, 47L, 9L, 8L)
        );
        int blockBytes;
        try (BytesStreamOutput blocks = new BytesStreamOutput()) {
            WireVersion.writeBlock(blocks, false, original.profile());
            WireVersion.writeBlock(blocks, false, o -> o.writeVLong(original.profile().takeColumns()));
            blockBytes = blocks.bytes().length();
        }
        try (BytesStreamOutput today = new BytesStreamOutput(); BytesStreamOutput version1 = new BytesStreamOutput()) {
            original.writeTo(today);
            try (StreamInput in = today.bytes().streamInput()) {
                assertEquals(LanceFragmentQueryResponse.WIRE_VERSION, in.readVInt());
                byte[] rest = in.readAllBytes();
                version1.writeVInt(1);
                version1.writeBytes(rest, 0, rest.length - blockBytes);
            }
            try (StreamInput in = version1.bytes().streamInput()) {
                LanceFragmentQueryResponse restored = new LanceFragmentQueryResponse(in);
                assertEquals(2L, restored.matched());
                assertEquals("the profile falls back to zero", LanceFragmentQueryResponse.Profile.NONE, restored.profile());
                assertEquals(-1, in.read());
            }
        }
    }

    public void testResponseStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(2L, false, 1, List.of(), new long[0], null);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceFragmentQueryResponse.WIRE_VERSION, in.readVInt());
            }
        }
        // The stream a version 4 data node would write back to a version
        // 3 coordinator: today's fields and one more optional block.
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            original,
            WireVersionTestSupport.NO_PRELUDE,
            false,
            o -> o.writeVLong(11L)
        );
        try (StreamInput in = new NamedWriteableAwareStreamInput(newer.streamInput(), AGG_REGISTRY)) {
            LanceFragmentQueryResponse restored = new LanceFragmentQueryResponse(in);
            assertEquals(2L, restored.matched());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }
}
