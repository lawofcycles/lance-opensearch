/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lance.StorageOptions;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchModule;
import org.opensearch.search.internal.SearchContext;
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
            "id >= 2",
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
        assertEquals(original.filterSql(), restored.filterSql());
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
                /* filterSql */ null,
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
            /* filterSql */ null,
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
        assertNull(restored.filterSql());
        assertTrue("empty fragmentIds is the all-fragments sentinel", restored.fragmentIds().isEmpty());
        assertNull("empty list must expose as null through the SDK helper", restored.fragmentIdsOrNull());
        assertEquals(original.size(), restored.size());
        assertNull(restored.aggregations());
        assertEquals("allFragments asks for an exact count", SearchContext.TRACK_TOTAL_HITS_ACCURATE, restored.trackTotalHitsUpTo());
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
}
