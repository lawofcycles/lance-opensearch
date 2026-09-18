/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lance.StorageOptions;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchModule;
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
            new org.opensearch.search.aggregations.AggregatorFactories.Builder()
                .addAggregator(new org.opensearch.search.aggregations.metrics.SumAggregationBuilder("s").field("id"))
                .addAggregator(new org.opensearch.search.aggregations.metrics.MinAggregationBuilder("m").field("id"));
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "s3://bucket/tables/demo.lance",
            "demo",
            storage,
            "id >= 2",
            /* query */ null,
            /* sorts */ Collections.emptyList(),
            5,
            aggs,
            List.of(0, 2, 4)
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
        assertNull(restored.filterSql());
        assertTrue("empty fragmentIds is the all-fragments sentinel", restored.fragmentIds().isEmpty());
        assertNull("empty list must expose as null through the SDK helper", restored.fragmentIdsOrNull());
        assertEquals(original.size(), restored.size());
        assertNull(restored.aggregations());
    }

    public void testResponseRoundTrip() throws Exception {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        hit.score(1.0f);
        hit.sourceRef(new BytesArray("{\"id\":3}"));

        // Wire format now carries InternalAggregations (Direction 1
        // Stage 2). Build a real InternalSum so the round-trip
        // exercises the aggregator's own StreamInput/StreamOutput
        // code path rather than an empty container: an empty
        // InternalAggregations would not catch bugs in
        // per-aggregation serialisation.
        org.opensearch.search.aggregations.InternalAggregations aggregations =
            org.opensearch.search.aggregations.InternalAggregations.from(
                List.<org.opensearch.search.aggregations.InternalAggregation>of(
                    new org.opensearch.search.aggregations.metrics.InternalSum("s", 3.0d,
                        org.opensearch.search.DocValueFormat.RAW, java.util.Map.of())
                )
            );

        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            1L,
            1,
            List.of(hit),
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
        assertEquals(original.fragmentCount(), restored.fragmentCount());
        assertEquals(1, restored.hits().size());
        assertEquals("0-3", restored.hits().get(0).getId());
        assertEquals("{\"id\":3}", restored.hits().get(0).getSourceAsString());
        assertNotNull(restored.aggregations());
        org.opensearch.search.aggregations.metrics.InternalSum restoredSum =
            (org.opensearch.search.aggregations.metrics.InternalSum) restored.aggregations().get("s");
        assertEquals(3.0d, restoredSum.getValue(), 0.0d);
    }

    public void testResponseRoundTripWithNoAggregations() throws Exception {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        hit.score(1.0f);

        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(1L, 1, List.of(hit), null);

        LanceFragmentQueryResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryResponse(in);
            }
        }

        assertEquals(original.matched(), restored.matched());
        assertEquals(1, restored.hits().size());
        assertNull(restored.aggregations());
    }
}
