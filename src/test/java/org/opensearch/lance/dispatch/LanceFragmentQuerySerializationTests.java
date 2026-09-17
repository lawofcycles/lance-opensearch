/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricType;
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the Milestone 5-C2 transport
 * classes. Guarantee that a request or response byte-for-byte
 * survives the wire so coordinator and node handlers can be
 * developed independently without breaking each other.
 */
public class LanceFragmentQuerySerializationTests extends OpenSearchTestCase {

    public void testMetricSpecRoundTrip() throws Exception {
        MetricSpec original = new MetricSpec("total", MetricType.SUM, "amount");
        MetricSpec restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new MetricSpec(in);
            }
        }
        assertEquals(original, restored);
    }

    public void testPartialStateRoundTrip() throws Exception {
        PartialState original = new PartialState(3L, 7.5d, -1.0d, 4.0d);
        PartialState restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new PartialState(in);
            }
        }
        assertEquals(original, restored);
    }

    public void testEmptyPartialStateRoundTrip() throws Exception {
        // PartialState.EMPTY carries +/- Infinity sentinels that
        // Double serialisation must preserve exactly for min / max
        // to remain neutral through the merge.
        PartialState restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            PartialState.EMPTY.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new PartialState(in);
            }
        }
        assertEquals(PartialState.EMPTY, restored);
    }

    public void testRequestRoundTrip() throws Exception {
        StorageOptions storage = StorageOptions.of(java.util.Map.of("region", "us-east-1"));
        LanceFragmentQueryRequest original = new LanceFragmentQueryRequest(
            "s3://bucket/tables/demo.lance",
            "demo",
            storage,
            "id >= 2",
            5,
            List.of(new MetricSpec("s", MetricType.SUM, "id"), new MetricSpec("m", MetricType.MIN, "id")),
            List.of(0, 2, 4)
        );

        LanceFragmentQueryRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryRequest(in);
            }
        }

        assertEquals(original.tableUri(), restored.tableUri());
        assertEquals(original.indexName(), restored.indexName());
        assertEquals(original.filterSql(), restored.filterSql());
        assertEquals(original.size(), restored.size());
        assertEquals(original.metrics(), restored.metrics());
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
            10,
            Collections.emptyList()
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
        assertEquals(original.metrics(), restored.metrics());
    }

    public void testResponseRoundTrip() throws Exception {
        SearchHit hit = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        hit.score(1.0f);
        hit.sourceRef(new BytesArray("{\"id\":3}"));

        LanceFragmentQueryResponse original = new LanceFragmentQueryResponse(
            1L,
            1,
            List.of(hit),
            List.of(new PartialState(1L, 3.0d, 3.0d, 3.0d))
        );

        LanceFragmentQueryResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceFragmentQueryResponse(in);
            }
        }

        assertEquals(original.matched(), restored.matched());
        assertEquals(original.fragmentCount(), restored.fragmentCount());
        assertEquals(1, restored.hits().size());
        assertEquals("0-3", restored.hits().get(0).getId());
        assertEquals("{\"id\":3}", restored.hits().get(0).getSourceAsString());
        assertEquals(original.partials(), restored.partials());
    }
}
