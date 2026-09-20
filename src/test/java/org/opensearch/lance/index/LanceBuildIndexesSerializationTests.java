/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the build_indexes transport
 * classes, so the REST layer and the transport action can evolve
 * independently without breaking the wire shape.
 */
public class LanceBuildIndexesSerializationTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws Exception {
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest(
            "demo",
            List.of("body", "id"),
            List.of("body"),
            List.of(0, 2),
            false,
            false,
            "lindera/ipadic"
        );

        LanceBuildIndexesRequest restored = roundTrip(original);

        assertEquals("demo", restored.index());
        assertArrayEquals(new String[] { "demo" }, restored.indices());
        assertEquals(List.of("body", "id"), restored.columns());
        assertEquals(List.of("body"), restored.ftsColumns());
        assertEquals(List.of(0, 2), restored.fragmentIds());
        assertFalse(restored.optimize());
        assertFalse(restored.retrain());
        assertEquals("lindera/ipadic", restored.tokenizer());
        assertNull(restored.validate());
    }

    public void testRequestWithoutFtsOptionsRoundTripsAsNull() throws Exception {
        // Absent fts_columns and tokenizer stay null on the wire so the
        // transport action applies LanceIndexBuilder.DEFAULT_FTS_TOKENIZER
        // itself and derives the FTS targets from the table.
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, false, false, null);

        LanceBuildIndexesRequest restored = roundTrip(original);

        assertNull(restored.ftsColumns());
        assertNull(restored.tokenizer());
        assertNull(restored.validate());
    }

    public void testOptimizeRequestWithNullFiltersRoundTrip() throws Exception {
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, true, true, null);

        LanceBuildIndexesRequest restored = roundTrip(original);

        assertNull(restored.columns());
        assertNull(restored.ftsColumns());
        assertNull(restored.fragmentIds());
        assertTrue(restored.optimize());
        assertTrue(restored.retrain());
        assertNull(restored.tokenizer());
        assertNull(restored.validate());
    }

    public void testRequestValidation() {
        assertNotNull(new LanceBuildIndexesRequest("", null, null, null, false, false, null).validate());
        // fragment_ids only scopes an initial build; optimize covers every
        // uncovered fragment on its own.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, List.of(1), true, false, null).validate());
        // retrain is an optimize option.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, true, null).validate());
        // fts_columns creates indexes; optimize creates none.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, true, false, null).validate());
        // An empty fts_columns list is a caller mistake.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of(), null, false, false, null).validate());
        // tokenizer only applies to indexes this request creates, which
        // are the fts_columns ones.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, false, "simple").validate());
        // An empty tokenizer is a caller mistake, not a request for the
        // default.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "").validate());
        // Any non-empty name passes plugin validation; Lance decides
        // whether it exists.
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "no-such-tokenizer").validate());
        // fts_columns without a tokenizer builds with the default.
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, null).validate());
    }

    public void testResponseRoundTrip() throws Exception {
        LanceBuildIndexesResponse original = new LanceBuildIndexesResponse(
            "demo",
            List.of("body"),
            List.of("id", "category"),
            List.of(),
            List.of("body", "id", "category"),
            List.of(0, 2)
        );

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertEquals("demo", restored.index());
        assertEquals(List.of("body"), restored.ftsBuilt());
        assertEquals(List.of("id", "category"), restored.scalarBuilt());
        assertTrue(restored.vectorBuilt().isEmpty());
        assertEquals(List.of("body", "id", "category"), restored.columnsFilter());
        assertEquals(List.of(0, 2), restored.fragmentIds());
    }

    public void testResponseWithoutFiltersRoundTrip() throws Exception {
        LanceBuildIndexesResponse original = new LanceBuildIndexesResponse("demo", List.of(), List.of("id"), List.of("vec"), null, null);

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertNull(restored.columnsFilter());
        assertNull(restored.fragmentIds());
        assertEquals(List.of("vec"), restored.vectorBuilt());
    }

    private static LanceBuildIndexesRequest roundTrip(LanceBuildIndexesRequest original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new LanceBuildIndexesRequest(in);
            }
        }
    }

    private static LanceBuildIndexesResponse roundTrip(LanceBuildIndexesResponse original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new LanceBuildIndexesResponse(in);
            }
        }
    }
}
