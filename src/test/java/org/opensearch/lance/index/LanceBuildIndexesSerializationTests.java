/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.lance.index.LanceBuildIndexesResponse.ColumnResult;
import org.opensearch.lance.index.LanceBuildIndexesResponse.KindResult;
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
            "lindera/ipadic",
            true
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
        assertTrue(restored.withPosition());
        assertNull(restored.validate());
    }

    public void testRequestWithoutFtsOptionsRoundTripsAsNull() throws Exception {
        // Absent fts_columns and tokenizer stay null on the wire so the
        // transport action applies LanceIndexBuilder.DEFAULT_FTS_TOKENIZER
        // itself and derives the FTS targets from the table; with_position
        // defaults to false like Lance's own default.
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, false);

        LanceBuildIndexesRequest restored = roundTrip(original);

        assertNull(restored.ftsColumns());
        assertNull(restored.tokenizer());
        assertFalse(restored.withPosition());
        assertNull(restored.validate());
    }

    public void testOptimizeRequestWithNullFiltersRoundTrip() throws Exception {
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, true, true, null, false);

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
        assertNotNull(new LanceBuildIndexesRequest("", null, null, null, false, false, null, false).validate());
        // fragment_ids only scopes an initial build; optimize covers every
        // uncovered fragment on its own.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, List.of(1), true, false, null, false).validate());
        // retrain is an optimize option.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, true, null, false).validate());
        // fts_columns creates indexes; optimize creates none.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, true, false, null, false).validate());
        // An empty fts_columns list is a caller mistake.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of(), null, false, false, null, false).validate());
        // tokenizer only applies to indexes this request creates, which
        // are the fts_columns ones.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, false, "simple", false).validate());
        // An empty tokenizer is a caller mistake, not a request for the
        // default.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "", false).validate());
        // Any non-empty name passes plugin validation; Lance decides
        // whether it exists.
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "no-such-tokenizer", false).validate());
        // fts_columns without a tokenizer builds with the default.
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, null, false).validate());
        // with_position, like tokenizer, only shapes the indexes this
        // request creates.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, true).validate());
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, null, true).validate());
    }

    public void testResponseRoundTrip() throws Exception {
        LanceBuildIndexesResponse original = new LanceBuildIndexesResponse(
            "demo",
            new KindResult(List.of("body"), List.of(), List.of()),
            new KindResult(
                List.of("id", "category"),
                List.of(new ColumnResult("rating", "scalar index already exists; use optimize=true to extend it over new fragments")),
                List.of(new ColumnResult("price", "LanceError(IO): Permission denied (os error 13)"))
            ),
            new KindResult(
                List.of(),
                List.of(new ColumnResult("embedding", "table has 5 rows, below the IVF_PQ training minimum of 256")),
                List.of()
            ),
            List.of("body", "id", "category", "rating", "price", "embedding"),
            List.of(0, 2),
            RestStatus.INTERNAL_SERVER_ERROR
        );

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertEquals("demo", restored.index());
        assertEquals(List.of("body"), restored.ftsBuilt());
        assertEquals(List.of("id", "category"), restored.scalarBuilt());
        assertTrue(restored.vectorBuilt().isEmpty());
        assertEquals(original.scalar().skipped(), restored.scalar().skipped());
        assertEquals(original.scalar().failed(), restored.scalar().failed());
        assertEquals(original.vector().skipped(), restored.vector().skipped());
        assertTrue(restored.fts().skipped().isEmpty());
        assertTrue(restored.fts().failed().isEmpty());
        assertTrue(restored.vector().failed().isEmpty());
        assertTrue(restored.hasFailures());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, restored.status());
        assertEquals(List.of("body", "id", "category", "rating", "price", "embedding"), restored.columnsFilter());
        assertEquals(List.of(0, 2), restored.fragmentIds());
    }

    public void testResponseWithoutFiltersRoundTrip() throws Exception {
        LanceBuildIndexesResponse original = new LanceBuildIndexesResponse(
            "demo",
            new KindResult(List.of(), List.of(), List.of()),
            new KindResult(List.of("id"), List.of(), List.of()),
            new KindResult(List.of("vec"), List.of(), List.of()),
            null,
            null,
            RestStatus.OK
        );

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertNull(restored.columnsFilter());
        assertNull(restored.fragmentIds());
        assertEquals(List.of("vec"), restored.vectorBuilt());
        assertFalse(restored.hasFailures());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testResponseXContentListsBuiltSkippedAndFailedPerKind() throws Exception {
        LanceBuildIndexesResponse response = new LanceBuildIndexesResponse(
            "demo",
            new KindResult(List.of(), List.of(), List.of(new ColumnResult("text", "unknown base tokenizer no-such-tokenizer"))),
            new KindResult(List.of("id"), List.of(new ColumnResult("category", "already")), List.of()),
            new KindResult(List.of(), List.of(), List.of()),
            null,
            null,
            RestStatus.BAD_REQUEST
        );

        String json = Strings.toString(MediaTypeRegistry.JSON, response);

        // The pre-existing built shape is unchanged; skipped and failed
        // sit next to it with one {column, reason} object per column.
        assertTrue(json, json.contains("\"built\":{\"fts\":[],\"scalar\":[\"id\"],\"vector\":[]}"));
        assertTrue(
            json,
            json.contains("\"skipped\":{\"fts\":[],\"scalar\":[{\"column\":\"category\",\"reason\":\"already\"}],\"vector\":[]}")
        );
        assertTrue(
            json,
            json.contains(
                "\"failed\":{\"fts\":[{\"column\":\"text\",\"reason\":\"unknown base tokenizer no-such-tokenizer\"}],\"scalar\":[],\"vector\":[]}"
            )
        );
        assertFalse("status is transport metadata, not body", json.contains("status"));
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
