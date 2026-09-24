/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.util.Collections;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.lance.index.LanceBuildIndexesResponse.BuiltResult;
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
            true,
            "{\"rating\":{\"scalar\":\"bitmap\"}}"
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
        assertEquals("{\"rating\":{\"scalar\":\"bitmap\"}}", restored.indexesJson());
        assertNull(restored.validate());
    }

    public void testRequestWithoutFtsOptionsRoundTripsAsNull() throws Exception {
        // Absent fts_columns and tokenizer stay null on the wire so the
        // transport action applies LanceIndexBuilder.DEFAULT_FTS_TOKENIZER
        // itself and derives the FTS targets from the table; with_position
        // defaults to false like Lance's own default.
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, false, null);

        LanceBuildIndexesRequest restored = roundTrip(original);

        assertNull(restored.ftsColumns());
        assertNull(restored.tokenizer());
        assertFalse(restored.withPosition());
        assertNull(restored.indexesJson());
        assertNull(restored.validate());
    }

    public void testOptimizeRequestWithNullFiltersRoundTrip() throws Exception {
        LanceBuildIndexesRequest original = new LanceBuildIndexesRequest("demo", null, null, null, true, true, null, false, null);

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
        assertNotNull(new LanceBuildIndexesRequest("", null, null, null, false, false, null, false, null).validate());
        // fragment_ids only scopes an initial build; optimize covers every
        // uncovered fragment on its own.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, List.of(1), true, false, null, false, null).validate());
        // retrain is an optimize option.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, true, null, false, null).validate());
        // fts_columns creates indexes; optimize creates none.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, true, false, null, false, null).validate());
        // An empty fts_columns list is a caller mistake.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of(), null, false, false, null, false, null).validate());
        // tokenizer only applies to indexes this request creates, which
        // are the fts_columns ones.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, false, "simple", false, null).validate());
        // An empty tokenizer is a caller mistake, not a request for the
        // default.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "", false, null).validate());
        // Any non-empty name passes plugin validation; Lance decides
        // whether it exists.
        assertNull(
            new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, "no-such-tokenizer", false, null).validate()
        );
        // fts_columns without a tokenizer builds with the default.
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, null, false, null).validate());
        // with_position, like tokenizer, only shapes the indexes this
        // request creates.
        assertNotNull(new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, true, null).validate());
        assertNull(new LanceBuildIndexesRequest("demo", null, List.of("body"), null, false, false, null, true, null).validate());
        // The one-shot indexes preference steers index creation; optimize
        // creates none.
        assertNotNull(
            new LanceBuildIndexesRequest("demo", null, null, null, true, false, null, false, "{\"rating\":{\"scalar\":\"bitmap\"}}")
                .validate()
        );
        assertNull(
            new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, false, "{\"rating\":{\"scalar\":\"bitmap\"}}")
                .validate()
        );
    }

    public void testResponseRoundTrip() throws Exception {
        LanceBuildIndexesResponse original = new LanceBuildIndexesResponse(
            "demo",
            new KindResult(List.of(new BuiltResult("body", "INVERTED")), List.of(), List.of()),
            new KindResult(
                List.of(new BuiltResult("id", "BTREE"), new BuiltResult("category", "BITMAP")),
                List.of(new ColumnResult("rating", "scalar index already exists; use optimize=true to extend it over new fragments")),
                List.of(new ColumnResult("price", "LanceError(IO): Permission denied (os error 13)"))
            ),
            new KindResult(
                List.of(),
                List.of(new ColumnResult("embedding", "table has 5 rows, below the ivf_pq training minimum of 256")),
                List.of()
            ),
            List.of("body", "id", "category", "rating", "price", "embedding"),
            List.of(0, 2),
            RestStatus.INTERNAL_SERVER_ERROR
        );

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertEquals("demo", restored.index());
        assertEquals(List.of(new BuiltResult("body", "INVERTED")), restored.ftsBuilt());
        assertEquals(List.of(new BuiltResult("id", "BTREE"), new BuiltResult("category", "BITMAP")), restored.scalarBuilt());
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
            new KindResult(List.of(new BuiltResult("id", "BTREE")), List.of(), List.of()),
            new KindResult(List.of(new BuiltResult("vec", "IVF_PQ")), List.of(), List.of()),
            null,
            null,
            RestStatus.OK
        );

        LanceBuildIndexesResponse restored = roundTrip(original);

        assertNull(restored.columnsFilter());
        assertNull(restored.fragmentIds());
        assertEquals(List.of(new BuiltResult("vec", "IVF_PQ")), restored.vectorBuilt());
        assertFalse(restored.hasFailures());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testResponseXContentListsBuiltSkippedAndFailedPerKind() throws Exception {
        LanceBuildIndexesResponse response = new LanceBuildIndexesResponse(
            "demo",
            new KindResult(List.of(), List.of(), List.of(new ColumnResult("text", "unknown base tokenizer no-such-tokenizer"))),
            new KindResult(List.of(new BuiltResult("id", "BTREE")), List.of(new ColumnResult("category", "already")), List.of()),
            new KindResult(List.of(), List.of(), List.of()),
            null,
            null,
            RestStatus.BAD_REQUEST
        );

        String json = Strings.toString(MediaTypeRegistry.JSON, response);

        // Built entries carry the column and the index type that was
        // built; skipped and failed sit next to them with one
        // {column, reason} object per column.
        assertTrue(json, json.contains("\"built\":{\"fts\":[],\"scalar\":[{\"column\":\"id\",\"type\":\"BTREE\"}],\"vector\":[]}"));
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

    public void testNodeRequestStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        LanceBuildIndexesRequest build = new LanceBuildIndexesRequest("demo", null, null, null, false, false, null, false, null);
        LanceBuildIndexesNodeRequest original = new LanceBuildIndexesNodeRequest(build, 9L);
        // The marker follows the parent task id the TransportRequest
        // base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                assertEquals(LanceBuildIndexesNodeRequest.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceBuildIndexesNodeRequest restored = new LanceBuildIndexesNodeRequest(in);
                assertEquals("demo", restored.request().index());
                assertEquals(9L, restored.sourceVersion());
            }
        }
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            original,
            o -> TaskId.EMPTY_TASK_ID.writeTo(o),
            false,
            o -> o.writeBoolean(true)
        );
        try (StreamInput in = newer.streamInput()) {
            LanceBuildIndexesNodeRequest restored = new LanceBuildIndexesNodeRequest(in);
            assertEquals(9L, restored.sourceVersion());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }

    public void testNodeResponseStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        DiscoveryNode node = new DiscoveryNode(
            "node-1",
            "node-1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        KindResult empty = new KindResult(List.of(), List.of(), List.of());
        LanceBuildIndexesNodeResponse original = new LanceBuildIndexesNodeResponse(node, empty, empty, empty, RestStatus.OK, null);
        // The marker follows the node the BaseNodeResponse base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                new DiscoveryNode(in);
                assertEquals(LanceBuildIndexesNodeResponse.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceBuildIndexesNodeResponse restored = new LanceBuildIndexesNodeResponse(in);
                assertEquals("node-1", restored.getNode().getId());
                assertEquals(RestStatus.OK, restored.status());
                assertNull(restored.mappingJson());
            }
        }
        BytesReference newer = WireVersionTestSupport.asNextVersion(original, node::writeToWithAttribute, false, o -> o.writeVLong(3L));
        try (StreamInput in = newer.streamInput()) {
            LanceBuildIndexesNodeResponse restored = new LanceBuildIndexesNodeResponse(in);
            assertEquals(RestStatus.OK, restored.status());
            assertEquals("the reader consumed the block", -1, in.read());
        }
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
