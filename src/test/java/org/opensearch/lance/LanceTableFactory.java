/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;

/**
 * Test-only helper that writes a small Lance table onto the local
 * filesystem for use by {@link LancePluginIT}. Mirrors the shape a
 * production table would have (int PK + Utf8 body + fixed-size vector
 * embedding) so the IT drives the same code paths a real deployment
 * would.
 *
 * <p>Uses the IPC-stream pattern from {@code LanceScannerFullTextSearchTest}
 * (serialize the batch with {@link ArrowStreamWriter}, hand the reader to
 * {@link Dataset#create(org.apache.arrow.memory.BufferAllocator,
 * org.apache.arrow.c.ArrowArrayStream, String, WriteParams)}). Passing a
 * schema-only path through {@code createWithFfiSchema} refused
 * {@code FixedSizeList} on Lance 11.0.0 with
 * "The FixedSizeList type requires an integer parameter representing number
 * of elements per list", so the IPC path is the reliable route.
 *
 * <p>Not part of the plugin runtime; kept under {@code src/test/java}
 * and only referenced from tests.
 */
final class LanceTableFactory {

    static final String VECTOR_COLUMN = "embedding";
    static final int VECTOR_DIM = 8;
    static final String BODY_COLUMN = "body";
    static final String PRIMARY_KEY = "id";

    private LanceTableFactory() {}

    /**
     * Writes a Lance table under {@code parent/name}. Rows are deterministic:
     * <ul>
     *   <li>{@code id = i}</li>
     *   <li>{@code body = "hello lance " + i} for even {@code i},
     *       {@code "quick brown fox " + i} for odd {@code i}</li>
     *   <li>{@code embedding[0] = i}, other coordinates 0</li>
     * </ul>
     * After writing, an INVERTED index is created on {@code body} so
     * the plugin's {@link RestAttachAction#derive} maps it to
     * {@code lance_text}. Without the index the derivation falls back
     * to {@code keyword} and match queries lose their analyzer step.
     *
     * @return absolute URI of the table (usable as-is for
     *         {@code /_lance/attach} or namespace register).
     */
    static String writeTable(Path parent, String name, int rowCount) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                new Field(PRIMARY_KEY, FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(BODY_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null),
                new Field(
                    VECTOR_COLUMN,
                    FieldType.nullable(new ArrowType.FixedSizeList(VECTOR_DIM)),
                    Collections.singletonList(
                        new Field("item", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)
                    )
                )
            ),
            Map.of()
        );

        // Dedicated allocator per invocation so the JNI-owned buffers can
        // be released without touching the plugin's shared registry
        // allocator.
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes = writeIpcBatch(allocator, schema, rowCount);

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                try (Dataset dataset = Dataset.create(allocator, stream, uri, writeParams)) {
                    // Build the INVERTED index on body so derive() maps it
                    // to lance_text. Match the parameters
                    // LanceScannerFullTextSearchTest uses upstream.
                    ScalarIndexParams scalarParams = ScalarIndexParams.create(
                        "inverted",
                        "{\"base_tokenizer\":\"simple\",\"language\":\"English\",\"with_position\":true}"
                    );
                    IndexParams indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build();
                    dataset.createIndex(
                        IndexOptions.builder(Collections.singletonList(BODY_COLUMN), IndexType.INVERTED, indexParams)
                            .withIndexName(BODY_COLUMN + "_fts")
                            .build()
                    );
                }
            }
        }
        return uri;
    }

    private static byte[] writeIpcBatch(RootAllocator allocator, Schema schema, int rowCount) throws Exception {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IntVector idVector = (IntVector) root.getVector(PRIMARY_KEY);
            VarCharVector bodyVector = (VarCharVector) root.getVector(BODY_COLUMN);
            FixedSizeListVector vecVector = (FixedSizeListVector) root.getVector(VECTOR_COLUMN);
            Float4Vector vecItems = (Float4Vector) vecVector.getDataVector();

            idVector.allocateNew(rowCount);
            bodyVector.allocateNew();
            vecVector.allocateNew();
            vecItems.allocateNew(rowCount * VECTOR_DIM);

            for (int i = 0; i < rowCount; i++) {
                idVector.set(i, i);
                String body = (i % 2 == 0 ? "hello lance " : "quick brown fox ") + i;
                bodyVector.setSafe(i, body.getBytes(StandardCharsets.UTF_8));
                for (int j = 0; j < VECTOR_DIM; j++) {
                    // Row i lives at coordinate (i, 0, 0, ...). Distances
                    // between two rows become |i - k| so nearest-neighbour
                    // ordering is deterministic and easy to assert.
                    float value = (j == 0) ? (float) i : 0.0f;
                    vecItems.set(i * VECTOR_DIM + j, value);
                }
                vecVector.setNotNull(i);
            }

            idVector.setValueCount(rowCount);
            bodyVector.setValueCount(rowCount);
            vecItems.setValueCount(rowCount * VECTOR_DIM);
            vecVector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
    }
}
