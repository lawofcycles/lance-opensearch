/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
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
    static final String TITLE_COLUMN = "title";
    static final String PRIMARY_KEY = "id";

    private LanceTableFactory() {}

    /**
     * Writes a Lance table under {@code parent/name}. Rows are deterministic:
     * <ul>
     *   <li>{@code id = i}</li>
     *   <li>{@code body = "hello lance " + i} for even {@code i},
     *       {@code "quick brown fox " + i} for odd {@code i}</li>
     *   <li>{@code title = "sunny morning " + i} for even {@code i},
     *       {@code "cloudy morning " + i} for odd {@code i}. Chosen so
     *       {@code body} and {@code title} share no tokens except the row
     *       index, letting multi_match tests distinguish which field a hit
     *       came from</li>
     *   <li>{@code embedding[0] = i}, other coordinates 0</li>
     * </ul>
     * After writing, INVERTED indexes are created on {@code body} and
     * {@code title} so the plugin's {@link RestAttachAction#derive}
     * maps them to {@code lance_text}. Without the index the derivation
     * falls back to {@code keyword} and match queries lose their analyzer
     * step.
     *
     * @return absolute URI of the table (usable as-is for
     *         {@code /_lance/attach} or namespace register).
     */
    static String writeTable(Path parent, String name, int rowCount) throws Exception {
        return withLocaleRoot(() -> writeTableOnce(parent, name, rowCount));
    }

    private static String writeTableOnce(Path parent, String name, int rowCount) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                // Note: the Lance schema does not declare
                // `lance-schema:unenforced-primary-key`. Adding that metadata
                // via `new FieldType(true, ArrowType.Int, null, meta)` breaks
                // the C Data serialisation of the FixedSizeList column that
                // follows (see the corresponding failure signature in the
                // review notes). Without a declared PK the derived
                // `primary_key_field` is empty and GET /_doc returns 404,
                // which B10 verifies through testAttachOfPkLessTableDisablesGet.
                new Field(PRIMARY_KEY, FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(BODY_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null),
                new Field(TITLE_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null),
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
                    // Build INVERTED indexes on body and title so derive()
                    // maps them to lance_text. Match the parameters
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
                    dataset.createIndex(
                        IndexOptions.builder(Collections.singletonList(TITLE_COLUMN), IndexType.INVERTED, indexParams)
                            .withIndexName(TITLE_COLUMN + "_fts")
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
            VarCharVector titleVector = (VarCharVector) root.getVector(TITLE_COLUMN);
            FixedSizeListVector vecVector = (FixedSizeListVector) root.getVector(VECTOR_COLUMN);
            Float4Vector vecItems = (Float4Vector) vecVector.getDataVector();

            idVector.allocateNew(rowCount);
            bodyVector.allocateNew();
            titleVector.allocateNew();
            vecVector.allocateNew();
            vecItems.allocateNew(rowCount * VECTOR_DIM);

            for (int i = 0; i < rowCount; i++) {
                idVector.set(i, i);
                String body = (i % 2 == 0 ? "hello lance " : "quick brown fox ") + i;
                bodyVector.setSafe(i, body.getBytes(StandardCharsets.UTF_8));
                String title = (i % 2 == 0 ? "sunny morning " : "cloudy morning ") + i;
                titleVector.setSafe(i, title.getBytes(StandardCharsets.UTF_8));
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
            titleVector.setValueCount(rowCount);
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

    /**
     * Writes a Lance table exercising nullable numeric columns. Used by
     * B4 regression tests to confirm the reader stops crashing on Arrow
     * nulls, int8 / int16 / int64 all surface with the correct OpenSearch
     * mapping type, and boolean nulls no longer collapse to false.
     *
     * <p>Row layout ({@code rowCount = 12}):
     * <ul>
     *   <li>{@code id}: int32 primary key, {@code i}</li>
     *   <li>{@code count8}: int8 nullable, {@code (byte) i}</li>
     *   <li>{@code count16}: int16 nullable, {@code (short) (i * 100)}</li>
     *   <li>{@code count64}: int64 nullable, {@code 4_000_000_000L + i}
     *       (exercises the >&nbsp;Integer.MAX_VALUE range)</li>
     *   <li>{@code flag}: bool nullable, true when {@code i % 3 == 0}</li>
     * </ul>
     * Rows with {@code i == 5} set every nullable column to Arrow null.
     */
    static String writeNullableTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeNullableTableOnce(parent, name));
    }

    private static String writeNullableTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                // Same rationale as writeTable: PK metadata cannot be
                // combined with the FixedSizeList carried over from the
                // other IT fixture without breaking C Data serialisation,
                // and the nullable-int IT does not exercise GET anyway.
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("count8", FieldType.nullable(new ArrowType.Int(8, true)), null),
                new Field("count16", FieldType.nullable(new ArrowType.Int(16, true)), null),
                new Field("count64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("flag", FieldType.nullable(new ArrowType.Bool()), null)
            ),
            Map.of()
        );

        int rowCount = 12;
        int nullRow = 5;

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                TinyIntVector count8 = (TinyIntVector) root.getVector("count8");
                SmallIntVector count16 = (SmallIntVector) root.getVector("count16");
                BigIntVector count64 = (BigIntVector) root.getVector("count64");
                BitVector flag = (BitVector) root.getVector("flag");

                idVector.allocateNew(rowCount);
                count8.allocateNew(rowCount);
                count16.allocateNew(rowCount);
                count64.allocateNew(rowCount);
                flag.allocateNew(rowCount);

                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    if (i == nullRow) {
                        count8.setNull(i);
                        count16.setNull(i);
                        count64.setNull(i);
                        flag.setNull(i);
                    } else {
                        count8.set(i, (byte) i);
                        count16.set(i, (short) (i * 100));
                        count64.set(i, 4_000_000_000L + i);
                        flag.set(i, (i % 3 == 0) ? 1 : 0);
                    }
                }
                idVector.setValueCount(rowCount);
                count8.setValueCount(rowCount);
                count16.setValueCount(rowCount);
                count64.setValueCount(rowCount);
                flag.setValueCount(rowCount);
                root.setRowCount(rowCount);

                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Drop columns from an existing Lance table. Simulates
     * {@code dataset.drop_columns([...])} from Python / Rust; used by
     * integration tests that exercise mapping-drift detection when the
     * writer removes columns from a table the plugin has already
     * surfaced.
     */
    static void dropColumns(String tableUri, java.util.List<String> columns) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            dataset.dropColumns(columns);
        }
    }

    /**
     * Delete rows from an existing Lance table by SQL predicate.
     * Simulates {@code dataset.delete("...")} from Python / Rust; the
     * fragment gains a deletion file and its physical row count stays
     * unchanged, which is the shape the leaf reader's liveDocs path
     * has to mask. Used by integration tests that assert deleted rows
     * stay out of hits, totals, and fetched {@code _source}.
     */
    static void deleteRows(String tableUri, String predicate) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            dataset.delete(predicate);
        }
    }

    /**
     * Writes a Lance table with a Utf8 column that has no FTS index, so
     * {@code RestAttachAction.derive} maps the column to
     * {@code keyword} rather than {@code lance_text}. The leaf reader
     * used to build a duplicate {@code FieldInfo} for that column
     * (once through the keyword doc values path and once through the
     * text-column-for-FLS loop), which tripped
     * {@code IllegalArgumentException: duplicate field names} and
     * left every FTS-less string-column table red. The regression
     * fixture is a two-column table so no other loader touches the
     * problem column: {@code id} int32 primary key and {@code label}
     * Utf8 without an inverted index.
     *
     * @return absolute URI of the table, usable as-is for
     *         {@code /_lance/attach} or namespace register.
     */
    static String writeKeywordOnlyTable(Path parent, String name, int rowCount) throws Exception {
        return withLocaleRoot(() -> writeKeywordOnlyTableOnce(parent, name, rowCount));
    }

    /**
     * Writes a Lance table whose primary key is a Utf8 column. Uses a Utf8
     * only schema so the {@code lance-schema:unenforced-primary-key}
     * metadata can be attached to the PK field without tripping the C Data
     * serialisation bug that fires when a FixedSizeList column sits in the
     * same schema (see the note on {@link #writeTable}). Row layout for
     * {@code rowCount} rows:
     * <ul>
     *   <li>{@code key = "alpha-i"} (deterministic, unique per row)</li>
     *   <li>{@code label = "row-i"} for even {@code i}, {@code "col-i"}
     *       for odd {@code i}</li>
     * </ul>
     * Used by the {@code _id} string PK integration tests to verify that
     * {@code _search} echoes the Utf8 PK values as {@code _id} and
     * {@code GET /{index}/_doc/{key}} resolves via a quoted Lance filter.
     */
    static String writeStringPkTable(Path parent, String name, int rowCount) throws Exception {
        return withLocaleRoot(() -> writeStringPkTableOnce(parent, name, rowCount));
    }

    private static String writeStringPkTableOnce(Path parent, String name, int rowCount) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        java.util.Map<String, String> pkMeta = Map.of("lance-schema:unenforced-primary-key", "true");
        Schema schema = new Schema(
            Arrays.asList(
                // PK metadata rides on the field's FieldType. Utf8 alone in
                // the schema keeps the C Data bridge from tripping on the
                // FixedSizeList issue the note on writeTable documents.
                // Lance also requires the primary key column itself to be
                // non-nullable ("Primary key column and all its ancestors
                // must not be nullable" from lance-core's schema
                // validator), so nullable is false on the PK field.
                new Field("key", new FieldType(false, new ArrowType.Utf8(), null, pkMeta), null),
                new Field("label", FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                VarCharVector keyVector = (VarCharVector) root.getVector("key");
                VarCharVector labelVector = (VarCharVector) root.getVector("label");
                keyVector.allocateNew();
                labelVector.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    keyVector.setSafe(i, ("alpha-" + i).getBytes(StandardCharsets.UTF_8));
                    String label = (i % 2 == 0 ? "row-" : "col-") + i;
                    labelVector.setSafe(i, label.getBytes(StandardCharsets.UTF_8));
                }
                keyVector.setValueCount(rowCount);
                labelVector.setValueCount(rowCount);
                root.setRowCount(rowCount);
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                // No FTS index on either column: the point is to exercise
                // the keyword PK path, not FTS. Derivation surfaces the
                // key column as `keyword` and the mapping still lets term
                // and match queries resolve.
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Writes a Lance table whose primary key is a UInt64 column. Values
     * intentionally straddle Long.MAX_VALUE so the resulting {@code _id}
     * strings cover both the low half (fits in signed long) and the top
     * half (only expressible as an unsigned long / BigInteger) of the
     * UInt64 range. Layout for a {@code rowCount == 4} table:
     * <ul>
     *   <li>row 0: {@code id = 0}</li>
     *   <li>row 1: {@code id = 42}</li>
     *   <li>row 2: {@code id = Long.MAX_VALUE} (9223372036854775807)</li>
     *   <li>row 3: {@code id = 18446744073709551610} (2^64 - 6, top of UInt64 range, wraps to -6 as a signed long)</li>
     * </ul>
     * The PK column is non-nullable to satisfy Lance's schema validator
     * ("Primary key column and all its ancestors must not be nullable").
     * Utf8-only sidecar column is included so integer / string columns
     * both round-trip.
     */
    static String writeUnsignedLongPkTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeUnsignedLongPkTableOnce(parent, name));
    }

    private static String writeUnsignedLongPkTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        java.util.Map<String, String> pkMeta = Map.of("lance-schema:unenforced-primary-key", "true");
        // Fixed four-row fixture. Kept out of the caller signature so
        // the values that straddle Long.MAX_VALUE stay stable across
        // tests without leaking test-specific tuning into other
        // fixtures.
        long[] rows = new long[] {
            0L,
            42L,
            Long.MAX_VALUE,
            // 2^64 - 6, the top of the UInt64 range. As a signed long
            // this is -6; the reader stores the raw bit pattern and
            // Long.toUnsignedString decodes it back.
            0xFFFFFFFFFFFFFFFAL };
        int rowCount = rows.length;
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", new FieldType(false, new ArrowType.Int(64, /* isSigned */ false), null, pkMeta), null),
                new Field("label", FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                org.apache.arrow.vector.UInt8Vector idVector = (org.apache.arrow.vector.UInt8Vector) root.getVector("id");
                VarCharVector labelVector = (VarCharVector) root.getVector("label");
                idVector.allocateNew(rowCount);
                labelVector.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, rows[i]);
                    labelVector.setSafe(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
                }
                idVector.setValueCount(rowCount);
                labelVector.setValueCount(rowCount);
                root.setRowCount(rowCount);
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    private static String writeKeywordOnlyTableOnce(Path parent, String name, int rowCount) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                // Same rationale as writeTable: no PK metadata in the
                // schema, because Arrow C Data serialisation cannot
                // carry it alongside our other fixtures.
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("label", FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector labelVector = (VarCharVector) root.getVector("label");
                idVector.allocateNew(rowCount);
                labelVector.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    labelVector.setSafe(i, ("row-" + i).getBytes(StandardCharsets.UTF_8));
                }
                idVector.setValueCount(rowCount);
                labelVector.setValueCount(rowCount);
                root.setRowCount(rowCount);
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                // Deliberately no createIndex call: leaving the Utf8
                // column without an inverted index is what triggers
                // the keyword code path in LanceFragmentLeafReader.
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Writes a Lance table exercising a Timestamp column. Used by the
     * date-range regression tests: a {@code range} query with an
     * ISO-8601 string literal on a Timestamp column used to return 400
     * because {@link org.opensearch.lance.query.LanceKnnFilterTranslator}
     * emitted a plain Utf8 SQL literal that DataFusion could not
     * compare against a Timestamp. This fixture is the smallest schema
     * that reproduces the bug: an int primary key so hits assertions
     * can pin down individual rows, a Utf8 category column so bool
     * filter tests can combine a keyword term with a date range, and a
     * {@code Timestamp(Microsecond, None)} column so the fragment
     * reader normalises the values to epoch millis for OpenSearch's
     * date field type.
     *
     * <p>Row layout (fixed six-row table so the caller does not have
     * to pick between date coverage and row count):
     * <ul>
     *   <li>id 0, category "even", ts 2024-01-15T00:00:00Z</li>
     *   <li>id 1, category "odd",  ts 2024-02-20T00:00:00Z</li>
     *   <li>id 2, category "even", ts 2024-03-10T00:00:00Z</li>
     *   <li>id 3, category "odd",  ts 2024-03-25T00:00:00Z</li>
     *   <li>id 4, category "even", ts 2024-04-05T00:00:00Z</li>
     *   <li>id 5, category "odd",  ts 2024-05-30T00:00:00Z</li>
     * </ul>
     * The spread lets a {@code [2024-03-01, 2024-04-01)} range pick
     * the two March rows, a datetime range starting mid-March slice
     * partial months, and a monthly {@code date_histogram} produce
     * five buckets with the March bucket carrying two docs. The
     * category column has no FTS index so derivation maps it to
     * {@code keyword}; term queries can pin the odd or even subset
     * without waking up FTS scoring.
     *
     * @return absolute URI of the table, usable as-is for
     *         {@code /_lance/attach} or namespace register.
     */
    static String writeDatedTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeDatedTableOnce(parent, name));
    }

    private static String writeDatedTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        // Fixed six-row layout. Values live inside the method so the
        // assertions in the IT can name the exact row that must fall
        // inside a given range without leaking test tuning into the
        // caller.
        long[] tsMicros = new long[] {
            Instant.parse("2024-01-15T00:00:00Z").toEpochMilli() * 1000L,
            Instant.parse("2024-02-20T00:00:00Z").toEpochMilli() * 1000L,
            Instant.parse("2024-03-10T00:00:00Z").toEpochMilli() * 1000L,
            Instant.parse("2024-03-25T00:00:00Z").toEpochMilli() * 1000L,
            Instant.parse("2024-04-05T00:00:00Z").toEpochMilli() * 1000L,
            Instant.parse("2024-05-30T00:00:00Z").toEpochMilli() * 1000L };
        int rowCount = tsMicros.length;
        Schema schema = new Schema(
            Arrays.asList(
                // Same rationale as writeTable: no PK metadata in the
                // schema. This fixture does not need GET /_doc so the
                // synthesised offset-based _id from LanceFragmentLeafReader
                // is enough.
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null)
            ),
            Map.of()
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector categoryVector = (VarCharVector) root.getVector("category");
                TimeStampMicroVector tsVector = (TimeStampMicroVector) root.getVector("ts");

                idVector.allocateNew(rowCount);
                categoryVector.allocateNew();
                tsVector.allocateNew(rowCount);

                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    String category = (i % 2 == 0) ? "even" : "odd";
                    categoryVector.setSafe(i, category.getBytes(StandardCharsets.UTF_8));
                    tsVector.set(i, tsMicros[i]);
                }
                idVector.setValueCount(rowCount);
                categoryVector.setValueCount(rowCount);
                tsVector.setValueCount(rowCount);
                root.setRowCount(rowCount);
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                // No index on the category or ts column: the point is
                // to exercise the range-on-Timestamp SQL path, not any
                // scalar index. Derivation maps ts to `date` and
                // category to `keyword` unconditionally.
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Writes a Lance table exercising Float32 and Float64 scalar columns.
     * The default {@link #writeTable} fixture only carries integer, string
     * and vector columns, so it cannot exercise the floating point mapping
     * path {@code RestAttachAction.derive} lays down for the {@code float}
     * and {@code double} OpenSearch field types.
     *
     * <p>Row layout (fixed six-row table):
     * <ul>
     *   <li>id (int32, non-null): {@code i}</li>
     *   <li>price (float32, nullable): {@code i * 12.5f} — spread across the
     *       full precision range so range assertions in the IT stay tight</li>
     *   <li>weight (float64, nullable): {@code i / 3.0} — irrational enough
     *       that a naive integer-only round trip mangles it</li>
     * </ul>
     * The floats and doubles are stored through the same shared
     * {@code long[]} column storage as integers via
     * {@code NumericUtils.floatToSortableInt} /
     * {@code NumericUtils.doubleToSortableLong}, then decoded back on the
     * read side. Assertions in the IT cross-check the JSON {@code _source}
     * values against the original {@code f}/{@code d} inputs to prove the
     * round trip does not lose precision.
     *
     * @return absolute URI of the table.
     */
    static String writeFloatColumnTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeFloatColumnTableOnce(parent, name));
    }

    private static String writeFloatColumnTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        int rowCount = 6;
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("price", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
                new Field("weight", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            ),
            Map.of()
        );

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                Float4Vector priceVector = (Float4Vector) root.getVector("price");
                Float8Vector weightVector = (Float8Vector) root.getVector("weight");

                idVector.allocateNew(rowCount);
                priceVector.allocateNew(rowCount);
                weightVector.allocateNew(rowCount);

                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    priceVector.set(i, i * 12.5f);
                    weightVector.set(i, i / 3.0);
                }
                idVector.setValueCount(rowCount);
                priceVector.setValueCount(rowCount);
                weightVector.setValueCount(rowCount);
                root.setRowCount(rowCount);

                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                ipcBytes = out.toByteArray();
            }

            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Pins the JVM's default {@link Locale} to {@link Locale#ROOT}
     * for the duration of a Lance write, saving and restoring the
     * previous default in a {@code finally} block.
     *
     * <h2>Why this is necessary</h2>
     *
     * <p>Apache Arrow Java 18.1.0 formats the C Data interface schema
     * string in {@code org.apache.arrow.c.Format#asString} using
     * {@code String.format("+w:%d", listSize)} without an explicit
     * locale. {@code String.format(String, Object...)} routes through
     * {@code Locale.getDefault(Locale.Category.FORMAT)} and Java's
     * {@code Formatter} Number Localization Algorithm rewrites each
     * digit using the current locale's
     * {@code DecimalFormatSymbols.getZeroDigit()}. Roughly 9.2% of the
     * locales available on JDK 21.0.6 (98 of 1069) use a non-ASCII zero
     * digit: Arabic-Indic ({@code ar-*}, {@code fa-*}, {@code ur-IN}),
     * Bengali ({@code as}, {@code bn-*}), Devanagari ({@code mr},
     * {@code ne}), Myanmar ({@code my}), Tibetan ({@code dz}) and
     * others. When the default locale is one of those, the schema
     * string becomes {@code "+w:٨"} (or similar), and the arrow-rs 58
     * side (which is what Lance 11 embeds) tries to parse the digits
     * with {@code num_elems.parse::<i32>()}. The {@code i32::from_str}
     * parser only accepts ASCII digits, so the call fails with
     * {@code "The FixedSizeList type requires an integer parameter
     * representing number of elements per list"}.
     *
     * <p>The same {@code %d} pattern is used in the Format helper for
     * {@code FixedSizeBinary} and {@code Decimal}, so any of those
     * three types passed through the Arrow C Data bridge from Java is
     * affected. Every fixture in this factory is wrapped for
     * uniformity even when the current schema does not contain one of
     * the three, so future edits that add {@code FixedSizeList} to a
     * fixture inherit the fix automatically.
     *
     * <h2>Why the JVM default is randomised inside integTest</h2>
     *
     * <p>Lucene's test framework rule
     * {@code TestRuleSetupAndRestoreClassEnv#before} picks a locale
     * from {@code LuceneTestCase.randomLocale(Random)} on every test
     * class and installs it with {@code Locale.setDefault(...)}
     * before the class runs. {@code OpenSearchTestCase.ensureSupportedLocale}
     * only overrides that to English on a FIPS JVM, so under a normal
     * integTest the seed of the run drives a decision that lands on
     * an "Arabic-Indic digits" locale about 9 out of 100 seeds. The
     * result is a decisive, seed-deterministic failure that looked
     * like a "flake" only because the reproducer had never been run
     * with the same seed twice.
     *
     * <h2>Why we scope the pin to the write region</h2>
     *
     * <p>{@code Locale.setDefault(...)} mutates a JVM-global piece of
     * state, so pinning it for the entire test run would erase the
     * Locale-randomization coverage that OpenSearch and Lucene rely
     * on to catch locale-sensitive bugs in the code under test. Only
     * the Lance write path needs ASCII digits, so scoping the pin to
     * that region keeps the coverage for everything else. The tests
     * run single-threaded so there is no window where another thread
     * observes the temporary {@code Locale.ROOT}.
     *
     * <h2>Follow-up: fix in Apache Arrow Java</h2>
     *
     * <p>The upstream fix is to add {@code Locale.ROOT} to the four
     * {@code String.format} calls in
     * {@code org.apache.arrow.c.Format#asString} (or replace them with
     * plain string concatenation, since {@code Integer.toString} is
     * locale-independent). Tracked as a separate follow-up in
     * {@code research/opensearch/lance-integration/lance-11-ffi-flake.md};
     * once a fixed arrow-java is released and the {@code arrow-c-data}
     * dependency in {@code build.gradle} is bumped, this workaround
     * can be deleted.
     */
    @FunctionalInterface
    private interface ThrowingSupplier {
        String get() throws Exception;
    }

    private static String withLocaleRoot(ThrowingSupplier supplier) throws Exception {
        Locale saved = Locale.getDefault();
        Locale.setDefault(Locale.ROOT);
        try {
            return supplier.get();
        } finally {
            Locale.setDefault(saved);
        }
    }
}
