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
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
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
import org.lance.index.DistanceType;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.vector.VectorIndexParams;

/**
 * Test-only helper that writes a small Lance table onto the local
 * filesystem for the REST integration tests. Mirrors the shape a
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
 * <p>Not part of the plugin runtime; kept under {@code src/testFixtures}
 * and only referenced from tests. The class is public so tests outside
 * this package can reach {@link #writeMultiFragmentTable}; every other
 * fixture is package-private.
 */
public final class LanceTableFactory {

    static final String VECTOR_COLUMN = "embedding";
    static final int VECTOR_DIM = 8;
    static final String BODY_COLUMN = "body";
    static final String TITLE_COLUMN = "title";
    static final String PRIMARY_KEY = "id";
    static final String JAPANESE_TEXT_COLUMN = "text";

    /**
     * Rows of {@link #writeJapaneseTable}. None of the sentences contains
     * whitespace or punctuation, so Lance's {@code simple} tokenizer keeps
     * each one as a single token and a one-word query only matches after a
     * morphological tokenizer ({@code icu}, {@code lindera/ipadic}) has
     * split it. Word counts the ITs rely on: 天気 in rows 0 and 1, 東京 in
     * rows 0 and 3, 京都 in row 2 only.
     */
    static final String[] JAPANESE_SENTENCES = new String[] { "東京の天気は晴れです", "大阪の天気は雨です", "京都には古い寺が多い", "今日は東京で会議があります", "日本語の形態素解析を試す" };

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
        return withLocaleRoot(() -> writeTableOnce(parent, name, rowCount, 0));
    }

    /**
     * Same row layout and indexes as {@link #writeTable}, but the writer
     * closes a data file (and therefore a fragment) every
     * {@code maxRowsPerFile} rows, so a {@code rowCount} of 12 with
     * {@code maxRowsPerFile} 4 yields three fragments holding rows
     * 0..3, 4..7 and 8..11. Fragment ids are assigned in write order
     * starting at 0, so the synthesised {@code _id} of row {@code i}
     * is {@code (i / maxRowsPerFile) + "-" + (i % maxRowsPerFile)}.
     *
     * <p>Public because the query package's unit tests need a real
     * multi-fragment {@link Dataset} to exercise fragment coverage
     * checks; the other fixtures stay package-private.
     */
    public static String writeMultiFragmentTable(Path parent, String name, int rowCount, int maxRowsPerFile) throws Exception {
        if (maxRowsPerFile <= 0) {
            throw new IllegalArgumentException("maxRowsPerFile must be positive, was " + maxRowsPerFile);
        }
        return withLocaleRoot(() -> writeTableOnce(parent, name, rowCount, maxRowsPerFile));
    }

    private static String writeTableOnce(Path parent, String name, int rowCount, int maxRowsPerFile) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                // No `lance-schema:unenforced-primary-key` metadata: adding it
                // to this field breaks the C Data serialisation of the
                // FixedSizeList column below. The table therefore has no
                // declared primary key and GET /_doc answers 404.
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
                WriteParams.Builder writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE);
                if (maxRowsPerFile > 0) {
                    // Only the multi-fragment fixture sets this; writeTable
                    // keeps Lance's default (one fragment for these sizes).
                    writeParams = writeParams.withMaxRowsPerFile(maxRowsPerFile);
                }
                try (Dataset dataset = Dataset.create(allocator, stream, uri, writeParams.build())) {
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

    /**
     * Append {@code rowCount} more rows with the same layout as
     * {@link #writeTable} to an existing table, with ids starting at
     * {@code startId}. Produces a new manifest version, which is what
     * tests of tag following and manifest advance need.
     */
    static void appendRows(String tableUri, int startId, int rowCount) throws Exception {
        withLocaleRoot(() -> {
            Schema schema = new Schema(
                Arrays.asList(
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
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                byte[] ipcBytes = writeIpcBatch(allocator, schema, startId, rowCount);
                try (
                    ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                    ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
                ) {
                    Data.exportArrayStream(allocator, reader, stream);
                    WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.APPEND).build();
                    try (Dataset ignored = Dataset.create(allocator, stream, tableUri, writeParams)) {
                        // Nothing to do: opening the appended dataset commits it.
                    }
                }
            }
            return tableUri;
        });
    }

    /** Current (latest) manifest version of an existing table. */
    static long currentVersion(String tableUri) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            return dataset.version();
        }
    }

    /**
     * Create a tag pointing at {@code version}. Simulates
     * {@code dataset.tags.create(...)} from Python / Rust.
     */
    static void createTag(String tableUri, String tag, long version) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            dataset.tags().create(tag, version);
        }
    }

    /**
     * Move an existing tag to {@code version}. Simulates
     * {@code dataset.tags.update(...)} from Python / Rust; the shape a
     * writer uses to promote a new snapshot under a stable name.
     */
    static void updateTag(String tableUri, String tag, long version) throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build()
        ) {
            dataset.tags().update(tag, version);
        }
    }

    private static byte[] writeIpcBatch(RootAllocator allocator, Schema schema, int rowCount) throws Exception {
        return writeIpcBatch(allocator, schema, 0, rowCount);
    }

    private static byte[] writeIpcBatch(RootAllocator allocator, Schema schema, int startId, int rowCount) throws Exception {
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

            for (int slot = 0; slot < rowCount; slot++) {
                int i = startId + slot;
                idVector.set(slot, i);
                String body = (i % 2 == 0 ? "hello lance " : "quick brown fox ") + i;
                bodyVector.setSafe(slot, body.getBytes(StandardCharsets.UTF_8));
                String title = (i % 2 == 0 ? "sunny morning " : "cloudy morning ") + i;
                titleVector.setSafe(slot, title.getBytes(StandardCharsets.UTF_8));
                for (int j = 0; j < VECTOR_DIM; j++) {
                    // Row i lives at coordinate (i, 0, 0, ...). Distances
                    // between two rows become |i - k| so nearest-neighbour
                    // ordering is deterministic and easy to assert.
                    float value = (j == 0) ? (float) i : 0.0f;
                    vecItems.set(slot * VECTOR_DIM + j, value);
                }
                vecVector.setNotNull(slot);
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
     * Writes a Lance table with nullable int8 / int16 / int64 and boolean
     * columns, for tests of Arrow null handling and integer width mapping.
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
     * Writes a Lance table with a nested Struct column, for tests of the
     * {@code object} mapping derivation and the dotted-path doc value /
     * {@code _source} plumbing. Public because the rest package's
     * derivation unit tests need it too.
     *
     * <p>Schema: {@code id} int32 primary key (non-nullable),
     * {@code meta} nullable struct with children {@code region} Utf8,
     * {@code score} Float64, {@code raw} UInt32 (deliberately a type the
     * derivation does not support inside a struct, so the skip note and
     * the "parent object still emitted" behaviour are exercised),
     * {@code flags}, itself a nullable struct with one {@code active}
     * Bool child, and {@code audit}, a nullable struct whose only child
     * {@code checksum} is UInt32 (no supported descendant, so the whole
     * nested struct must be skipped with a note and stay out of the
     * mapping and {@code _source}).
     *
     * <p>Six rows ({@code i = 0..5}):
     * <ul>
     *   <li>{@code id = i}</li>
     *   <li>{@code meta.region}: east, west, east, east, south, west</li>
     *   <li>{@code meta.score = i * 1.5}, except row 3 where it is
     *       Arrow null (a null scalar leaf inside a present struct)</li>
     *   <li>{@code meta.raw = i}</li>
     *   <li>{@code meta.flags.active = (i % 2 == 0)}, except row 3 where
     *       {@code meta.flags} is Arrow null (a null nested struct)</li>
     *   <li>{@code meta.audit.checksum = i}</li>
     * </ul>
     *
     * @param maxRowsPerFile 0 writes one fragment; a positive value
     *        closes a data file every that many rows (2 gives three
     *        fragments), for multi-node fan-out coverage
     */
    public static String writeStructTable(Path parent, String name, int maxRowsPerFile) throws Exception {
        return withLocaleRoot(() -> writeStructTableOnce(parent, name, maxRowsPerFile));
    }

    private static String writeStructTableOnce(Path parent, String name, int maxRowsPerFile) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Map<String, String> pkMeta = Map.of("lance-schema:unenforced-primary-key", "true");
        Field flagsField = new Field(
            "flags",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(new Field("active", FieldType.nullable(new ArrowType.Bool()), null))
        );
        Field auditField = new Field(
            "audit",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(new Field("checksum", FieldType.nullable(new ArrowType.Int(32, false)), null))
        );
        Field metaField = new Field(
            "meta",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field("region", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("raw", FieldType.nullable(new ArrowType.Int(32, false)), null),
                flagsField,
                auditField
            )
        );
        Schema schema = new Schema(
            Arrays.asList(new Field("id", new FieldType(false, new ArrowType.Int(32, true), null, pkMeta), null), metaField),
            Map.of()
        );

        int rowCount = 6;
        String[] regions = { "east", "west", "east", "east", "south", "west" };

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                StructVector meta = (StructVector) root.getVector("meta");
                VarCharVector region = (VarCharVector) meta.getChild("region");
                Float8Vector score = (Float8Vector) meta.getChild("score");
                UInt4Vector raw = (UInt4Vector) meta.getChild("raw");
                StructVector flags = (StructVector) meta.getChild("flags");
                BitVector active = (BitVector) flags.getChild("active");
                StructVector audit = (StructVector) meta.getChild("audit");
                UInt4Vector checksum = (UInt4Vector) audit.getChild("checksum");

                root.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    idVector.setSafe(i, i);
                    meta.setIndexDefined(i);
                    region.setSafe(i, regions[i].getBytes(StandardCharsets.UTF_8));
                    raw.setSafe(i, i);
                    audit.setIndexDefined(i);
                    checksum.setSafe(i, i);
                    if (i == 3) {
                        score.setNull(i);
                        flags.setNull(i);
                    } else {
                        score.setSafe(i, i * 1.5d);
                        flags.setIndexDefined(i);
                        active.setSafe(i, (i % 2 == 0) ? 1 : 0);
                    }
                }
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
                WriteParams.Builder writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE);
                if (maxRowsPerFile > 0) {
                    writeParams = writeParams.withMaxRowsPerFile(maxRowsPerFile);
                }
                Dataset.create(allocator, stream, uri, writeParams.build()).close();
            }
        }
        return uri;
    }

    /**
     * Writes a table with a {@code List<Struct>} column for the nested
     * field tests: {@code id} int32 PK, {@code title} Utf8 (no FTS
     * index, maps to keyword) and {@code items}, a list of
     * {@code struct<color: utf8, size: utf8, qty: int32>}. Six rows:
     * <ul>
     *   <li>row 0: title alpha, items [(red, small, 1)]</li>
     *   <li>row 1: title beta, items [(red, large, 2), (blue, small, 3),
     *       (green, medium, 4)]</li>
     *   <li>row 2: title alpha, items [] (zero elements)</li>
     *   <li>row 3: title beta, items [(red, small, 5), (blue, large, 6)]
     *       — red and large appear in different elements, the cross
     *       element case a nested query must not match</li>
     *   <li>row 4: title gamma, items [(yellow, small, 7)]</li>
     *   <li>row 5: title alpha, items [(red, large, 8), (purple, tiny,
     *       9)] — tests delete this row afterwards through
     *       {@link #deleteRows}</li>
     * </ul>
     *
     * @param maxRowsPerFile 0 writes one fragment; a positive value
     *        closes a data file every that many rows (2 gives three
     *        fragments), for multi-node fan-out coverage
     */
    public static String writeNestedTable(Path parent, String name, int maxRowsPerFile) throws Exception {
        return withLocaleRoot(() -> writeNestedTableOnce(parent, name, maxRowsPerFile));
    }

    private static String writeNestedTableOnce(Path parent, String name, int maxRowsPerFile) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Map<String, String> pkMeta = Map.of("lance-schema:unenforced-primary-key", "true");
        Field elementField = new Field(
            "item",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field("color", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("size", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("qty", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", new FieldType(false, new ArrowType.Int(32, true), null, pkMeta), null),
                new Field("title", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("items", FieldType.nullable(new ArrowType.List()), Collections.singletonList(elementField))
            ),
            Map.of()
        );

        String[] titles = { "alpha", "beta", "alpha", "beta", "gamma", "alpha" };
        String[][][] items = {
            { { "red", "small", "1" } },
            { { "red", "large", "2" }, { "blue", "small", "3" }, { "green", "medium", "4" } },
            {},
            { { "red", "small", "5" }, { "blue", "large", "6" } },
            { { "yellow", "small", "7" } },
            { { "red", "large", "8" }, { "purple", "tiny", "9" } } };
        int rowCount = titles.length;

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector titleVector = (VarCharVector) root.getVector("title");
                ListVector itemsVector = (ListVector) root.getVector("items");
                StructVector element = (StructVector) itemsVector.getDataVector();
                VarCharVector color = (VarCharVector) element.getChild("color");
                VarCharVector size = (VarCharVector) element.getChild("size");
                IntVector qty = (IntVector) element.getChild("qty");

                root.allocateNew();
                int elem = 0;
                for (int i = 0; i < rowCount; i++) {
                    idVector.setSafe(i, i);
                    titleVector.setSafe(i, titles[i].getBytes(StandardCharsets.UTF_8));
                    itemsVector.startNewValue(i);
                    for (String[] item : items[i]) {
                        element.setIndexDefined(elem);
                        color.setSafe(elem, item[0].getBytes(StandardCharsets.UTF_8));
                        size.setSafe(elem, item[1].getBytes(StandardCharsets.UTF_8));
                        qty.setSafe(elem, Integer.parseInt(item[2]));
                        elem++;
                    }
                    itemsVector.endValue(i, items[i].length);
                }
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
                WriteParams.Builder writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE);
                if (maxRowsPerFile > 0) {
                    writeParams = writeParams.withMaxRowsPerFile(maxRowsPerFile);
                }
                Dataset.create(allocator, stream, uri, writeParams.build()).close();
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
     * Writes a two-column Lance table ({@code id} int32, {@code label}
     * Utf8) whose Utf8 column has no FTS index, so the derived mapping is
     * {@code keyword} rather than {@code lance_text}.
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
     */
    static String writeStringPkTable(Path parent, String name, int rowCount) throws Exception {
        return withLocaleRoot(() -> writeStringPkTableOnce(parent, name, rowCount, 0));
    }

    /**
     * Same rows as {@link #writeStringPkTable(Path, String, int)}, written
     * {@code maxRowsPerFile} rows per fragment, so a table with a primary
     * key spans several fragments (row {@code i} sits in fragment
     * {@code i / maxRowsPerFile}).
     */
    public static String writeStringPkTable(Path parent, String name, int rowCount, int maxRowsPerFile) throws Exception {
        if (maxRowsPerFile <= 0) {
            throw new IllegalArgumentException("maxRowsPerFile must be positive, was " + maxRowsPerFile);
        }
        return withLocaleRoot(() -> writeStringPkTableOnce(parent, name, rowCount, maxRowsPerFile));
    }

    private static String writeStringPkTableOnce(Path parent, String name, int rowCount, int maxRowsPerFile) throws Exception {
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
                WriteParams.Builder writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE);
                if (maxRowsPerFile > 0) {
                    writeParams = writeParams.withMaxRowsPerFile(maxRowsPerFile);
                }
                // No FTS index on either column: the point is to exercise
                // the keyword PK path, not FTS. Derivation surfaces the
                // key column as `keyword` and the mapping still lets term
                // and match queries resolve.
                Dataset.create(allocator, stream, uri, writeParams.build()).close();
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
        String[] labels = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            labels[i] = "row-" + i;
        }
        writeIdAndUtf8Table(uri, schema, "label", labels);
        return uri;
    }

    /**
     * Writes a two-column Lance table ({@code id} int32, {@code text} Utf8)
     * holding the five Japanese sentences in {@link #JAPANESE_SENTENCES},
     * without an FTS index. The ITs build the index afterwards through
     * {@code POST /_lance/build_indexes/{index}} with a chosen tokenizer,
     * so the attach derivation first maps {@code text} as {@code keyword}
     * and the namespace poll flips it to {@code lance_text} once the
     * build lands.
     *
     * @return absolute URI of the table, usable as-is for
     *         {@code /_lance/attach} or namespace register.
     */
    static String writeJapaneseTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeJapaneseTableOnce(parent, name));
    }

    private static String writeJapaneseTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(JAPANESE_TEXT_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );
        writeIdAndUtf8Table(uri, schema, JAPANESE_TEXT_COLUMN, JAPANESE_SENTENCES);
        return uri;
    }

    /**
     * Shared writer for the {@code id} int32 + one Utf8 column fixtures.
     * Row {@code i} gets {@code id = i} and {@code values[i]} in the Utf8
     * column. Deliberately no createIndex call: leaving the Utf8 column
     * without an inverted index is what makes the attach derivation map
     * it as {@code keyword}.
     */
    private static void writeIdAndUtf8Table(String uri, Schema schema, String utf8Column, String[] values) throws Exception {
        int rowCount = values.length;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes;
            try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream()
            ) {
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector textVector = (VarCharVector) root.getVector(utf8Column);
                idVector.allocateNew(rowCount);
                textVector.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    textVector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
                }
                idVector.setValueCount(rowCount);
                textVector.setValueCount(rowCount);
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
    }

    /**
     * Writes a Lance table with a {@code Timestamp(Microsecond, None)}
     * column for date range tests, plus an int primary key so assertions
     * can pin individual rows and a Utf8 category column so bool filters
     * can combine a keyword term with a date range.
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
     * Public because the query package's unit tests read it directly.
     *
     * @return absolute URI of the table, usable as-is for
     *         {@code /_lance/attach} or namespace register.
     */
    public static String writeDatedTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeDatedTableOnce(parent, name));
    }

    /**
     * Writes a Lance table for the mapping override tests: epoch millis
     * stored as a plain Int64 column next to a Utf8 column that carries
     * an inverted index.
     *
     * <p>Row layout (fixed six-row table):
     * <ul>
     *   <li>id (int32, nullable, no PK metadata): {@code i}</li>
     *   <li>ts (int64, nullable): epoch millis of 2024-01-15, 2024-02-20,
     *       2024-03-10, 2024-03-25, 2024-04-05, 2024-05-30 (midnight
     *       UTC), so a March range holds exactly rows 2 and 3</li>
     *   <li>label (utf8, nullable, INVERTED index): {@code "hello lance " + i}
     *       for even {@code i}, {@code "quick brown fox " + i} for odd
     *       {@code i} — six distinct values, each with spaces so an
     *       exact term match proves the value was not analysed</li>
     * </ul>
     *
     * <p>Without overrides derivation maps ts to {@code long} and label
     * to {@code lance_text}; the override tests flip them to
     * {@code date} and {@code keyword}.
     */
    public static String writeEpochMillisTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeEpochMillisTableOnce(parent, name));
    }

    /** Epoch millis (midnight UTC) of the six rows {@link #writeEpochMillisTable} writes. */
    public static long[] epochMillisFixtureValues() {
        return new long[] {
            Instant.parse("2024-01-15T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-02-20T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-03-10T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-03-25T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-04-05T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-05-30T00:00:00Z").toEpochMilli() };
    }

    private static String writeEpochMillisTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        long[] millis = epochMillisFixtureValues();
        int rowCount = millis.length;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            byte[] ipcBytes = epochMillisBatch(allocator, 0, rowCount, millis);
            try (
                ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE).build();
                try (Dataset dataset = Dataset.create(allocator, stream, uri, writeParams)) {
                    ScalarIndexParams scalarParams = ScalarIndexParams.create(
                        "inverted",
                        "{\"base_tokenizer\":\"simple\",\"language\":\"English\",\"with_position\":true}"
                    );
                    IndexParams indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build();
                    dataset.createIndex(
                        IndexOptions.builder(Collections.singletonList("label"), IndexType.INVERTED, indexParams)
                            .withIndexName("label_fts")
                            .build()
                    );
                }
            }
        }
        return uri;
    }

    /**
     * Appends {@code rowCount} more rows to an existing
     * {@link #writeEpochMillisTable} table: ids from {@code startId},
     * ts values one day apart from 2024-06-01 and labels
     * {@code "extra lance " + id}. Produces a new manifest version.
     */
    public static void appendEpochMillisRows(String tableUri, int startId, int rowCount) throws Exception {
        withLocaleRoot(() -> {
            long base = Instant.parse("2024-06-01T00:00:00Z").toEpochMilli();
            long[] millis = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                millis[i] = base + i * 86_400_000L;
            }
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                byte[] ipcBytes = epochMillisBatch(allocator, startId, rowCount, millis);
                try (
                    ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes);
                    ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
                ) {
                    Data.exportArrayStream(allocator, reader, stream);
                    WriteParams writeParams = new WriteParams.Builder().withMode(WriteParams.WriteMode.APPEND).build();
                    Dataset.create(allocator, stream, tableUri, writeParams).close();
                }
            }
            return tableUri;
        });
    }

    private static byte[] epochMillisBatch(RootAllocator allocator, int startId, int rowCount, long[] millis) throws Exception {
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("label", FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IntVector idVector = (IntVector) root.getVector("id");
            BigIntVector tsVector = (BigIntVector) root.getVector("ts");
            VarCharVector labelVector = (VarCharVector) root.getVector("label");
            idVector.allocateNew(rowCount);
            tsVector.allocateNew(rowCount);
            labelVector.allocateNew();
            for (int i = 0; i < rowCount; i++) {
                int id = startId + i;
                idVector.set(i, id);
                tsVector.set(i, millis[i]);
                String label = startId > 0 ? "extra lance " + id : (id % 2 == 0 ? "hello lance " + id : "quick brown fox " + id);
                labelVector.setSafe(i, label.getBytes(StandardCharsets.UTF_8));
            }
            idVector.setValueCount(rowCount);
            tsVector.setValueCount(rowCount);
            labelVector.setValueCount(rowCount);
            root.setRowCount(rowCount);
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
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
     * Values of the {@code v} column of {@link #writeSignedValuesTable},
     * chosen around the multiples of 100 so a histogram with interval 100
     * has to floor negative quotients rather than truncate them: -250 and
     * -201 belong to bucket -300, -200 and -101 to -200, -1 to -100.
     */
    public static final long[] SIGNED_VALUES = new long[] { -250L, -201L, -200L, -101L, -1L, 0L, 1L, 99L, 100L, 250L };

    /**
     * Writes a Lance table with signed integer, float and millisecond
     * timestamp columns crossing zero, for tests of floor semantics in
     * bucket key arithmetic.
     *
     * <p>Row layout ({@code i} from 0 to 9):
     * <ul>
     *   <li>{@code id}: int32, {@code i}</li>
     *   <li>{@code v}: int64, {@link #SIGNED_VALUES}{@code [i]}</li>
     *   <li>{@code f}: float64, {@code SIGNED_VALUES[i] + 0.5}</li>
     *   <li>{@code ts}: timestamp[ms], {@code SIGNED_VALUES[i] * 86 400 000}
     *       (days around the epoch, so the 1969 rows are negative
     *       millis)</li>
     * </ul>
     * Public because the query package's unit tests read it directly.
     *
     * @return absolute URI of the table.
     */
    public static String writeSignedValuesTable(Path parent, String name) throws Exception {
        return withLocaleRoot(() -> writeSignedValuesTableOnce(parent, name));
    }

    private static String writeSignedValuesTableOnce(Path parent, String name) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        int rowCount = SIGNED_VALUES.length;
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("f", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null)
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
                BigIntVector vVector = (BigIntVector) root.getVector("v");
                Float8Vector fVector = (Float8Vector) root.getVector("f");
                TimeStampMilliVector tsVector = (TimeStampMilliVector) root.getVector("ts");

                idVector.allocateNew(rowCount);
                vVector.allocateNew(rowCount);
                fVector.allocateNew(rowCount);
                tsVector.allocateNew(rowCount);

                for (int i = 0; i < rowCount; i++) {
                    idVector.set(i, i);
                    vVector.set(i, SIGNED_VALUES[i]);
                    fVector.set(i, SIGNED_VALUES[i] + 0.5d);
                    tsVector.set(i, SIGNED_VALUES[i] * 86_400_000L);
                }
                idVector.setValueCount(rowCount);
                vVector.setValueCount(rowCount);
                fVector.setValueCount(rowCount);
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
                Dataset.create(allocator, stream, uri, writeParams).close();
            }
        }
        return uri;
    }

    /**
     * Writes a Lance table made of {@code fragments} fragments whose rows
     * interleave by id, for tests of the coordinator's cross-node hit
     * merge. Fragment {@code f} holds the rows with
     * {@code id % fragments == f}, so once the coordinator hands each
     * fragment to a different node, any id- or ts-ordered page has to
     * take hits from every node in turn; a merge that merely
     * concatenates per-node lists produces a visibly wrong order.
     *
     * <p>Columns, for a row with global id {@code i} ({@code 0 <= i <
     * fragments * rowsPerFragment}):
     * <ul>
     *   <li>{@code id}: int32, {@code i}</li>
     *   <li>{@code body}: Utf8 with an INVERTED index, {@code "hello
     *       tok<i>"} followed by the token {@code lance} repeated
     *       {@code i + 1} times. {@code tok<i>} matches exactly one row.
     *       Distinct term frequencies give every row a distinct BM25
     *       score for a {@code lance_match} on {@code lance}, so score
     *       order is total and does not depend on how ties are
     *       broken.</li>
     *   <li>{@code category}: Utf8 without an index (derives to
     *       {@code keyword}), {@code "c" + (i % 3)}</li>
     *   <li>{@code ts}: timestamp[us], {@code 2024-01-01T00:00:00Z} plus
     *       {@code i} days, so ts order equals id order</li>
     * </ul>
     * The first fragment is written with {@code CREATE}, the rest with
     * {@code APPEND}, one manifest version per fragment. Public because
     * the query and dispatch packages' unit tests need a multi-fragment
     * table with a total score order.
     *
     * @return absolute URI of the table.
     */
    public static String writeInterleavedTable(Path parent, String name, int fragments, int rowsPerFragment) throws Exception {
        return withLocaleRoot(() -> writeInterleavedTableOnce(parent, name, fragments, rowsPerFragment));
    }

    private static String writeInterleavedTableOnce(Path parent, String name, int fragments, int rowsPerFragment) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(BODY_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null)
            ),
            Map.of()
        );
        long epochMicros = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli() * 1000L;
        long dayMicros = 24L * 60L * 60L * 1_000_000L;

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            for (int fragment = 0; fragment < fragments; fragment++) {
                byte[] ipcBytes;
                try (
                    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    ByteArrayOutputStream out = new ByteArrayOutputStream()
                ) {
                    IntVector idVector = (IntVector) root.getVector("id");
                    VarCharVector bodyVector = (VarCharVector) root.getVector(BODY_COLUMN);
                    VarCharVector categoryVector = (VarCharVector) root.getVector("category");
                    TimeStampMicroVector tsVector = (TimeStampMicroVector) root.getVector("ts");
                    idVector.allocateNew(rowsPerFragment);
                    bodyVector.allocateNew();
                    categoryVector.allocateNew();
                    tsVector.allocateNew(rowsPerFragment);
                    for (int slot = 0; slot < rowsPerFragment; slot++) {
                        int i = fragment + slot * fragments;
                        idVector.set(slot, i);
                        String body = "hello tok" + i + " lance".repeat(i + 1);
                        bodyVector.setSafe(slot, body.getBytes(StandardCharsets.UTF_8));
                        categoryVector.setSafe(slot, ("c" + (i % 3)).getBytes(StandardCharsets.UTF_8));
                        tsVector.set(slot, epochMicros + i * dayMicros);
                    }
                    idVector.setValueCount(rowsPerFragment);
                    bodyVector.setValueCount(rowsPerFragment);
                    categoryVector.setValueCount(rowsPerFragment);
                    tsVector.setValueCount(rowsPerFragment);
                    root.setRowCount(rowsPerFragment);
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
                    WriteParams.WriteMode mode = fragment == 0 ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.APPEND;
                    WriteParams writeParams = new WriteParams.Builder().withMode(mode).build();
                    Dataset.create(allocator, stream, uri, writeParams).close();
                }
            }
            // The FTS index goes on last so it covers every fragment;
            // same parameters as writeTable.
            try (Dataset dataset = Dataset.open().allocator(allocator).uri(uri).build()) {
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
        return uri;
    }

    /**
     * Writes a Lance table made of {@code fragments} contiguous fragments
     * of {@code rowsPerFragment} rows each, with one column of every doc
     * value kind the fragment reader serves, for tests of hinted
     * (per-hit) doc value loading. Fragment {@code f} holds the rows
     * with global id {@code f * rowsPerFragment <= i < (f + 1) *
     * rowsPerFragment}; the fragment ids are assigned in write order
     * starting at 0, so the physical offset of row {@code i} inside its
     * fragment is {@code i % rowsPerFragment}.
     *
     * <p>Columns, for a row with global id {@code i}:
     * <ul>
     *   <li>{@code id}: int32, {@code i}</li>
     *   <li>{@code body}: Utf8 with an INVERTED index,
     *       {@code "hello tok<i> grp<i % 25> sp<i % 625>"} followed by
     *       the token {@code lance} repeated {@code (i % 5) + 1} times.
     *       {@code tok<i>} matches exactly one row, {@code grp<n>} one
     *       row in twenty five (4 percent, above the reader's sparse
     *       ratio, so a hit set of it loads the whole column),
     *       {@code sp<n>} one row in 625 (0.16 percent, below the sparse
     *       ratio once a fragment has at least 400 rows, so a hit set of
     *       it takes the hit rows; 625 is 1 modulo 3 and 1 modulo 4, so
     *       {@code category} and its nulls cycle through the rows of one
     *       group), {@code hello} every row.</li>
     *   <li>{@code rating}: int32, {@code (i * 37) % 1000}, distinct for
     *       {@code i < 1000} so a sort on it has no ties; Arrow null when
     *       {@code i % 5 == 4}</li>
     *   <li>{@code category}: Utf8 without an index (derives to
     *       {@code keyword}), {@code "c" + (i % 3)}; Arrow null when
     *       {@code i % 4 == 3}</li>
     *   <li>{@code tags}: List&lt;Utf8&gt; (derives to multi-valued
     *       {@code keyword}), {@code ["t" + (i % 2), "t" + (i % 5)]} so
     *       rows with {@code i % 10 == 0} or {@code i % 10 == 1} carry a
     *       duplicate element; Arrow null when {@code i % 6 == 5}</li>
     *   <li>{@code flag}: bool, {@code i % 2 == 0}; Arrow null when
     *       {@code i % 7 == 6}</li>
     *   <li>{@code embedding}: FixedSizeList&lt;Float32, 8&gt; with
     *       {@code embedding[0] = i} and zeros elsewhere, so the
     *       nearest neighbours of {@code (x, 0, ...)} are the rows whose
     *       id is closest to {@code x}</li>
     * </ul>
     * The first fragment is written with {@code CREATE}, the rest with
     * {@code APPEND}; the FTS index is built last so it covers every
     * fragment. Public because the engine package's unit tests and the
     * REST ITs both need this layout.
     *
     * @return absolute URI of the table.
     */
    public static String writeHintFixtureTable(Path parent, String name, int fragments, int rowsPerFragment) throws Exception {
        return withLocaleRoot(() -> writeHintFixtureTableOnce(parent, name, fragments, rowsPerFragment));
    }

    /**
     * The {@link #writeHintFixtureTable} layout with one index of every
     * kind the plugin's index warm-up knows a scan for: the inverted
     * index on {@code body} the hint fixture already builds, a BTree on
     * {@code rating}, a bitmap on {@code category} and an IVF_PQ vector
     * index on {@code embedding}. IVF_PQ trains on at least 256 rows, so
     * {@code fragments * rowsPerFragment} must reach that.
     *
     * @return absolute URI of the table.
     */
    public static String writeIndexedFixtureTable(Path parent, String name, int fragments, int rowsPerFragment) throws Exception {
        return withLocaleRoot(() -> {
            String uri = writeHintFixtureTableOnce(parent, name, fragments, rowsPerFragment);
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open().allocator(allocator).uri(uri).build()
            ) {
                dataset.createIndex(
                    IndexOptions.builder(
                        Collections.singletonList("rating"),
                        IndexType.BTREE,
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build()
                    ).withIndexName("rating_btree").build()
                );
                dataset.createIndex(
                    IndexOptions.builder(
                        Collections.singletonList("category"),
                        IndexType.BITMAP,
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("bitmap")).build()
                    ).withIndexName("category_bitmap").build()
                );
                VectorIndexParams ivfPq = VectorIndexParams.ivfPq(1, 8, 8, DistanceType.L2, 20);
                dataset.createIndex(
                    IndexOptions.builder(
                        Collections.singletonList(VECTOR_COLUMN),
                        IndexType.IVF_PQ,
                        IndexParams.builder().setVectorIndexParams(ivfPq).build()
                    ).withIndexName(VECTOR_COLUMN + "_ivf").train(true).build()
                );
            }
            return uri;
        });
    }

    private static String writeHintFixtureTableOnce(Path parent, String name, int fragments, int rowsPerFragment) throws Exception {
        Path tablePath = parent.resolve(name + ".lance");
        String uri = tablePath.toString();
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field(BODY_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("rating", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field(
                    "tags",
                    FieldType.nullable(new ArrowType.List()),
                    Collections.singletonList(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null))
                ),
                new Field("flag", FieldType.nullable(new ArrowType.Bool()), null),
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

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            for (int fragment = 0; fragment < fragments; fragment++) {
                byte[] ipcBytes;
                try (
                    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    ByteArrayOutputStream out = new ByteArrayOutputStream()
                ) {
                    IntVector idVector = (IntVector) root.getVector("id");
                    VarCharVector bodyVector = (VarCharVector) root.getVector(BODY_COLUMN);
                    IntVector ratingVector = (IntVector) root.getVector("rating");
                    VarCharVector categoryVector = (VarCharVector) root.getVector("category");
                    ListVector tagsVector = (ListVector) root.getVector("tags");
                    VarCharVector tagItems = (VarCharVector) tagsVector.getDataVector();
                    BitVector flagVector = (BitVector) root.getVector("flag");
                    FixedSizeListVector vecVector = (FixedSizeListVector) root.getVector(VECTOR_COLUMN);
                    Float4Vector vecItems = (Float4Vector) vecVector.getDataVector();
                    idVector.allocateNew(rowsPerFragment);
                    bodyVector.allocateNew();
                    ratingVector.allocateNew(rowsPerFragment);
                    categoryVector.allocateNew();
                    tagsVector.allocateNew();
                    flagVector.allocateNew(rowsPerFragment);
                    vecVector.allocateNew();
                    vecItems.allocateNew(rowsPerFragment * VECTOR_DIM);
                    int tagCount = 0;
                    for (int slot = 0; slot < rowsPerFragment; slot++) {
                        int i = fragment * rowsPerFragment + slot;
                        idVector.set(slot, i);
                        String body = "hello tok" + i + " grp" + (i % 25) + " sp" + (i % 625) + " lance".repeat((i % 5) + 1);
                        bodyVector.setSafe(slot, body.getBytes(StandardCharsets.UTF_8));
                        if (i % 5 == 4) {
                            ratingVector.setNull(slot);
                        } else {
                            ratingVector.set(slot, (i * 37) % 1000);
                        }
                        if (i % 4 == 3) {
                            categoryVector.setNull(slot);
                        } else {
                            categoryVector.setSafe(slot, ("c" + (i % 3)).getBytes(StandardCharsets.UTF_8));
                        }
                        if (i % 6 == 5) {
                            tagsVector.setNull(slot);
                        } else {
                            tagsVector.startNewValue(slot);
                            tagItems.setSafe(tagCount++, ("t" + (i % 2)).getBytes(StandardCharsets.UTF_8));
                            tagItems.setSafe(tagCount++, ("t" + (i % 5)).getBytes(StandardCharsets.UTF_8));
                            tagsVector.endValue(slot, 2);
                        }
                        if (i % 7 == 6) {
                            flagVector.setNull(slot);
                        } else {
                            flagVector.set(slot, i % 2 == 0 ? 1 : 0);
                        }
                        for (int j = 0; j < VECTOR_DIM; j++) {
                            vecItems.set(slot * VECTOR_DIM + j, j == 0 ? (float) i : 0.0f);
                        }
                        vecVector.setNotNull(slot);
                    }
                    idVector.setValueCount(rowsPerFragment);
                    bodyVector.setValueCount(rowsPerFragment);
                    ratingVector.setValueCount(rowsPerFragment);
                    categoryVector.setValueCount(rowsPerFragment);
                    tagItems.setValueCount(tagCount);
                    tagsVector.setValueCount(rowsPerFragment);
                    flagVector.setValueCount(rowsPerFragment);
                    vecItems.setValueCount(rowsPerFragment * VECTOR_DIM);
                    vecVector.setValueCount(rowsPerFragment);
                    root.setRowCount(rowsPerFragment);
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
                    WriteParams.WriteMode mode = fragment == 0 ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.APPEND;
                    WriteParams writeParams = new WriteParams.Builder().withMode(mode).build();
                    Dataset.create(allocator, stream, uri, writeParams).close();
                }
            }
            try (Dataset dataset = Dataset.open().allocator(allocator).uri(uri).build()) {
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
        return uri;
    }

    /**
     * Pins the JVM's default {@link Locale} to {@link Locale#ROOT} for the
     * duration of a Lance write, restoring the previous default afterwards.
     *
     * <p>Arrow Java's C Data bridge ({@code org.apache.arrow.c.Format#asString})
     * renders the {@code FixedSizeList} width with {@code String.format("%d")}
     * and no explicit locale, so under a default locale whose zero digit is
     * not ASCII (Arabic-Indic, Bengali, Devanagari and others) the schema
     * string carries localised digits that arrow-rs cannot parse, and
     * {@code Dataset.create} fails with "The FixedSizeList type requires an
     * integer parameter". The Lucene test framework randomises the default
     * locale per test class, so the failure is seed-dependent. The pin is
     * scoped to the write so the rest of the test keeps the randomised
     * locale. Every fixture is wrapped so a schema that later gains a
     * {@code FixedSizeList}, {@code FixedSizeBinary} or {@code Decimal}
     * column inherits the fix. Can be removed once arrow-java formats those
     * widths locale-independently.
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
