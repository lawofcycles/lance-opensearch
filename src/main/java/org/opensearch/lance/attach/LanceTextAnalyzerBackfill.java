/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.engine.LanceIndexBuilder;

/**
 * The write side of the {@code type: text_analyzer} mapping override
 * (the RFC's second text mode): for every overridden Utf8 column the
 * plugin adds a derived Utf8 column to the Lance table, backfills it
 * with the column's values run through the declared OpenSearch
 * analyzer (tokens joined by single spaces), and builds a whitespace
 * inverted index over the derived column with every default
 * transformation of Lance's inverted index turned off (see
 * {@link LanceIndexBuilder#ensureVerbatimWhitespaceFtsIndexes}), so
 * the analyzer's tokens are indexed and queried exactly as produced.
 * Query time re-applies the same analyzer to the query text, so the
 * index-time and query-time tokenisation agree.
 *
 * <p>The backfill is one {@code AddColumns} commit: the source column
 * is scanned in fragment order (live rows only), each value is
 * tokenized on the Java side, the token strings are spooled to a
 * temporary Arrow IPC file under {@code java.io.tmpdir}, and
 * {@link Dataset#addColumns(ArrowArrayStream, Optional)} consumes the
 * spool. The spool keeps the memory footprint at one batch regardless
 * of table size and keeps the native {@code add_columns} from pulling
 * batches out of a live scanner of the same dataset. An interrupted
 * backfill leaves no derived column (the commit is atomic), so
 * re-running the attach retries it from the start.
 *
 * <p>Idempotent: a derived column that already exists (with a Utf8
 * type) is left alone, and the FTS build skips a derived column that
 * already carries an inverted index, so re-attaching a table is safe.
 */
public final class LanceTextAnalyzerBackfill {

    private static final Logger LOG = LogManager.getLogger(LanceTextAnalyzerBackfill.class);

    private LanceTextAnalyzerBackfill() {}

    /** The per-column outcome of one {@link #ensureDerivedColumns} pass. */
    public record Ensured(List<String> created, List<String> existing) {
    }

    /**
     * Make every {@code type: text_analyzer} override of
     * {@code textAnalyzerColumns} servable on {@code dataset}: create
     * and backfill each missing derived column, then make sure each
     * derived column carries a whitespace inverted index (with
     * positions, so phrase queries work).
     *
     * @param dataset the table, opened at its latest version; the
     *     backfill commits advance it in place
     * @param textAnalyzerColumns the overrides, keyed by base column
     *     (see {@link LanceOverrides#textAnalyzerColumns()})
     * @param analyzers resolves a base column's declared analyzer name
     *     to the {@link Analyzer} to run; the caller has already
     *     validated the names
     * @param allocator the Arrow allocator for the scan and the spool
     * @return which derived columns were created and which already
     *     existed
     * @throws IllegalArgumentException when a base column is missing or
     *     not Utf8, or a declared derived column name collides with an
     *     existing non-Utf8 column; the caller answers 400
     */
    public static Ensured ensureDerivedColumns(
        Dataset dataset,
        Map<String, LanceOverrides.Column> textAnalyzerColumns,
        Function<String, Analyzer> analyzers,
        BufferAllocator allocator
    ) throws Exception {
        List<String> created = new ArrayList<>();
        List<String> existing = new ArrayList<>();
        Set<String> derivedColumns = new LinkedHashSet<>();
        for (Map.Entry<String, LanceOverrides.Column> entry : textAnalyzerColumns.entrySet()) {
            String base = entry.getKey();
            String derived = LanceOverrides.derivedColumnName(base, entry.getValue());
            Map<String, LanceField> fields = fieldsByName(dataset.getLanceSchema());
            LanceField baseField = fields.get(base);
            if (baseField == null) {
                throw new IllegalArgumentException("[overrides] references unknown column [" + base + "]");
            }
            if (!(baseField.getType() instanceof ArrowType.Utf8)) {
                throw new IllegalArgumentException(
                    "[overrides." + base + ".type=text_analyzer] needs a Utf8 column; [" + base + "] is " + baseField.getType()
                );
            }
            LanceField derivedField = fields.get(derived);
            if (derivedField != null) {
                if (!(derivedField.getType() instanceof ArrowType.Utf8)) {
                    throw new IllegalArgumentException(
                        "[overrides."
                            + base
                            + "] derived column ["
                            + derived
                            + "] exists with Arrow type "
                            + derivedField.getType()
                            + "; the analyzer mode needs a Utf8 tokens column. Declare a different [derived_column_name]"
                    );
                }
                existing.add(derived);
            } else {
                long started = System.nanoTime();
                backfill(dataset, base, derived, analyzers.apply(base), allocator);
                LOG.info(
                    "backfilled derived tokens column {} from {} ({} rows, {} ms, version {})",
                    derived,
                    base,
                    dataset.countRows(),
                    (System.nanoTime() - started) / 1_000_000L,
                    dataset.version()
                );
                created.add(derived);
            }
            derivedColumns.add(derived);
        }
        LanceIndexBuilder.BuildResult ftsBuild = LanceIndexBuilder.ensureVerbatimWhitespaceFtsIndexes(dataset, derivedColumns);
        if (!ftsBuild.failed().isEmpty()) {
            LanceIndexBuilder.Failed first = ftsBuild.failed().get(0);
            String message = "whitespace FTS build on derived column [" + first.column() + "] failed: " + first.reason();
            if (first.invalidInput()) {
                throw new IllegalArgumentException(message);
            }
            throw new IllegalStateException(message);
        }
        return new Ensured(created, existing);
    }

    /**
     * Scan {@code source}, run every value through {@code analyzer},
     * and commit the space-joined token strings as the new Utf8 column
     * {@code derived}. Null source values stay null. One AddColumns
     * commit; the dataset serves the new version afterwards.
     */
    static void backfill(Dataset dataset, String source, String derived, Analyzer analyzer, BufferAllocator allocator) throws Exception {
        // The plugin's security policy grants read / write / delete under
        // java.io.tmpdir (the same location lance-jni extracts its native
        // library to), so the spool goes there explicitly.
        Path spool = Files.createTempFile(Path.of(System.getProperty("java.io.tmpdir")), "lance-" + derived + "-", ".arrows");
        try {
            Schema derivedSchema = new Schema(Collections.singletonList(Field.nullable(derived, ArrowType.Utf8.INSTANCE)));
            try (
                VectorSchemaRoot out = VectorSchemaRoot.create(derivedSchema, allocator);
                OutputStream os = Files.newOutputStream(spool);
                ArrowStreamWriter writer = new ArrowStreamWriter(out, null, os)
            ) {
                writer.start();
                ScanOptions options = new ScanOptions.Builder().columns(Collections.singletonList(source)).build();
                try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot in = reader.getVectorSchemaRoot();
                        VarCharVector src = (VarCharVector) in.getVector(source);
                        int rows = in.getRowCount();
                        out.allocateNew();
                        VarCharVector dst = (VarCharVector) out.getVector(0);
                        for (int i = 0; i < rows; i++) {
                            if (src.isNull(i)) {
                                dst.setNull(i);
                            } else {
                                String text = new String(src.get(i), StandardCharsets.UTF_8);
                                dst.setSafe(i, joinTokens(analyzer, source, text).getBytes(StandardCharsets.UTF_8));
                            }
                        }
                        dst.setValueCount(rows);
                        out.setRowCount(rows);
                        writer.writeBatch();
                    }
                }
                writer.end();
            }
            try (
                InputStream is = Files.newInputStream(spool);
                ArrowStreamReader reader = new ArrowStreamReader(is, allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                dataset.addColumns(stream, Optional.empty());
            }
        } finally {
            Files.deleteIfExists(spool);
        }
    }

    /**
     * The analyzer's tokens of {@code text}, joined by single spaces:
     * the string the backfill stores and the query side matches
     * against. An empty token stream yields an empty string (the value
     * existed; it matches nothing).
     */
    public static String joinTokens(Analyzer analyzer, String field, String text) throws IOException {
        StringBuilder joined = new StringBuilder();
        try (TokenStream stream = analyzer.tokenStream(field, text)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                if (joined.length() > 0) {
                    joined.append(' ');
                }
                joined.append(term.buffer(), 0, term.length());
            }
            stream.end();
        }
        return joined.toString();
    }

    private static Map<String, LanceField> fieldsByName(LanceSchema schema) {
        Map<String, LanceField> fields = new LinkedHashMap<>();
        for (LanceField field : schema.fields()) {
            fields.put(field.getName(), field);
        }
        return fields;
    }
}
