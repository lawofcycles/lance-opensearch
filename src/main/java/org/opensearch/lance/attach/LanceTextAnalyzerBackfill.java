/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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
 * <p>The backfill is one {@code AddColumns} commit fed by a streaming
 * producer: {@link Dataset#addColumns(ArrowArrayStream, Optional)}
 * pulls the derived column batch by batch from a {@link TokenizingReader}
 * that scans the source column in fragment order (live rows only) and
 * tokenizes each batch on a bounded number of executor threads while
 * emitting the results in source order. Nothing is written to a local
 * disk: the derived column goes straight into the table's data files,
 * and the memory footprint is bounded by the batches in flight
 * ({@code 2 * threads} source batches of {@link #BATCH_ROWS} rows plus
 * their token strings). The scan and the {@code add_columns} run on
 * the same {@link Dataset} handle: the scanner and its Arrow stream
 * are opened before {@code addColumns} takes the dataset's write lock
 * and the native dataset mutex, and pulling a scan batch afterwards
 * touches neither, so the two proceed concurrently without a
 * deadlock. An interrupted or failed backfill leaves no derived column
 * (Lance removes the partial data files and commits nothing), so
 * re-running the attach retries it from the start.
 *
 * <p>Idempotent: a derived column that already exists (with a Utf8
 * type) is left alone, and the FTS build skips a derived column that
 * already carries an inverted index, so re-attaching a table is safe.
 */
public final class LanceTextAnalyzerBackfill {

    private static final Logger LOG = LogManager.getLogger(LanceTextAnalyzerBackfill.class);

    /** Rows per source batch: the unit of parallel tokenisation and of the memory bound. */
    static final int BATCH_ROWS = 4096;

    /** Rows sampled from the head of the source column for {@link #estimateDerivedBytes}. */
    static final int SAMPLE_ROWS = 4096;

    /**
     * Safety factor on the sampled average value length: the derived
     * column is about the size of the source column (stemming shortens
     * tokens, stop word removal drops some, the joining spaces add
     * back), so the estimate leans high rather than low.
     */
    static final double ESTIMATE_FACTOR = 1.2d;

    private LanceTextAnalyzerBackfill() {}

    /** The per-column outcome of one {@link #ensureDerivedColumns} pass. */
    public record Ensured(List<String> created, List<String> existing) {
    }

    /**
     * How one backfill run tokenizes: {@code executor} runs the per
     * batch analysis tasks, at most {@code threads} of them at a time;
     * {@code cancelled} is polled before every batch and stops the
     * backfill (the pending {@code addColumns} fails and commits
     * nothing) when it answers {@code true}.
     */
    public record Options(Executor executor, int threads, BooleanSupplier cancelled) {
        public Options {
            if (threads < 1) {
                throw new IllegalArgumentException("threads must be at least 1, was " + threads);
            }
        }

        /** Tokenizes on the calling thread, one batch at a time, never cancelled. */
        public static Options inline() {
            return new Options(Runnable::run, 1, () -> false);
        }
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
     * @param allocator the Arrow allocator for the scan and the
     *     produced batches
     * @param options the executor, thread bound and cancellation hook
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
        BufferAllocator allocator,
        Options options
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
                backfill(dataset, base, derived, analyzers.apply(base), allocator, options);
                LOG.info(
                    "backfilled derived tokens column {} from {} ({} rows, {} ms, {} threads, version {})",
                    derived,
                    base,
                    dataset.countRows(),
                    (System.nanoTime() - started) / 1_000_000L,
                    options.threads(),
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
     * The derived columns of {@code textAnalyzerColumns} that
     * {@code dataset} does not carry yet, keyed by base column: what a
     * backfill of this table would write. Validation of the base
     * columns is left to {@link #ensureDerivedColumns}.
     */
    public static Map<String, String> missingDerivedColumns(Dataset dataset, Map<String, LanceOverrides.Column> textAnalyzerColumns) {
        Map<String, LanceField> fields = fieldsByName(dataset.getLanceSchema());
        Map<String, String> missing = new LinkedHashMap<>();
        for (Map.Entry<String, LanceOverrides.Column> entry : textAnalyzerColumns.entrySet()) {
            String derived = LanceOverrides.derivedColumnName(entry.getKey(), entry.getValue());
            if (!fields.containsKey(derived)) {
                missing.put(entry.getKey(), derived);
            }
        }
        return missing;
    }

    /**
     * About how many bytes the derived tokens column of {@code source}
     * adds to the table: the average UTF-8 length of the first
     * {@link #SAMPLE_ROWS} live values (nulls count as zero) times the
     * row count times {@link #ESTIMATE_FACTOR}. A column that is not
     * Utf8 or a table without rows estimates to zero; the backfill
     * itself refuses the former.
     */
    public static long estimateDerivedBytes(Dataset dataset, String source) throws Exception {
        long rows = dataset.countRows();
        if (rows == 0) {
            return 0L;
        }
        long sampledRows = 0;
        long sampledBytes = 0;
        ScanOptions options = new ScanOptions.Builder().columns(Collections.singletonList(source))
            .limit(SAMPLE_ROWS)
            .batchSize(SAMPLE_ROWS)
            .build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot in = reader.getVectorSchemaRoot();
                if (!(in.getVector(source) instanceof VarCharVector src)) {
                    return 0L;
                }
                int count = in.getRowCount();
                for (int i = 0; i < count; i++) {
                    if (!src.isNull(i)) {
                        sampledBytes += src.getEndOffset(i) - src.getStartOffset(i);
                    }
                }
                sampledRows += count;
            }
        }
        return estimateDerivedBytes(rows, sampledRows, sampledBytes);
    }

    /** The arithmetic of {@link #estimateDerivedBytes(Dataset, String)}, exposed for tests. */
    static long estimateDerivedBytes(long rows, long sampledRows, long sampledBytes) {
        if (rows <= 0 || sampledRows <= 0) {
            return 0L;
        }
        double average = (double) sampledBytes / (double) sampledRows;
        return (long) Math.ceil(average * (double) rows * ESTIMATE_FACTOR);
    }

    /**
     * Scan {@code source}, run every value through {@code analyzer},
     * and commit the space-joined token strings as the new Utf8 column
     * {@code derived}. Null source values stay null. One AddColumns
     * commit; the dataset serves the new version afterwards.
     */
    static void backfill(Dataset dataset, String source, String derived, Analyzer analyzer, BufferAllocator allocator, Options options)
        throws Exception {
        ScanOptions scan = new ScanOptions.Builder().columns(Collections.singletonList(source))
            .batchSize(BATCH_ROWS)
            .scanInOrder(true)
            .build();
        // The scanner and its stream are opened before addColumns so
        // that no dataset lock is taken while addColumns holds the
        // write lock; see the class Javadoc.
        try (
            LanceScanner scanner = dataset.newScan(scan);
            ArrowReader sourceReader = scanner.scanBatches();
            TokenizingReader producer = new TokenizingReader(sourceReader, source, derived, analyzer, allocator, options);
            ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
        ) {
            Data.exportArrayStream(allocator, producer, stream);
            try {
                dataset.addColumns(stream, Optional.empty());
            } catch (RuntimeException e) {
                // The native consumer relays a producer failure as a
                // generic stream error; raise the producer's own
                // exception when there is one so the caller sees the
                // real cause (the cancel, the analyzer failure).
                producer.rethrowFailure(e);
                throw e;
            }
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

    /**
     * The streaming producer {@code addColumns} consumes: each
     * {@link #loadNextBatch} tops up the in flight window with source
     * batches (copied out of the scan so the scan's buffers can be
     * reused), each tokenized as one task on the executor with at most
     * {@code threads} running at once, then emits the oldest batch's
     * tokens into the derived column vector. The window is a FIFO, so
     * the derived rows come out in the order the scan produced the
     * source rows, which is the order {@code addColumns} assigns them
     * to fragments. {@code null} source values stay {@code null}.
     *
     * <p>A failure of the scan, of a tokenisation task or a cancel
     * surfaces from {@link #loadNextBatch} as an {@link IOException}:
     * the Arrow C stream reports it to the native consumer, which fails
     * the {@code addColumns} call, and {@link #rethrowFailure} raises
     * the original cause to the caller.
     */
    static final class TokenizingReader extends ArrowReader {

        private final ArrowReader source;
        private final String sourceColumn;
        private final Schema derivedSchema;
        private final Analyzer analyzer;
        private final Executor executor;
        private final Semaphore permits;
        private final int maxInFlight;
        private final BooleanSupplier cancelled;
        private final ArrayDeque<CompletableFuture<byte[][]>> inFlight = new ArrayDeque<>();
        private boolean sourceExhausted;
        private long rowsEmitted;
        private volatile Exception failure;
        // Runs on the task thread between completing a batch's future
        // and releasing its permit; a test hook to hold that gap open.
        private volatile Runnable beforePermitRelease = () -> {};

        TokenizingReader(
            ArrowReader source,
            String sourceColumn,
            String derivedColumn,
            Analyzer analyzer,
            BufferAllocator allocator,
            Options options
        ) {
            super(allocator);
            this.source = source;
            this.sourceColumn = sourceColumn;
            this.derivedSchema = new Schema(Collections.singletonList(Field.nullable(derivedColumn, ArrowType.Utf8.INSTANCE)));
            this.analyzer = analyzer;
            this.executor = options.executor();
            this.permits = new Semaphore(options.threads());
            this.maxInFlight = 2 * options.threads();
            this.cancelled = options.cancelled();
        }

        /** Rows handed to the consumer so far. */
        long rowsEmitted() {
            return rowsEmitted;
        }

        /**
         * Test hook: {@code hook} runs on the task thread after a
         * batch's future is completed and before its permit is
         * released, so a test can hold the permit while the producer
         * observes an empty window. Set before the first batch is read.
         */
        void beforePermitRelease(Runnable hook) {
            this.beforePermitRelease = hook;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            try {
                if (cancelled.getAsBoolean()) {
                    throw new IOException("text_analyzer backfill cancelled after " + rowsEmitted + " rows");
                }
                topUp();
                CompletableFuture<byte[][]> head = inFlight.poll();
                if (head == null) {
                    return false;
                }
                byte[][] tokens;
                try {
                    tokens = head.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    throw cause instanceof IOException io ? io : new IOException("tokenizing a source batch failed", cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("text_analyzer backfill interrupted", e);
                }
                VectorSchemaRoot out = getVectorSchemaRoot();
                out.allocateNew();
                VarCharVector dst = (VarCharVector) out.getVector(0);
                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i] == null) {
                        dst.setNull(i);
                    } else {
                        dst.setSafe(i, tokens[i]);
                    }
                }
                dst.setValueCount(tokens.length);
                out.setRowCount(tokens.length);
                rowsEmitted += tokens.length;
                return true;
            } catch (IOException | RuntimeException e) {
                // Remembered so the caller can raise the real cause; the
                // native consumer only relays a generic stream error.
                if (failure == null) {
                    failure = e;
                }
                throw e;
            }
        }

        /**
         * Read source batches and submit their tokenisation until the
         * window is full, the scan is exhausted, or every permit is
         * taken. A permit is held from submit until the task completes
         * (released in the task itself, after the future is completed),
         * so no more than {@code threads} tokenise at once. When the
         * window is empty the producer must have at least one batch to
         * hand out, so it waits for a permit rather than trying: the
         * permit of the batch just emitted is released moments after
         * its future completed, and a {@code tryAcquire} in that gap
         * would look like the end of the stream.
         */
        private void topUp() throws IOException {
            while (!sourceExhausted && inFlight.size() < maxInFlight) {
                if (inFlight.isEmpty()) {
                    try {
                        permits.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("text_analyzer backfill interrupted", e);
                    }
                } else if (!permits.tryAcquire()) {
                    return;
                }
                boolean submitted = false;
                try {
                    if (!source.loadNextBatch()) {
                        sourceExhausted = true;
                        return;
                    }
                    byte[][] values = copyValues(source.getVectorSchemaRoot());
                    CompletableFuture<byte[][]> future = new CompletableFuture<>();
                    executor.execute(() -> {
                        try {
                            future.complete(tokenize(values));
                            beforePermitRelease.run();
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        } finally {
                            permits.release();
                        }
                    });
                    submitted = true;
                    inFlight.add(future);
                } finally {
                    if (!submitted) {
                        permits.release();
                    }
                }
            }
        }

        private byte[][] copyValues(VectorSchemaRoot in) {
            VarCharVector src = (VarCharVector) in.getVector(sourceColumn);
            int rows = in.getRowCount();
            byte[][] values = new byte[rows][];
            for (int i = 0; i < rows; i++) {
                values[i] = src.isNull(i) ? null : src.get(i);
            }
            return values;
        }

        private byte[][] tokenize(byte[][] values) throws IOException {
            byte[][] tokens = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    String text = new String(values[i], StandardCharsets.UTF_8);
                    tokens[i] = joinTokens(analyzer, sourceColumn, text).getBytes(StandardCharsets.UTF_8);
                }
            }
            return tokens;
        }

        /**
         * Raise the exception a {@link #loadNextBatch} failed with, if
         * any, with {@code consumerFailure} (what {@code addColumns}
         * threw) attached as suppressed; return when the producer did
         * not fail, so the caller rethrows the consumer's exception.
         */
        void rethrowFailure(Exception consumerFailure) throws Exception {
            Exception cause = failure;
            if (cause != null) {
                cause.addSuppressed(consumerFailure);
                throw cause;
            }
        }

        @Override
        public long bytesRead() {
            return 0L;
        }

        @Override
        protected void closeReadSource() throws IOException {
            // Tasks still running hold only copies of the source rows;
            // let them finish and drop their results.
            inFlight.clear();
        }

        @Override
        protected Schema readSchema() {
            return derivedSchema;
        }
    }

    private static Map<String, LanceField> fieldsByName(LanceSchema schema) {
        Map<String, LanceField> fields = new LinkedHashMap<>();
        for (LanceField field : schema.fields()) {
            fields.put(field.getName(), field);
        }
        return fields;
    }
}
