/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.index.IndexCriteria;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * The text_analyzer backfill helper: token generation through a real
 * Lucene analyzer, the AddColumns commit that materialises the derived
 * column (null values stay null, values land in row order), the
 * whitespace FTS index over it, and the idempotency of a second pass.
 * Also the streaming producer: a multi million row table backfills
 * without a spool file, the parallel tokenisation emits the same
 * column as the single threaded one, a cancel commits nothing, and
 * the size estimate.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceTextAnalyzerBackfillTests extends OpenSearchTestCase {

    private static final String[] WORDS = {
        "the",
        "dogs",
        "are",
        "running",
        "quickly",
        "through",
        "park",
        "a",
        "dog",
        "runs",
        "across",
        "wide",
        "field",
        "cats",
        "sleep",
        "all",
        "day",
        "on",
        "warm",
        "windowsill",
        "he",
        "ran",
        "to",
        "store",
        "before",
        "it",
        "closed",
        "runners",
        "run",
        "marathon",
        "in",
        "morning",
        "houses",
        "painted",
        "brightly",
        "children",
        "played",
        "games",
        "until",
        "evening" };

    public void testJoinTokensAppliesAnalyzer() throws Exception {
        try (Analyzer english = new EnglishAnalyzer()) {
            // The english analyzer drops stop words, lowercases and stems.
            assertEquals("dog run quickli", LanceTextAnalyzerBackfill.joinTokens(english, "body", "The dogs are running quickly"));
            assertEquals("", LanceTextAnalyzerBackfill.joinTokens(english, "body", "the and of"));
        }
        try (Analyzer whitespace = new WhitespaceAnalyzer()) {
            // The whitespace analyzer splits only; case is kept.
            assertEquals("Cats sleep", LanceTextAnalyzerBackfill.joinTokens(whitespace, "body", "Cats  sleep"));
        }
    }

    public void testEnsureDerivedColumnsBackfillsAndIndexes() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            long versionBefore = dataset.version();
            LanceTextAnalyzerBackfill.Ensured ensured = LanceTextAnalyzerBackfill.ensureDerivedColumns(
                dataset,
                overrides,
                name -> english,
                LanceRegistry.allocator(),
                LanceTextAnalyzerBackfill.Options.inline()
            );
            assertEquals(List.of("body__lance_tokens"), ensured.created());
            assertTrue(ensured.existing().isEmpty());
            assertTrue("AddColumns must commit a new version", dataset.version() > versionBefore);

            // The schema now carries the derived Utf8 column.
            LanceField derived = null;
            for (LanceField field : dataset.getLanceSchema().fields()) {
                if (field.getName().equals("body__lance_tokens")) {
                    derived = field;
                }
            }
            assertNotNull("derived column must exist", derived);
            assertTrue(derived.getType() instanceof ArrowType.Utf8);

            // Values land in row order: id 0 gets the analyzed tokens of
            // sentence 0, the Arrow-null row stays null.
            String[] tokens = readDerivedByRow(dataset, "body__lance_tokens", LanceTableFactory.ENGLISH_SENTENCES.length);
            assertEquals("dog run quickli through park", tokens[0]);
            assertEquals("dog run across wide field", tokens[1]);
            assertNull("null source row must stay null", tokens[4]);
            assertEquals("runner run marathon morn", tokens[5]);

            // The derived column carries an FTS-capable index.
            assertFalse(
                "derived column must carry an inverted index",
                dataset.describeIndices(new IndexCriteria.Builder().forColumn("body__lance_tokens").mustSupportFts(true).build()).isEmpty()
            );

            // A second pass is a no-op: the column and its index exist.
            LanceTextAnalyzerBackfill.Ensured again = LanceTextAnalyzerBackfill.ensureDerivedColumns(
                dataset,
                overrides,
                name -> english,
                LanceRegistry.allocator(),
                LanceTextAnalyzerBackfill.Options.inline()
            );
            assertTrue(again.created().isEmpty());
            assertEquals(List.of("body__lance_tokens"), again.existing());
        }
    }

    public void testEnsureDerivedColumnsRefusesUnknownBaseColumn() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("missing", Map.of("type", "text_analyzer", "analyzer", "english")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> LanceTextAnalyzerBackfill.ensureDerivedColumns(
                    dataset,
                    overrides,
                    name -> english,
                    LanceRegistry.allocator(),
                    LanceTextAnalyzerBackfill.Options.inline()
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unknown column [missing]"));
        }
    }

    public void testEnsureDerivedColumnsRefusesNonUtf8Collision() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "backfill-" + getTestName().toLowerCase(Locale.ROOT));
        // derived_column_name points at the Int32 id column.
        Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "derived_column_name", "id")),
            null
        ).textAnalyzerColumns();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> LanceTextAnalyzerBackfill.ensureDerivedColumns(
                    dataset,
                    overrides,
                    name -> english,
                    LanceRegistry.allocator(),
                    LanceTextAnalyzerBackfill.Options.inline()
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("exists with Arrow type"));
        }
    }

    /**
     * The streaming producer on a table of a few million rows across
     * many fragments: the backfill on one {@link Dataset} handle
     * completes (the scan and {@code addColumns} do not deadlock on the
     * dataset lock or the native mutex), writes no file under
     * {@code java.io.tmpdir} while it runs, and lands every row's
     * tokens in row order.
     */
    public void testStreamingBackfillOfMillionsOfRowsWritesNoSpool() throws Exception {
        int rows = 2_000_000;
        Path scratchDir = createTempDir();
        String uri = writeGeneratedTextTable(scratchDir, "large-" + getTestName().toLowerCase(Locale.ROOT), rows, 250_000);
        Path tmpdir = Path.of(System.getProperty("java.io.tmpdir"));
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            Set<Path> before = listTmpdir(tmpdir);
            long versionBefore = dataset.version();
            int threads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
            long started = System.nanoTime();
            LanceTextAnalyzerBackfill.backfill(
                dataset,
                "body",
                "body__lance_tokens",
                english,
                LanceRegistry.allocator(),
                new LanceTextAnalyzerBackfill.Options(threadPool.generic(), threads, () -> false)
            );
            logger.info("streamed backfill of {} rows on {} threads in {} ms", rows, threads, (System.nanoTime() - started) / 1_000_000L);
            assertEquals("one AddColumns commit", versionBefore + 1, dataset.version());
            Set<Path> after = listTmpdir(tmpdir);
            after.removeAll(before);
            assertTrue("the backfill must not write a file under java.io.tmpdir, found " + after, after.isEmpty());
            assertEquals(rows, dataset.countRows());

            // Every row's tokens are those of its own sentence.
            int checked = 0;
            ScanOptions options = new ScanOptions.Builder().columns(new ArrayList<>(Arrays.asList("id", "body__lance_tokens"))).build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    IntVector ids = (IntVector) root.getVector("id");
                    VarCharVector tokens = (VarCharVector) root.getVector("body__lance_tokens");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int id = ids.get(i);
                        String expected = sentenceOf(id) == null
                            ? null
                            : LanceTextAnalyzerBackfill.joinTokens(english, "body", sentenceOf(id));
                        String actual = tokens.isNull(i) ? null : new String(tokens.get(i), StandardCharsets.UTF_8);
                        assertEquals("row " + id, expected, actual);
                        checked++;
                    }
                }
            }
            assertEquals(rows, checked);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * The parallel producer emits exactly the column the single
     * threaded one does: both backfills on the same table, compared
     * row by row.
     */
    public void testParallelBackfillMatchesSingleThreaded() throws Exception {
        int rows = 300_000;
        Path scratchDir = createTempDir();
        String uri = writeGeneratedTextTable(scratchDir, "parallel-" + getTestName().toLowerCase(Locale.ROOT), rows, 50_000);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            LanceTextAnalyzerBackfill.backfill(
                dataset,
                "body",
                "single",
                english,
                LanceRegistry.allocator(),
                LanceTextAnalyzerBackfill.Options.inline()
            );
            LanceTextAnalyzerBackfill.backfill(
                dataset,
                "body",
                "parallel",
                english,
                LanceRegistry.allocator(),
                new LanceTextAnalyzerBackfill.Options(threadPool.generic(), 7, () -> false)
            );
            // One permit on a real pool, end to end through AddColumns
            // (which refuses a stream that ends before every row has a
            // value). The race this exercises by chance is forced on
            // every run by testEmptyWindowWaitsForTheUnreleasedPermit.
            LanceTextAnalyzerBackfill.backfill(
                dataset,
                "body",
                "one_thread",
                english,
                LanceRegistry.allocator(),
                new LanceTextAnalyzerBackfill.Options(threadPool.generic(), 1, () -> false)
            );
            int compared = 0;
            ScanOptions options = new ScanOptions.Builder().columns(new ArrayList<>(Arrays.asList("single", "parallel", "one_thread")))
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    VarCharVector single = (VarCharVector) root.getVector("single");
                    VarCharVector parallel = (VarCharVector) root.getVector("parallel");
                    VarCharVector oneThread = (VarCharVector) root.getVector("one_thread");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        assertEquals(single.isNull(i), parallel.isNull(i));
                        assertEquals(single.isNull(i), oneThread.isNull(i));
                        if (!single.isNull(i)) {
                            assertArrayEquals(single.get(i), parallel.get(i));
                            assertArrayEquals(single.get(i), oneThread.get(i));
                        }
                        compared++;
                    }
                }
            }
            assertEquals(rows, compared);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * The gap between a task completing its future and releasing its
     * permit: with one permit the producer, woken by the completed
     * future, empties its window and comes back for the next batch
     * while the permit is still held. The producer must wait for the
     * permit there rather than take the failed {@code tryAcquire} for
     * the end of the scan. The hook holds every task's permit until
     * the producer has entered its next {@code loadNextBatch} (seen
     * through the cancel poll that starts it) and parked on the
     * permit, so the empty window with an unreleased permit happens
     * on every batch of every run; a producer that gave up instead
     * returns {@code false} early and the row count below is short.
     */
    public void testEmptyWindowWaitsForTheUnreleasedPermit() throws Exception {
        int rows = 50_000;
        Path scratchDir = createTempDir();
        String uri = writeGeneratedTextTable(scratchDir, "gap-" + getTestName().toLowerCase(Locale.ROOT), rows, 10_000);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        Thread producerThread = Thread.currentThread();
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicBoolean finished = new AtomicBoolean();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            ScanOptions scan = new ScanOptions.Builder().columns(new ArrayList<>(List.of("body")))
                .batchSize(LanceTextAnalyzerBackfill.BATCH_ROWS)
                .scanInOrder(true)
                .build();
            long emitted = 0;
            int batches = 0;
            try (
                LanceScanner scanner = dataset.newScan(scan);
                ArrowReader source = scanner.scanBatches();
                LanceTextAnalyzerBackfill.TokenizingReader producer = new LanceTextAnalyzerBackfill.TokenizingReader(
                    source,
                    "body",
                    "body__lance_tokens",
                    english,
                    LanceRegistry.allocator(),
                    new LanceTextAnalyzerBackfill.Options(threadPool.generic(), 1, () -> {
                        loads.incrementAndGet();
                        return false;
                    })
                )
            ) {
                producer.beforePermitRelease(() -> {
                    // Task i completes during loadNextBatch i + 1; hold
                    // the permit until the producer is inside
                    // loadNextBatch i + 2 and parked (on the permit, the
                    // only thing it can wait for with an empty window),
                    // or until the producer has given up and the test
                    // is unwinding.
                    int mine = completed.getAndIncrement();
                    while (!finished.get()) {
                        Thread.State state = producerThread.getState();
                        if (loads.get() >= mine + 2 && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)) {
                            return;
                        }
                        Thread.onSpinWait();
                    }
                });
                while (producer.loadNextBatch()) {
                    emitted += producer.getVectorSchemaRoot().getRowCount();
                    batches++;
                }
            } finally {
                finished.set(true);
            }
            assertEquals("every row must reach the consumer", rows, emitted);
            assertTrue("the scan must produce more than one batch, saw " + batches, batches > 1);
            assertEquals("every task went through the gap", batches, completed.get());
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A cancel observed between two batches stops the producer; the
     * pending AddColumns fails, commits nothing, and the table keeps
     * its version and schema.
     */
    public void testCancelledBackfillCommitsNothing() throws Exception {
        Path scratchDir = createTempDir();
        String uri = writeGeneratedTextTable(scratchDir, "cancel-" + getTestName().toLowerCase(Locale.ROOT), 50_000, 10_000);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty()); Analyzer english = new EnglishAnalyzer()) {
            long versionBefore = dataset.version();
            AtomicInteger polls = new AtomicInteger();
            Exception e = expectThrows(
                Exception.class,
                () -> LanceTextAnalyzerBackfill.backfill(
                    dataset,
                    "body",
                    "body__lance_tokens",
                    english,
                    LanceRegistry.allocator(),
                    new LanceTextAnalyzerBackfill.Options(Runnable::run, 1, () -> polls.incrementAndGet() > 2)
                )
            );
            assertTrue(e.toString(), e instanceof IOException && e.getMessage().contains("cancelled"));
            assertEquals("a cancelled backfill must not commit", versionBefore, dataset.version());
            for (LanceField field : dataset.getLanceSchema().fields()) {
                assertNotEquals("body__lance_tokens", field.getName());
            }
        }
    }

    public void testEstimateDerivedBytes() throws Exception {
        // The arithmetic: average sampled length times rows times the factor.
        assertEquals(0L, LanceTextAnalyzerBackfill.estimateDerivedBytes(0L, 10L, 1000L));
        assertEquals(0L, LanceTextAnalyzerBackfill.estimateDerivedBytes(100L, 0L, 0L));
        // 1000 bytes over 10 rows is 100 per row; 1,000,000 rows times 1.2.
        assertEquals(120_000_000L, LanceTextAnalyzerBackfill.estimateDerivedBytes(1_000_000L, 10L, 1000L));
        // Fractional averages round up.
        assertEquals((long) Math.ceil(2.5d * 7L * 1.2d), LanceTextAnalyzerBackfill.estimateDerivedBytes(7L, 2L, 5L));

        // On a real table: the six English sentences, one of them null.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeEnglishTextTable(scratchDir, "estimate-" + getTestName().toLowerCase(Locale.ROOT));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            long bytes = 0;
            for (String sentence : LanceTableFactory.ENGLISH_SENTENCES) {
                if (sentence != null) {
                    bytes += sentence.getBytes(StandardCharsets.UTF_8).length;
                }
            }
            long rows = LanceTableFactory.ENGLISH_SENTENCES.length;
            assertEquals(
                LanceTextAnalyzerBackfill.estimateDerivedBytes(rows, rows, bytes),
                LanceTextAnalyzerBackfill.estimateDerivedBytes(dataset, "body")
            );
            Map<String, LanceOverrides.Column> overrides = LanceOverrides.parseAttachClauses(
                Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english")),
                null
            ).textAnalyzerColumns();
            assertEquals(Map.of("body", "body__lance_tokens"), LanceTextAnalyzerBackfill.missingDerivedColumns(dataset, overrides));
        }
    }

    /** The derived column's values indexed by the row's {@code id}, null where the value is Arrow null. */
    private static String[] readDerivedByRow(Dataset dataset, String derived, int rows) throws Exception {
        String[] byId = new String[rows];
        ScanOptions options = new ScanOptions.Builder().columns(new ArrayList<>(Arrays.asList("id", derived))).build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                IntVector ids = (IntVector) root.getVector("id");
                VarCharVector tokens = (VarCharVector) root.getVector(derived);
                for (int i = 0; i < root.getRowCount(); i++) {
                    byId[ids.get(i)] = tokens.isNull(i) ? null : new String(tokens.get(i), StandardCharsets.UTF_8);
                }
            }
        }
        return byId;
    }

    /**
     * The regular files directly under {@code tmpdir}, which is where a
     * spool would appear. Other test JVMs of the same Gradle run share
     * the directory: they create per suite directories and Lance
     * extracts its native library there as {@code jnilib-*.tmp}, so
     * directories and those files are left out.
     */
    private static Set<Path> listTmpdir(Path tmpdir) throws IOException {
        if (!Files.isDirectory(tmpdir)) {
            return new HashSet<>();
        }
        try (Stream<Path> entries = Files.list(tmpdir)) {
            Set<Path> files = new HashSet<>();
            entries.filter(Files::isRegularFile).filter(p -> !p.getFileName().toString().startsWith("jnilib-")).forEach(files::add);
            return files;
        }
    }

    /**
     * The sentence of row {@code id}: eight to fifteen words drawn from
     * {@link #WORDS} by a deterministic function of the id, so a test
     * can recompute any row's expected tokens; every 97th row is null.
     */
    static String sentenceOf(int id) {
        if (id % 97 == 96) {
            return null;
        }
        int words = 8 + (id % 8);
        StringBuilder sentence = new StringBuilder();
        long state = id * 2654435761L + 12345L;
        for (int i = 0; i < words; i++) {
            state = state * 6364136223846793005L + 1442695040888963407L;
            int pick = (int) ((state >>> 33) % WORDS.length);
            if (i > 0) {
                sentence.append(' ');
            }
            sentence.append(WORDS[pick]);
        }
        sentence.append(" doc").append(id);
        return sentence.toString();
    }

    /**
     * Writes a two column table ({@code id} int32, {@code body} Utf8) of
     * {@code rows} rows whose sentences are {@link #sentenceOf}, in
     * fragments of {@code rowsPerFragment} rows, streaming the batches
     * into {@link Dataset#create} so the test does not hold the table
     * in memory.
     */
    static String writeGeneratedTextTable(Path parent, String name, int rows, int rowsPerFragment) throws Exception {
        String uri = parent.resolve(name + ".lance").toString();
        Schema schema = new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("body", FieldType.nullable(new ArrowType.Utf8()), null)
            ),
            Map.of()
        );
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (
                GeneratedReader reader = new GeneratedReader(allocator, schema, rows);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)
            ) {
                Data.exportArrayStream(allocator, reader, stream);
                WriteParams params = new WriteParams.Builder().withMode(WriteParams.WriteMode.CREATE)
                    .withMaxRowsPerFile(rowsPerFragment)
                    .withMaxRowsPerGroup(Math.min(rowsPerFragment, 8192))
                    .build();
                Dataset.create(allocator, stream, uri, params).close();
            }
        }
        return uri;
    }

    /** Produces {@link #sentenceOf} rows in batches of 8192. */
    private static final class GeneratedReader extends ArrowReader {
        private static final int BATCH = 8192;
        private final Schema schema;
        private final int rows;
        private int next;

        GeneratedReader(BufferAllocator allocator, Schema schema, int rows) {
            super(allocator);
            this.schema = schema;
            this.rows = rows;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (next >= rows) {
                return false;
            }
            int count = Math.min(BATCH, rows - next);
            VectorSchemaRoot root = getVectorSchemaRoot();
            root.allocateNew();
            IntVector ids = (IntVector) root.getVector("id");
            VarCharVector body = (VarCharVector) root.getVector("body");
            for (int i = 0; i < count; i++) {
                int id = next + i;
                ids.setSafe(i, id);
                String sentence = sentenceOf(id);
                if (sentence == null) {
                    body.setNull(i);
                } else {
                    body.setSafe(i, sentence.getBytes(StandardCharsets.UTF_8));
                }
            }
            ids.setValueCount(count);
            body.setValueCount(count);
            root.setRowCount(count);
            next += count;
            return true;
        }

        @Override
        public long bytesRead() {
            return 0L;
        }

        @Override
        protected void closeReadSource() {}

        @Override
        protected Schema readSchema() {
            return schema;
        }
    }
}
