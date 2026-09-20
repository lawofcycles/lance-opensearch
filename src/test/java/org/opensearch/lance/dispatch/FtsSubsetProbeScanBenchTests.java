/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Timing of the Lance scans a subset executor can run for an unbounded
 * FTS shape, on a 2,000,000 row table with a 200,000 match term and a
 * 1,000,000 match term. Skipped unless {@code tests.lance.fts_probe_bench_dir} names a
 * directory; the table is written there on the first run and reused
 * afterwards, so the same table can be attached to a cluster for the
 * request level comparison.
 *
 * <p>Fixture: eight fragments of 250,000 rows, row {@code i} in
 * fragment {@code i / 250000}. Columns {@code id} (int32),
 * {@code body} (Utf8 with an INVERTED index: {@code "hello tok<i>"},
 * {@code hit10} when {@code i % 10 == 0}, {@code hit2} when
 * {@code i % 2 == 0}, {@code grp<i % 25>}, then
 * {@code lance} repeated {@code (i % 5) + 1} times so BM25 scores
 * vary), {@code category} (Utf8, {@code "c" + (i % 3)}),
 * {@code rating} (int32, {@code (i * 37) % 1000}) and {@code ts}
 * (timestamp[us], 2024-01-01 plus {@code i} seconds).
 *
 * <p>Scans, each returning {@code _rowaddr} and {@code _score} like
 * the hits scan of {@code LanceFtsWeight}: (a) whole table with
 * {@code limit(1_000_000)}, the probe at the default
 * {@code lance.fts.subset_probe_limit}; (b) whole table without a
 * limit, the probe without the top k plan the limit makes Lance
 * build; (c) restricted to fragments 0 to 2 without a limit, the
 * fallback; (d) the scan of (b) closed after the batch that carries
 * it past 1,000 rows, the cost of an unlimited probe that is given
 * up. (a) to (c) are read to their end; the run to run median of
 * nine is logged per term, with the batch layout of (b).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FtsSubsetProbeScanBenchTests extends OpenSearchTestCase {

    static final String DIR_PROPERTY = "tests.lance.fts_probe_bench_dir";
    static final String TABLE_NAME = "fts-probe-bench-2m";
    static final int FRAGMENTS = 8;
    static final int ROWS_PER_FRAGMENT = 250_000;
    static final int HIT_EVERY = 10;
    static final String HIT_TERM = "hit10";
    static final int HALF_EVERY = 2;
    static final String HALF_TERM = "hit2";
    static final List<Integer> SUBSET = List.of(0, 1, 2);
    static final int ROUNDS = 9;

    public void testScanTimings() throws Exception {
        String dir = System.getProperty(DIR_PROPERTY);
        assumeTrue("set -D" + DIR_PROPERTY + "=<dir> to run the scan timing", dir != null);
        String uri = ensureTable(Path.of(dir));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            timeTerm(dataset, HIT_TERM, HIT_EVERY);
            timeTerm(dataset, HALF_TERM, HALF_EVERY);
        }
    }

    private void timeTerm(Dataset dataset, String term, int every) throws Exception {
        long expectedHits = (long) FRAGMENTS * ROWS_PER_FRAGMENT / every;
        long expectedSubsetHits = (long) SUBSET.size() * ROWS_PER_FRAGMENT / every;
        FullTextQuery query = FullTextQuery.match(term, "body");
        ScanOptions limited = scan(query).limit(1_000_000L).build();
        ScanOptions unlimited = scan(query).build();
        ScanOptions restricted = scan(query).fragmentIds(SUBSET).build();
        // Warm up: the first scan loads the inverted index.
        assertEquals(expectedHits, readAll(dataset, limited, Long.MAX_VALUE));
        assertEquals(expectedHits, readAll(dataset, unlimited, Long.MAX_VALUE));
        assertEquals(expectedSubsetHits, readAll(dataset, restricted, Long.MAX_VALUE));
        long[] limitedMillis = new long[ROUNDS];
        long[] unlimitedMillis = new long[ROUNDS];
        long[] restrictedMillis = new long[ROUNDS];
        long[] abandonedMillis = new long[ROUNDS];
        for (int round = 0; round < ROUNDS; round++) {
            limitedMillis[round] = time(dataset, limited, Long.MAX_VALUE, expectedHits);
            unlimitedMillis[round] = time(dataset, unlimited, Long.MAX_VALUE, expectedHits);
            restrictedMillis[round] = time(dataset, restricted, Long.MAX_VALUE, expectedSubsetHits);
            abandonedMillis[round] = time(dataset, unlimited, 1_000L, -1L);
        }
        logger.info(
            "fts probe scan timing for {} ({} matches, ms, {} rounds): (a) limit(1000000) {} median {}; (b) no limit {} median {}; "
                + "(c) fragmentIds {} no limit {} median {}; (d) no limit, closed after the batch that passes 1000 rows {} median {}; "
                + "(b) delivers {}",
            term,
            expectedHits,
            ROUNDS,
            Arrays.toString(limitedMillis),
            median(limitedMillis),
            Arrays.toString(unlimitedMillis),
            median(unlimitedMillis),
            SUBSET,
            Arrays.toString(restrictedMillis),
            median(restrictedMillis),
            Arrays.toString(abandonedMillis),
            median(abandonedMillis),
            batchShape(dataset, unlimited)
        );
    }

    /** How the unlimited scan streams: time to the first batch, batch count, rows of the first batch. */
    private static String batchShape(Dataset dataset, ScanOptions options) throws Exception {
        long start = System.nanoTime();
        long firstBatchMillis = -1L;
        long firstBatchRows = 0L;
        int batches = 0;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                if (batches == 0) {
                    firstBatchMillis = (System.nanoTime() - start) / 1_000_000L;
                    firstBatchRows = reader.getVectorSchemaRoot().getRowCount();
                }
                batches++;
            }
        }
        long totalMillis = (System.nanoTime() - start) / 1_000_000L;
        return String.format(
            Locale.ROOT,
            "%d batches, first batch of %d rows after %d ms, last after %d ms",
            batches,
            firstBatchRows,
            firstBatchMillis,
            totalMillis
        );
    }

    private static ScanOptions.Builder scan(FullTextQuery query) {
        return new ScanOptions.Builder().fullTextQuery(query).columns(Collections.emptyList()).withRowAddress(true).withRowId(false);
    }

    /** Milliseconds of one scan; {@code expectedRows < 0} skips the row check (a scan closed early). */
    private static long time(Dataset dataset, ScanOptions options, long maxRows, long expectedRows) throws Exception {
        long start = System.nanoTime();
        long rows = readAll(dataset, options, maxRows);
        long millis = (System.nanoTime() - start) / 1_000_000L;
        if (expectedRows >= 0) {
            assertEquals(expectedRows, rows);
        } else {
            assertTrue("the scan was meant to be closed early, read " + rows, rows > maxRows);
        }
        return millis;
    }

    /** Rows read before the scan ended or, once more than {@code maxRows} came back, before it was closed. */
    private static long readAll(Dataset dataset, ScanOptions options, long maxRows) throws Exception {
        long rows = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
                if (rows > maxRows) {
                    break;
                }
            }
        }
        return rows;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    /** URI of the bench table under {@code dir}, written if it is not there yet. */
    static String ensureTable(Path dir) throws Exception {
        Path tablePath = dir.resolve(TABLE_NAME + ".lance");
        if (Files.isDirectory(tablePath.resolve("_versions"))) {
            return tablePath.toString();
        }
        Files.createDirectories(dir);
        String uri = tablePath.toString();
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("body", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("rating", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null)
            ),
            Map.of()
        );
        long epochMicros = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli() * 1000L;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            for (int fragment = 0; fragment < FRAGMENTS; fragment++) {
                byte[] ipcBytes;
                try (
                    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    ByteArrayOutputStream out = new ByteArrayOutputStream()
                ) {
                    IntVector idVector = (IntVector) root.getVector("id");
                    VarCharVector bodyVector = (VarCharVector) root.getVector("body");
                    VarCharVector categoryVector = (VarCharVector) root.getVector("category");
                    IntVector ratingVector = (IntVector) root.getVector("rating");
                    TimeStampMicroVector tsVector = (TimeStampMicroVector) root.getVector("ts");
                    idVector.allocateNew(ROWS_PER_FRAGMENT);
                    bodyVector.allocateNew(64L * ROWS_PER_FRAGMENT, ROWS_PER_FRAGMENT);
                    categoryVector.allocateNew(2L * ROWS_PER_FRAGMENT, ROWS_PER_FRAGMENT);
                    ratingVector.allocateNew(ROWS_PER_FRAGMENT);
                    tsVector.allocateNew(ROWS_PER_FRAGMENT);
                    for (int slot = 0; slot < ROWS_PER_FRAGMENT; slot++) {
                        int i = fragment * ROWS_PER_FRAGMENT + slot;
                        idVector.set(slot, i);
                        StringBuilder body = new StringBuilder("hello tok").append(i);
                        if (i % HIT_EVERY == 0) {
                            body.append(' ').append(HIT_TERM);
                        }
                        if (i % HALF_EVERY == 0) {
                            body.append(' ').append(HALF_TERM);
                        }
                        body.append(" grp").append(i % 25).append(" lance".repeat((i % 5) + 1));
                        bodyVector.setSafe(slot, body.toString().getBytes(StandardCharsets.UTF_8));
                        categoryVector.setSafe(slot, ("c" + (i % 3)).getBytes(StandardCharsets.UTF_8));
                        ratingVector.set(slot, (i * 37) % 1000);
                        tsVector.set(slot, epochMicros + i * 1_000_000L);
                    }
                    idVector.setValueCount(ROWS_PER_FRAGMENT);
                    bodyVector.setValueCount(ROWS_PER_FRAGMENT);
                    categoryVector.setValueCount(ROWS_PER_FRAGMENT);
                    ratingVector.setValueCount(ROWS_PER_FRAGMENT);
                    tsVector.setValueCount(ROWS_PER_FRAGMENT);
                    root.setRowCount(ROWS_PER_FRAGMENT);
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
                    WriteParams writeParams = new WriteParams.Builder().withMode(mode).withMaxRowsPerFile(ROWS_PER_FRAGMENT).build();
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
                    IndexOptions.builder(Collections.singletonList("body"), IndexType.INVERTED, indexParams)
                        .withIndexName("body_fts")
                        .build()
                );
                List<Integer> fragmentIds = new ArrayList<>();
                dataset.getFragments().forEach(fragment -> fragmentIds.add(fragment.getId()));
                assertEquals(String.format(Locale.ROOT, "fragments of %s", uri), FRAGMENTS, fragmentIds.size());
            }
        }
        return uri;
    }
}
