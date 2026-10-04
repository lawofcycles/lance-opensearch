/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.LanceFragmentSchema.TakeProjection;
import org.opensearch.lance.query.LanceHitsAccounting;
import org.opensearch.lance.query.ScanAdmission;

/**
 * One {@code _rowaddr IN (...)} take over the rows of several
 * {@link LanceFragmentLeafReader}s of one reader, for the fetch round of
 * a page. The round hands a node the rows of the page it holds, which
 * on a sorted or scored page over a wide table sit in many fragments;
 * Lance's take by address accepts the addresses of any fragment of the
 * dataset in one call, so the node issues one scan over its share of
 * the page instead of one per fragment, and on object storage pays one
 * round trip instead of one per fragment. The leaves' own take
 * ({@link LanceStoredFields#prefetchRows}, one per leaf, the leaves side
 * by side) stays the path of a page rendered on the query round, where
 * every leaf's rows are a page of their own.
 *
 * <p>The take reads exactly what the leaves' own takes would: every leaf
 * sets aside the rows it already holds and, when the fragment hits phase
 * marked it eligible, the rows the node's {@link LanceFetchCache} serves
 * ({@link LanceStoredFields#addressesToTake}); the remaining addresses
 * are taken in chunks of {@link LanceStoredFields#TAKE_CHUNK} with the
 * columns of the request's {@link TakeProjection}, each chunk judged by
 * {@link ScanAdmission#admitFetchTake} before its scan opens and
 * recorded in {@link FetchTakeStats} as one take; every row returned is
 * decoded and stored by the leaf that owns its fragment
 * ({@link LanceStoredFields#recordTakenRow}), and the rows the take did
 * not return are marked missing ({@link LanceStoredFields#recordMissingRows}).
 * The per row, per column cache lookups and the admission per chunk are
 * the ones the leaves' own takes make; only the number of scans differs.
 */
public final class LanceMultiLeafTake {

    private static final Logger LOGGER = LogManager.getLogger(LanceMultiLeafTake.class);

    private LanceMultiLeafTake() {}

    /**
     * Fetch the rows behind {@code docIdsByLeaf} (leaf local doc ids of
     * parent docs, per leaf) in one take per {@link LanceStoredFields#TAKE_CHUNK}
     * addresses over the dataset the leaves read, and stash them in the
     * leaves for their {@code document(...)} calls. The leaves must be
     * leaves of one reader opened for one request: they share the
     * dataset, the take projection, the take accumulator and the
     * admission ticket, which are read from the first. A request that
     * renders no column ({@code _source: false} on a table without a
     * key) calls into Lance for nothing and lets every leaf record its
     * rows as empty the way its own take does.
     *
     * <p>{@code cancellation} is the task the fetch round runs under and
     * is checked after every batch the take returns, so a cancelled
     * round gives the thread back at the next batch boundary instead of
     * at the end of the chunk; the check throws
     * {@link TaskCancelledException}, which closes the scanner through
     * the try-with-resources. Null is not accepted.
     */
    public static void prefetchRows(Map<LanceFragmentLeafReader, int[]> docIdsByLeaf, LanceCancellation cancellation) throws IOException {
        Objects.requireNonNull(cancellation, "cancellation");
        if (docIdsByLeaf.isEmpty()) {
            return;
        }
        LanceFragmentLeafReader first = docIdsByLeaf.keySet().iterator().next();
        List<String> takeColumns = first.takeProjection().columns();
        if (takeColumns.isEmpty()) {
            for (Map.Entry<LanceFragmentLeafReader, int[]> entry : docIdsByLeaf.entrySet()) {
                entry.getKey().prefetchRows(entry.getValue(), cancellation);
            }
            return;
        }
        Dataset dataset = first.dataset();
        Map<Integer, LanceStoredFields> storedFieldsByFragment = new HashMap<>();
        Map<LanceStoredFields, Set<Integer>> requestedByLeaf = new HashMap<>();
        List<Integer> fragmentIds = new ArrayList<>(docIdsByLeaf.size());
        List<Long> addresses = new ArrayList<>();
        for (Map.Entry<LanceFragmentLeafReader, int[]> entry : docIdsByLeaf.entrySet()) {
            LanceFragmentLeafReader leaf = entry.getKey();
            LanceStoredFields storedFields = (LanceStoredFields) leaf.storedFields();
            if (!storedFields.takeProjection().columns().equals(takeColumns)) {
                throw new IllegalStateException(
                    "fragment "
                        + leaf.fragmentId()
                        + " takes "
                        + storedFields.takeProjection().columns()
                        + " where the fetch round takes "
                        + takeColumns
                );
            }
            Set<Integer> requested = new HashSet<>();
            addresses.addAll(storedFields.addressesToTake(entry.getValue(), requested));
            storedFieldsByFragment.put(leaf.fragmentId(), storedFields);
            requestedByLeaf.put(storedFields, requested);
            fragmentIds.add(leaf.fragmentId());
        }
        if (addresses.isEmpty()) {
            return;
        }
        LanceShardColumnCache columnCache = first.shardColumnCache();
        LanceHitsAccounting ticket = columnCache == null ? null : columnCache.admissionTicket();
        FetchTakeStats.Accumulator accumulator = first.takeAccumulator();
        for (int from = 0; from < addresses.size(); from += LanceStoredFields.TAKE_CHUNK) {
            List<Long> chunk = addresses.subList(from, Math.min(from + LanceStoredFields.TAKE_CHUNK, addresses.size()));
            StringBuilder sql = new StringBuilder(chunk.size() * 12 + 16).append("_rowaddr IN (");
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(chunk.get(i).longValue());
            }
            sql.append(')');
            ScanOptions options = new ScanOptions.Builder().fragmentIds(fragmentIds)
                .columns(takeColumns)
                .filter(sql.toString())
                .withRowAddress(true)
                .build();
            ScanAdmission.admitFetchTake(dataset.uri(), dataset, takeColumns, chunk.size(), ticket);
            ScanAdmission.scanStarted();
            long start = System.nanoTime();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    // A cancelled round stops at the batch boundary; the
                    // batch itself runs in native code and cannot be
                    // interrupted.
                    cancellation.checkCancelled();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector[] vectors = new FieldVector[takeColumns.size()];
                    for (int c = 0; c < vectors.length; c++) {
                        vectors[c] = root.getVector(takeColumns.get(c));
                    }
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long address = rowAddr.get(i);
                        LanceStoredFields owner = storedFieldsByFragment.get((int) (address >>> 32));
                        if (owner == null) {
                            // The scan was pinned to the leaves' fragments,
                            // so every address names one of them.
                            throw new IllegalStateException("row address " + address + " of the take names a fragment outside the reader");
                        }
                        Object[] row = new Object[vectors.length];
                        for (int c = 0; c < vectors.length; c++) {
                            row[c] = owner.decodeTakeValue(takeColumns.get(c), vectors[c], i);
                        }
                        owner.recordTakenRow(address, row);
                    }
                }
            } catch (IOException | TaskCancelledException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                ScanAdmission.scanFinished();
                long elapsed = System.nanoTime() - start;
                FetchTakeStats.record(FetchTakeStats.Kind.STORED_FIELDS, chunk.size(), takeColumns.size(), elapsed, accumulator);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "lance.fetch: take of {} rows and {} columns from {} fragments of [{}] on thread [{}] in {} ms",
                        chunk.size(),
                        takeColumns.size(),
                        fragmentIds.size(),
                        dataset.uri(),
                        Thread.currentThread().getName(),
                        elapsed / 1_000_000L
                    );
                }
            }
        }
        for (Map.Entry<LanceStoredFields, Set<Integer>> entry : requestedByLeaf.entrySet()) {
            entry.getKey().recordMissingRows(entry.getValue());
        }
    }
}
