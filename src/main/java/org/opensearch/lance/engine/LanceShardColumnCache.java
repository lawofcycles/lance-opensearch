/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.util.FixedBitSet;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

/**
 * Per-{@link LanceDirectoryReader} coordinator that reads a single
 * Lance column once across the reader's fragment subset and hands each
 * {@link LanceFragmentLeafReader} its fragment's slice. Every
 * {@code ensureXxxLoaded} accessor in the leaf goes through this cache
 * on the fragment-dispatch path; the shard engine's whole-table
 * {@link LanceDirectoryReader#open} path also installs a cache so both
 * lifecycles benefit uniformly.
 *
 * <p>Before this cache existed, each leaf's {@code ensureXxxLoaded}
 * opened its own {@code dataset.newScan} restricted to one fragment.
 * On a per-node subset of ~20 fragments loading three columns for an
 * aggregation, that meant 60 native scan startups (15-19 ms of FFI
 * overhead each on the QA perf table, 20M rows), so an aggregation
 * paid 0.9-1.1 s of fixed cost before Lance did any real per-doc
 * work. Consolidating into one scan per (column, cache) shrinks the
 * fixed cost to a single {@code newScan} call per column materialised.
 * See {@code issue-42-design.md} Phase D / Direction 4 (column
 * materialisation side).
 *
 * <p>The cache does not own the loaded arrays. {@link #loadNumericColumn}
 * scans, buckets the rows by the fragment id embedded in {@code _rowaddr},
 * and pushes the per-fragment slices into each leaf's own
 * {@link LanceFragmentLeafReader#publishNumericColumn} sink. Every leaf
 * accessor still reads from its own {@code numericColumns} /
 * {@code numericPresence} map, so the surface exposed to Lucene is
 * unchanged; the only observable difference is that the {@code Map}
 * populates atomically for every leaf on the first request rather than
 * lazily per leaf.
 *
 * <p>Concurrency: a per-column {@code Object} lock serialises
 * concurrent loads of the same column. Different columns load in
 * parallel. The {@code loaded*} sets are used as short-circuit
 * guards after the lock releases so a second caller sees the
 * completion flag through {@code ConcurrentHashMap}'s happens-before.
 */
public final class LanceShardColumnCache {

    private final Dataset dataset;
    private final String filterSql;
    private final Map<Integer, LanceFragmentLeafReader> leavesByFragmentId;
    private final Map<String, Object> columnLocks = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedNumericColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedBooleanColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedTextColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedKeywordArrayColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedBinaryColumns = new ConcurrentHashMap<>();

    /**
     * Build a cache scoped to {@code leaves} against {@code dataset}.
     * The list is copied into a fragment-id map so per-leaf lookups
     * during scan iteration are constant time.
     *
     * @param dataset   the shared Lance dataset the reader was opened
     *                  against; every scan in the cache is issued
     *                  against this same handle.
     * @param filterSql top-level filter to layer into every column
     *                  scan (currently the same value the leaves use
     *                  in their own {@code singleColumnScan}); may
     *                  be {@code null} when the reader was opened
     *                  without a top-level filter push-down.
     * @param leaves    the {@link LanceFragmentLeafReader}s attached
     *                  to the reader, one per fragment in the
     *                  reader's subset.
     */
    LanceShardColumnCache(Dataset dataset, String filterSql, List<LanceFragmentLeafReader> leaves) {
        this.dataset = dataset;
        this.filterSql = filterSql;
        Map<Integer, LanceFragmentLeafReader> byId = new HashMap<>(leaves.size() * 2);
        for (LanceFragmentLeafReader leaf : leaves) {
            byId.put(leaf.fragmentId(), leaf);
        }
        this.leavesByFragmentId = Collections.unmodifiableMap(byId);
    }

    /**
     * Load {@code name} into every leaf's numeric column cache. No-op
     * when the column has already been loaded for this cache instance
     * (idempotent by design; callers hit this method every time they
     * fault into their own {@code ensureNumericLoaded} branch).
     *
     * <p>Preallocates a {@code long[maxDoc]} + {@code FixedBitSet}
     * pair for every leaf before opening the scan; the sizes come
     * from each leaf's {@code physicalRows} which was fixed at
     * fragment metadata read time and does not change.
     *
     * <p>The scan is {@code newScan(fragmentIds = allLeafFragments,
     * columns = [name], withRowAddress = true)} plus the cache's
     * top-level filter if present. Each row's {@code _rowaddr}
     * splits into (fragmentId, offset); the row's value goes into
     * the fragment's leaf. Rows for fragments outside the cache's
     * subset are dropped defensively — they should not occur because
     * we scope the scan by {@code fragmentIds}, but the guard keeps
     * an unexpected batch from throwing an NPE.
     */
    public void loadNumericColumn(String name) throws IOException {
        if (loadedNumericColumns.containsKey(name)) {
            return;
        }
        Object lock = columnLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            if (loadedNumericColumns.containsKey(name)) {
                return;
            }
            Map<Integer, long[]> valuesByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            Map<Integer, FixedBitSet> presenceByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                int maxDoc = leaf.maxDoc();
                valuesByFragment.put(leaf.fragmentId(), new long[maxDoc]);
                presenceByFragment.put(leaf.fragmentId(), new FixedBitSet(maxDoc));
            }
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(new java.util.ArrayList<>(leavesByFragmentId.keySet()))
                .columns(Collections.singletonList(name))
                .withRowAddress(true);
            if (filterSql != null) {
                builder = builder.filter(filterSql);
            }
            try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector vector = root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        long[] values = valuesByFragment.get(fragId);
                        FixedBitSet presence = presenceByFragment.get(fragId);
                        if (values == null || presence == null) {
                            // Scan returned a row for a fragment not
                            // in the cache subset. Skip defensively;
                            // the fragmentIds filter above should
                            // have prevented this.
                            continue;
                        }
                        if (!vector.isNull(i)) {
                            values[offset] = LanceFragmentLeafReader.readAsLong(vector, i);
                            presence.set(offset);
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                leaf.publishNumericColumn(name, valuesByFragment.get(leaf.fragmentId()), presenceByFragment.get(leaf.fragmentId()));
            }
            loadedNumericColumns.put(name, Boolean.TRUE);
        }
    }

    /**
     * Load a Boolean column across the fragment subset. Same shape as
     * {@link #loadNumericColumn} but reads {@link BitVector} bits into
     * a long-encoded array so the leaf's numeric-doc-value accessors
     * can read them uniformly (a 1 for true, a 0 for false).
     */
    public void loadBooleanColumn(String name) throws IOException {
        if (loadedBooleanColumns.containsKey(name)) {
            return;
        }
        Object lock = columnLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            if (loadedBooleanColumns.containsKey(name)) {
                return;
            }
            Map<Integer, long[]> valuesByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            Map<Integer, FixedBitSet> presenceByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                int maxDoc = leaf.maxDoc();
                valuesByFragment.put(leaf.fragmentId(), new long[maxDoc]);
                presenceByFragment.put(leaf.fragmentId(), new FixedBitSet(maxDoc));
            }
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(new java.util.ArrayList<>(leavesByFragmentId.keySet()))
                .columns(Collections.singletonList(name))
                .withRowAddress(true);
            if (filterSql != null) {
                builder = builder.filter(filterSql);
            }
            try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    BitVector vector = (BitVector) root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        long[] values = valuesByFragment.get(fragId);
                        FixedBitSet presence = presenceByFragment.get(fragId);
                        if (values == null || presence == null) {
                            continue;
                        }
                        if (!vector.isNull(i)) {
                            values[offset] = vector.get(i);
                            presence.set(offset);
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                leaf.publishBooleanColumn(name, valuesByFragment.get(leaf.fragmentId()), presenceByFragment.get(leaf.fragmentId()));
            }
            loadedBooleanColumns.put(name, Boolean.TRUE);
        }
    }

    /**
     * Load a Utf8 column across the fragment subset. The cache reads
     * the raw {@link String} values into a per-fragment
     * {@code String[maxDoc]} and hands them to each leaf via
     * {@link LanceFragmentLeafReader#publishTextColumn}; the leaf
     * builds any per-fragment keyword dictionary
     * ({@code keywordTerms} / {@code keywordOrds}) itself based on
     * its own {@code columnKind} / {@code basesWithKeywordSub} state.
     * Keyword dictionaries stay per-fragment because that is what
     * {@link org.apache.lucene.index.SortedDocValues} expects for
     * ord-comparison semantics.
     */
    public void loadTextColumn(String name) throws IOException {
        if (loadedTextColumns.containsKey(name)) {
            return;
        }
        Object lock = columnLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            if (loadedTextColumns.containsKey(name)) {
                return;
            }
            Map<Integer, String[]> rawByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                rawByFragment.put(leaf.fragmentId(), new String[leaf.maxDoc()]);
            }
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(new java.util.ArrayList<>(leavesByFragmentId.keySet()))
                .columns(Collections.singletonList(name))
                .withRowAddress(true);
            if (filterSql != null) {
                builder = builder.filter(filterSql);
            }
            try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    VarCharVector vector = (VarCharVector) root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        String[] raw = rawByFragment.get(fragId);
                        if (raw == null) {
                            continue;
                        }
                        raw[offset] = vector.isNull(i) ? null : new String(vector.get(i), java.nio.charset.StandardCharsets.UTF_8);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                leaf.publishTextColumn(name, rawByFragment.get(leaf.fragmentId()));
            }
            loadedTextColumns.put(name, Boolean.TRUE);
        }
    }

    /**
     * Load a {@code List<Utf8>} column across the fragment subset.
     * Reads the nested Arrow list-of-varchar values into per-fragment
     * {@code String[maxDoc][]} arrays and hands them to each leaf via
     * {@link LanceFragmentLeafReader#publishKeywordArrayColumn};
     * the leaf builds its own per-fragment ord dictionary in the
     * publish sink (multi-valued keyword semantics live in the leaf's
     * {@code keywordArrayValues} / {@code keywordArrayOrds} /
     * {@code keywordArrayTerms} maps).
     */
    public void loadKeywordArrayColumn(String name) throws IOException {
        if (loadedKeywordArrayColumns.containsKey(name)) {
            return;
        }
        Object lock = columnLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            if (loadedKeywordArrayColumns.containsKey(name)) {
                return;
            }
            Map<Integer, String[][]> rowsByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                rowsByFragment.put(leaf.fragmentId(), new String[leaf.maxDoc()][]);
            }
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(new java.util.ArrayList<>(leavesByFragmentId.keySet()))
                .columns(Collections.singletonList(name))
                .withRowAddress(true);
            if (filterSql != null) {
                builder = builder.filter(filterSql);
            }
            try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    ListVector vector = (ListVector) root.getVector(name);
                    VarCharVector elements = (VarCharVector) vector.getDataVector();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        String[][] rows = rowsByFragment.get(fragId);
                        if (rows == null) {
                            continue;
                        }
                        if (vector.isNull(i)) {
                            rows[offset] = null;
                            continue;
                        }
                        int start = vector.getElementStartIndex(i);
                        int end = vector.getElementEndIndex(i);
                        String[] arr = new String[end - start];
                        for (int e = start; e < end; e++) {
                            arr[e - start] = elements.isNull(e)
                                ? null
                                : new String(elements.get(e), java.nio.charset.StandardCharsets.UTF_8);
                        }
                        rows[offset] = arr;
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                leaf.publishKeywordArrayColumn(name, rowsByFragment.get(leaf.fragmentId()));
            }
            loadedKeywordArrayColumns.put(name, Boolean.TRUE);
        }
    }

    /**
     * Load a Binary (or LargeBinary) column across the fragment
     * subset. Reads raw bytes into per-fragment {@code byte[maxDoc][]}
     * arrays and hands them to each leaf via
     * {@link LanceFragmentLeafReader#publishBinaryColumn}.
     */
    public void loadBinaryColumn(String name) throws IOException {
        if (loadedBinaryColumns.containsKey(name)) {
            return;
        }
        Object lock = columnLocks.computeIfAbsent(name, k -> new Object());
        synchronized (lock) {
            if (loadedBinaryColumns.containsKey(name)) {
                return;
            }
            Map<Integer, byte[][]> valuesByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                valuesByFragment.put(leaf.fragmentId(), new byte[leaf.maxDoc()][]);
            }
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(new java.util.ArrayList<>(leavesByFragmentId.keySet()))
                .columns(Collections.singletonList(name))
                .withRowAddress(true);
            if (filterSql != null) {
                builder = builder.filter(filterSql);
            }
            try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector v = root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        byte[][] values = valuesByFragment.get(fragId);
                        if (values == null) {
                            continue;
                        }
                        if (v.isNull(i)) {
                            values[offset] = null;
                        } else if (v instanceof VarBinaryVector vb) {
                            values[offset] = vb.get(i);
                        } else if (v instanceof LargeVarBinaryVector lb) {
                            values[offset] = lb.get(i);
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                leaf.publishBinaryColumn(name, valuesByFragment.get(leaf.fragmentId()));
            }
            loadedBinaryColumns.put(name, Boolean.TRUE);
        }
    }
}
