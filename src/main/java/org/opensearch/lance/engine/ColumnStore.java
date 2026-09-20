/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.engine.LanceWarmCache.SnapshotKey;

/**
 * Node-wide off-heap store of columns, one {@link StoreEntry} per
 * (snapshot, column, fragment): a {@link CachedColumn} for numeric and
 * boolean columns, a {@link CachedKeywordColumn} for single-valued
 * keyword columns and a {@link CachedKeywordArrayColumn} for
 * {@code List<Utf8>} columns. Every entry is made of Arrow vectors
 * allocated from one child allocator whose limit is the column cache
 * budget, so {@link #allocatedBytes()} is the exact off-heap footprint
 * the {@code lance_native} circuit breaker has to account for.
 *
 * <p>Loading is one Lance scan per (snapshot, column) over the fragments
 * a request needs and does not have yet, without any filter, so the
 * loaded slice serves every later request over the same snapshot
 * regardless of its query. Numeric columns are sized from the fragment's
 * row count before the scan and written while the batches stream.
 * Keyword columns cannot be sized up front (the dictionary size depends
 * on the values), so their scan interns the values into a heap
 * {@link KeywordDictionaryBuilder} per fragment, the budget is checked
 * against the resulting sizes, and only then are the vectors allocated
 * and written; the heap structures live for the load only.
 *
 * <p>Eviction is least recently used at (column, fragment) granularity
 * and skips entries a running request has pinned. When the budget cannot
 * be met even after evicting everything unpinned, the numeric
 * {@link #acquire} returns {@code null} before any scan and the caller
 * falls back to its request scoped heap load; the keyword variants have
 * already scanned by then, so they hand the interned dictionary and
 * ordinals back as {@link HeapKeyword} / {@link HeapKeywordArray} and
 * the caller publishes those instead of scanning the column again.
 */
public final class ColumnStore implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(ColumnStore.class);

    /** Label the circuit breaker check reports when a column load trips it. */
    static final String BREAKER_LABEL = "lance_column_cache";

    /** Buffers at or above this size are allocated exactly; smaller ones round up to a power of two. */
    private static final long ALLOCATOR_CHUNK_SIZE = 16L * 1024 * 1024;

    /**
     * A column's Arrow type is fixed by the table schema, so one key maps
     * to exactly one entry kind; the {@code acquire} methods cast on
     * lookup.
     */
    private record ColumnKey(SnapshotKey snapshot, String column, int fragmentId) {
    }

    /**
     * Heap dictionary of one fragment's Utf8 column, the shape
     * {@code LanceFragmentLeafReader#publishTextColumn} takes: terms in
     * unsigned byte order and one ordinal per row ({@code -1} for an
     * Arrow null). Handed to the caller when a keyword scan produced a
     * dictionary the store has no room for.
     */
    public record HeapKeyword(BytesRef[] terms, int[] ords) {
    }

    /**
     * {@link HeapKeyword} for a {@code List<Utf8>} column: per row a
     * strictly ascending duplicate-free ordinal array, {@code null} for
     * an Arrow null list.
     */
    public record HeapKeywordArray(BytesRef[] terms, int[][] rowOrds) {
    }

    /**
     * Result of {@link #acquireKeyword}: every requested fragment is in
     * exactly one of the two maps. {@code stored} entries are pinned and
     * the caller unpins them through {@link #unpin}; {@code heap}
     * dictionaries belong to the caller's request.
     */
    public record KeywordLoad(Map<Integer, CachedKeywordColumn> stored, Map<Integer, HeapKeyword> heap) {
    }

    /** {@link KeywordLoad} for {@link #acquireKeywordArray}. */
    public record KeywordArrayLoad(Map<Integer, CachedKeywordArrayColumn> stored, Map<Integer, HeapKeywordArray> heap) {
    }

    /**
     * What a loader produced for the missing fragments: entries that fit
     * the budget (not yet in the map, not pinned) and, for keyword
     * loaders, the heap dictionaries of the fragments that did not fit.
     * A fragment is in one of the two maps, never both.
     */
    private record Loaded<T extends StoreEntry, H>(Map<Integer, T> stored, Map<Integer, H> heap) {
    }

    /**
     * Loads the missing fragments of one column. Returns {@code null}
     * when the budget check that precedes the scan fails (numeric
     * columns); may throw {@link OutOfMemoryException} when an allocation
     * exceeds the allocator's limit despite the budget check.
     */
    @FunctionalInterface
    private interface Loader<T extends StoreEntry, H> {
        Loaded<T, H> load(Dataset dataset, String column, Map<Integer, Integer> missing) throws IOException;
    }

    private final BufferAllocator allocator;
    private final long limitBytes;
    /** Access ordered so iteration starts at the least recently used entry. Guarded by {@code this}. */
    private final LinkedHashMap<ColumnKey, StoreEntry> columns = new LinkedHashMap<>(256, 0.75f, true);
    /** Serialises the load of one (snapshot, column) so two requests do not scan it twice. */
    private final Map<String, Object> loadLocks = new ConcurrentHashMap<>();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong loads = new AtomicLong();
    private final AtomicLong scans = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong budgetMisses = new AtomicLong();

    /**
     * @param parent     allocator the store's child allocator is created
     *                   from ({@code LanceRegistry.allocator()} in
     *                   production)
     * @param limitBytes upper bound of the child allocator; the store never
     *                   holds more than this and falls back to heap loads
     *                   when a request would exceed it
     */
    public ColumnStore(BufferAllocator parent, long limitBytes) {
        this.limitBytes = Math.max(0L, limitBytes);
        this.allocator = parent.newChildAllocator("lance-column-cache", 0L, this.limitBytes);
    }

    /**
     * Return the numeric or boolean columns for {@code fragmentRows}
     * (fragment id to row count), loading the missing ones with one scan
     * of {@code dataset}, and pin every returned column. The caller
     * unpins them through {@link #unpin} when its request ends.
     *
     * @return one {@link CachedColumn} per requested fragment, or
     *         {@code null} when the missing fragments do not fit the
     *         budget even after eviction (the caller falls back to a heap
     *         load)
     * @throws org.opensearch.core.common.breaker.CircuitBreakingException
     *         when the {@code lance_native} breaker is at its limit and a
     *         load would be needed
     */
    public Map<Integer, CachedColumn> acquire(
        SnapshotKey snapshot,
        Dataset dataset,
        String column,
        boolean isBoolean,
        Map<Integer, Integer> fragmentRows
    ) throws IOException {
        Loaded<CachedColumn, Void> loaded = acquire(snapshot, dataset, column, fragmentRows, CachedColumn.class, (d, c, missing) -> {
            long needed = 0L;
            for (int rows : missing.values()) {
                needed += estimateBytes(rows, isBoolean);
            }
            if (!makeRoom(needed)) {
                return null;
            }
            return new Loaded<>(loadNumeric(d, c, isBoolean, missing), Collections.emptyMap());
        });
        return loaded == null ? null : loaded.stored();
    }

    /**
     * Same as {@link #acquire(SnapshotKey, Dataset, String, boolean, Map)}
     * for a single-valued keyword (Utf8) column: each stored entry holds
     * the fragment's sorted term dictionary and per-row ordinals. The
     * dictionary size is only known after the scan, so when the missing
     * fragments turn out not to fit the budget the scan is not wasted:
     * their dictionaries come back in {@link KeywordLoad#heap()} for the
     * caller to publish as its request scoped heap columns. Fragments the
     * store already held are in {@link KeywordLoad#stored()} either way.
     *
     * @return the stored and heap parts covering every requested
     *         fragment, or {@code null} when the scan itself failed on an
     *         allocator limit (the caller then loads into heap for
     *         itself)
     */
    public KeywordLoad acquireKeyword(SnapshotKey snapshot, Dataset dataset, String column, Map<Integer, Integer> fragmentRows)
        throws IOException {
        Loaded<CachedKeywordColumn, HeapKeyword> loaded = acquire(
            snapshot,
            dataset,
            column,
            fragmentRows,
            CachedKeywordColumn.class,
            this::loadKeyword
        );
        return loaded == null ? null : new KeywordLoad(loaded.stored(), loaded.heap());
    }

    /**
     * Same as {@link #acquireKeyword} for a multi-valued keyword
     * ({@code List<Utf8>}) column.
     */
    public KeywordArrayLoad acquireKeywordArray(SnapshotKey snapshot, Dataset dataset, String column, Map<Integer, Integer> fragmentRows)
        throws IOException {
        Loaded<CachedKeywordArrayColumn, HeapKeywordArray> loaded = acquire(
            snapshot,
            dataset,
            column,
            fragmentRows,
            CachedKeywordArrayColumn.class,
            this::loadKeywordArray
        );
        return loaded == null ? null : new KeywordArrayLoad(loaded.stored(), loaded.heap());
    }

    /**
     * Shared body of the {@code acquire} methods. Returns the pinned
     * entries of every fragment found or stored plus whatever heap
     * fallback the loader produced, or {@code null} when nothing could be
     * loaded (the loader declined before its scan, or an allocation
     * failed during it); in the {@code null} case nothing stays pinned.
     */
    private <T extends StoreEntry, H> Loaded<T, H> acquire(
        SnapshotKey snapshot,
        Dataset dataset,
        String column,
        Map<Integer, Integer> fragmentRows,
        Class<T> type,
        Loader<T, H> loader
    ) throws IOException {
        Map<Integer, T> found = lookupAndPin(snapshot, column, fragmentRows.keySet(), type);
        if (found.size() == fragmentRows.size()) {
            hits.incrementAndGet();
            return new Loaded<>(found, Collections.emptyMap());
        }
        Object lock = loadLocks.computeIfAbsent(snapshot.indexUuid() + '/' + snapshot.version() + '/' + column, k -> new Object());
        synchronized (lock) {
            boolean handedOut = false;
            try {
                // Another request may have loaded the same slice while
                // this one waited for the lock.
                found.putAll(lookupAndPin(snapshot, column, missingOf(fragmentRows.keySet(), found), type));
                if (found.size() == fragmentRows.size()) {
                    hits.incrementAndGet();
                    handedOut = true;
                    return new Loaded<>(found, Collections.emptyMap());
                }
                Map<Integer, Integer> missing = new HashMap<>();
                for (Map.Entry<Integer, Integer> entry : fragmentRows.entrySet()) {
                    if (!found.containsKey(entry.getKey())) {
                        missing.put(entry.getKey(), entry.getValue());
                    }
                }
                LanceCircuitBreaker.checkAndTrip(BREAKER_LABEL);
                Loaded<T, H> loaded;
                try {
                    loaded = loader.load(dataset, column, missing);
                } catch (OutOfMemoryException oom) {
                    // The estimate undershot the allocator's rounding, or
                    // a concurrent load of another column took the room.
                    // Serve this request from heap; the next one
                    // estimates against the new footprint.
                    budgetMisses.incrementAndGet();
                    LOGGER.debug("column cache has no room for [{}] of {}", column, snapshot, oom);
                    return null;
                }
                if (loaded == null) {
                    budgetMisses.incrementAndGet();
                    return null;
                }
                if (!loaded.heap().isEmpty()) {
                    budgetMisses.incrementAndGet();
                }
                if (!loaded.stored().isEmpty()) {
                    loads.incrementAndGet();
                    synchronized (this) {
                        for (Map.Entry<Integer, T> entry : loaded.stored().entrySet()) {
                            StoreEntry previous = columns.put(new ColumnKey(snapshot, column, entry.getKey()), entry.getValue());
                            if (previous != null) {
                                // Cannot happen while the per-column lock is
                                // held, but never leak a vector if it does.
                                previous.close();
                            }
                            entry.getValue().pin();
                        }
                    }
                    found.putAll(loaded.stored());
                    LOGGER.debug("column cache loaded [{}] for {} fragments of {}", column, loaded.stored().size(), snapshot);
                }
                handedOut = true;
                return new Loaded<>(found, loaded.heap());
            } finally {
                if (!handedOut) {
                    unpin(found.values());
                }
            }
        }
    }

    private static List<Integer> missingOf(Iterable<Integer> wanted, Map<Integer, ?> found) {
        List<Integer> missing = new ArrayList<>();
        for (int fragmentId : wanted) {
            if (!found.containsKey(fragmentId)) {
                missing.add(fragmentId);
            }
        }
        return missing;
    }

    private synchronized <T extends StoreEntry> Map<Integer, T> lookupAndPin(
        SnapshotKey snapshot,
        String column,
        Iterable<Integer> fragmentIds,
        Class<T> type
    ) {
        Map<Integer, T> found = new HashMap<>();
        for (int fragmentId : fragmentIds) {
            StoreEntry cached = columns.get(new ColumnKey(snapshot, column, fragmentId));
            if (cached != null) {
                cached.pin();
                found.put(fragmentId, type.cast(cached));
            }
        }
        return found;
    }

    /** Release the pins the {@code acquire} methods took. Safe to call with entries that were already evicted. */
    public void unpin(Iterable<? extends StoreEntry> pinned) {
        for (StoreEntry entry : pinned) {
            entry.unpin();
        }
    }

    /**
     * Evict least recently used unpinned entries until {@code needed} more
     * bytes fit under the limit.
     *
     * @return whether the room could be made
     */
    private synchronized boolean makeRoom(long needed) {
        if (needed > limitBytes) {
            return false;
        }
        Iterator<Map.Entry<ColumnKey, StoreEntry>> eldest = columns.entrySet().iterator();
        while (allocator.getAllocatedMemory() + needed > limitBytes) {
            StoreEntry victim = null;
            while (eldest.hasNext()) {
                Map.Entry<ColumnKey, StoreEntry> candidate = eldest.next();
                if (!candidate.getValue().isPinned()) {
                    victim = candidate.getValue();
                    eldest.remove();
                    break;
                }
            }
            if (victim == null) {
                return false;
            }
            victim.close();
            evictions.incrementAndGet();
        }
        return true;
    }

    /** Scan of {@code column} over {@code fragmentIds} with {@code _rowaddr}, the shape every load here uses. */
    private static ScanOptions scanOf(String column, Iterable<Integer> fragmentIds) {
        List<Integer> ids = new ArrayList<>();
        for (int id : fragmentIds) {
            ids.add(id);
        }
        return new ScanOptions.Builder().fragmentIds(ids).columns(Collections.singletonList(column)).withRowAddress(true).build();
    }

    /** Receives one scanned cell: the fragment id and row offset from {@code _rowaddr}, the column vector and the row inside it. */
    @FunctionalInterface
    private interface ScannedCellConsumer {
        void accept(int fragmentId, int offset, FieldVector vector, int row);
    }

    /** Run {@code scan} and hand every non-null cell to {@code consumer}. */
    private void scan(Dataset dataset, String column, Iterable<Integer> fragmentIds, ScannedCellConsumer consumer) throws IOException {
        scans.incrementAndGet();
        try (LanceScanner scanner = dataset.newScan(scanOf(column, fragmentIds)); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                FieldVector source = root.getVector(column);
                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount; i++) {
                    if (source.isNull(i)) {
                        continue;
                    }
                    long addr = rowAddr.get(i);
                    consumer.accept((int) (addr >>> 32), (int) (addr & 0xFFFFFFFFL), source, i);
                }
            }
        } catch (OutOfMemoryException oom) {
            throw oom;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * One scan of {@code column} over the fragments in {@code missing},
     * bucketed by the fragment id carried in {@code _rowaddr}. Returns the
     * vectors as {@link CachedColumn}s (not yet in the map, not pinned).
     */
    private Map<Integer, CachedColumn> loadNumeric(Dataset dataset, String column, boolean isBoolean, Map<Integer, Integer> missing)
        throws IOException {
        Map<Integer, FieldVector> vectors = new HashMap<>(missing.size() * 2);
        boolean success = false;
        try {
            for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
                int rows = entry.getValue();
                if (isBoolean) {
                    BitVector v = new BitVector(column, allocator);
                    v.allocateNew(rows);
                    v.setValueCount(rows);
                    vectors.put(entry.getKey(), v);
                } else {
                    BigIntVector v = new BigIntVector(column, allocator);
                    v.allocateNew(rows);
                    v.setValueCount(rows);
                    vectors.put(entry.getKey(), v);
                }
            }
            scan(dataset, column, missing.keySet(), (fragmentId, offset, source, row) -> {
                FieldVector target = vectors.get(fragmentId);
                if (target == null) {
                    return;
                }
                if (isBoolean) {
                    ((BitVector) target).set(offset, ((BitVector) source).get(row));
                } else {
                    ((BigIntVector) target).set(offset, LanceFragmentLeafReader.readAsLong(source, row));
                }
            });
            Map<Integer, CachedColumn> loaded = new HashMap<>(vectors.size() * 2);
            for (Map.Entry<Integer, FieldVector> entry : vectors.entrySet()) {
                loaded.put(entry.getKey(), new CachedColumn(entry.getValue(), missing.get(entry.getKey())));
            }
            success = true;
            return loaded;
        } finally {
            if (!success) {
                closeAll(vectors.values());
            }
        }
    }

    /**
     * One scan of the Utf8 column {@code column} over the fragments in
     * {@code missing}. The values are interned per fragment into a heap
     * {@link KeywordDictionaryBuilder} with one {@code int} id per row;
     * once the scan is done the dictionary and ordinal sizes are known
     * and the budget is checked. When it holds, the sorted terms and
     * remapped ordinals are written into freshly allocated vectors. When
     * it does not (or an allocation still fails), the same builders and
     * ids are finished into {@link HeapKeyword}s instead, so the scan
     * serves the request either way.
     */
    private Loaded<CachedKeywordColumn, HeapKeyword> loadKeyword(Dataset dataset, String column, Map<Integer, Integer> missing)
        throws IOException {
        Map<Integer, int[]> idsByFragment = new HashMap<>(missing.size() * 2);
        Map<Integer, KeywordDictionaryBuilder> builders = new HashMap<>(missing.size() * 2);
        for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
            int[] ids = new int[entry.getValue()];
            Arrays.fill(ids, -1);
            idsByFragment.put(entry.getKey(), ids);
            builders.put(entry.getKey(), new KeywordDictionaryBuilder());
        }
        scan(dataset, column, missing.keySet(), (fragmentId, offset, source, row) -> {
            int[] ids = idsByFragment.get(fragmentId);
            if (ids != null) {
                ids[offset] = builders.get(fragmentId).intern((VarCharVector) source, row);
            }
        });
        long needed = 0L;
        for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
            KeywordDictionaryBuilder builder = builders.get(entry.getKey());
            needed += dictionaryBytes(builder.size(), builder.termBytes()) + intVectorBytes(entry.getValue());
        }
        if (!makeRoom(needed)) {
            return new Loaded<>(Collections.emptyMap(), finishKeywordInHeap(missing.keySet(), builders, idsByFragment));
        }
        List<ValueVector> allocated = new ArrayList<>(missing.size() * 2);
        boolean success = false;
        try {
            Map<Integer, CachedKeywordColumn> loaded = new HashMap<>(missing.size() * 2);
            for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
                int rows = entry.getValue();
                KeywordDictionaryBuilder builder = builders.get(entry.getKey());
                int[] ids = idsByFragment.get(entry.getKey());
                int[] idToOrd = builder.sort();
                VarCharVector terms = new VarCharVector(column, allocator);
                allocated.add(terms);
                terms.allocateNew(builder.termBytes(), builder.size());
                builder.writeTerms(terms);
                IntVector ordinals = new IntVector(column, allocator);
                allocated.add(ordinals);
                ordinals.allocateNew(rows);
                ArrowBuf data = ordinals.getDataBuffer();
                data.setOne(0L, (long) rows * Integer.BYTES);
                for (int doc = 0; doc < rows; doc++) {
                    if (ids[doc] >= 0) {
                        data.setInt((long) doc << 2, idToOrd[ids[doc]]);
                    }
                }
                ordinals.setValueCount(rows);
                loaded.put(entry.getKey(), new CachedKeywordColumn(terms, ordinals, rows));
            }
            success = true;
            return new Loaded<>(loaded, Collections.emptyMap());
        } catch (OutOfMemoryException oom) {
            // The size accounting undershot the allocator, or a concurrent
            // load of another column took the room between makeRoom and
            // the allocation. The builders are intact (sort is
            // idempotent and the ids have not been remapped), so the
            // request still gets its dictionaries without another scan;
            // the shard cache logs the fallback when it publishes them.
            return new Loaded<>(Collections.emptyMap(), finishKeywordInHeap(missing.keySet(), builders, idsByFragment));
        } finally {
            if (!success) {
                closeAll(allocated);
            }
        }
    }

    /**
     * Turn the interned builders and per-row ids of a keyword scan into
     * the heap shape the leaves take: sorted {@link BytesRef} terms and
     * ids remapped in place to ordinals.
     */
    private static Map<Integer, HeapKeyword> finishKeywordInHeap(
        Iterable<Integer> fragmentIds,
        Map<Integer, KeywordDictionaryBuilder> builders,
        Map<Integer, int[]> idsByFragment
    ) {
        Map<Integer, HeapKeyword> heap = new HashMap<>();
        for (int fragmentId : fragmentIds) {
            KeywordDictionaryBuilder.Dictionary dictionary = builders.get(fragmentId).finish();
            int[] ids = idsByFragment.get(fragmentId);
            dictionary.remap(ids);
            heap.put(fragmentId, new HeapKeyword(dictionary.terms(), ids));
        }
        return heap;
    }

    /**
     * {@link #loadKeyword} for a {@code List<Utf8>} column: each row's
     * non-null elements are interned, remapped to strictly ascending
     * duplicate-free ordinals after the sort, and flattened into one
     * ordinal vector addressed through a row offset vector. Without room
     * the remapped rows and the sorted terms go back as
     * {@link HeapKeywordArray}s.
     */
    private Loaded<CachedKeywordArrayColumn, HeapKeywordArray> loadKeywordArray(
        Dataset dataset,
        String column,
        Map<Integer, Integer> missing
    ) throws IOException {
        Map<Integer, int[][]> rowsByFragment = new HashMap<>(missing.size() * 2);
        Map<Integer, KeywordDictionaryBuilder> builders = new HashMap<>(missing.size() * 2);
        for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
            rowsByFragment.put(entry.getKey(), new int[entry.getValue()][]);
            builders.put(entry.getKey(), new KeywordDictionaryBuilder());
        }
        scan(dataset, column, missing.keySet(), (fragmentId, offset, source, row) -> {
            int[][] rows = rowsByFragment.get(fragmentId);
            if (rows != null) {
                ListVector list = (ListVector) source;
                rows[offset] = LanceFragmentLeafReader.internListElements(
                    builders.get(fragmentId),
                    list,
                    (VarCharVector) list.getDataVector(),
                    row
                );
            }
        });
        Map<Integer, Integer> totalOrdinals = new HashMap<>(missing.size() * 2);
        long needed = 0L;
        for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
            KeywordDictionaryBuilder builder = builders.get(entry.getKey());
            int[] idToOrd = builder.sort();
            int[][] rows = rowsByFragment.get(entry.getKey());
            int total = 0;
            for (int r = 0; r < rows.length; r++) {
                if (rows[r] != null) {
                    rows[r] = KeywordDictionaryBuilder.remapSortedUnique(idToOrd, rows[r]);
                    total += rows[r].length;
                }
            }
            totalOrdinals.put(entry.getKey(), total);
            needed += dictionaryBytes(builder.size(), builder.termBytes()) + intVectorBytes(rows.length + 1) + intVectorBytes(total);
        }
        if (!makeRoom(needed)) {
            return new Loaded<>(Collections.emptyMap(), finishKeywordArrayInHeap(missing.keySet(), builders, rowsByFragment));
        }
        List<ValueVector> allocated = new ArrayList<>(missing.size() * 3);
        boolean success = false;
        try {
            Map<Integer, CachedKeywordArrayColumn> loaded = new HashMap<>(missing.size() * 2);
            for (Map.Entry<Integer, Integer> entry : missing.entrySet()) {
                int rowCount = entry.getValue();
                KeywordDictionaryBuilder builder = builders.get(entry.getKey());
                int[][] rows = rowsByFragment.get(entry.getKey());
                int total = totalOrdinals.get(entry.getKey());
                VarCharVector terms = new VarCharVector(column, allocator);
                allocated.add(terms);
                terms.allocateNew(builder.termBytes(), builder.size());
                builder.writeTerms(terms);
                IntVector offsets = new IntVector(column, allocator);
                allocated.add(offsets);
                offsets.allocateNew(rowCount + 1);
                IntVector ordinals = new IntVector(column, allocator);
                allocated.add(ordinals);
                ordinals.allocateNew(total);
                ArrowBuf offsetData = offsets.getDataBuffer();
                ArrowBuf ordinalData = ordinals.getDataBuffer();
                int position = 0;
                offsetData.setInt(0L, 0);
                for (int doc = 0; doc < rowCount; doc++) {
                    int[] row = rows[doc];
                    if (row != null) {
                        for (int ord : row) {
                            ordinalData.setInt((long) position++ << 2, ord);
                        }
                    }
                    offsetData.setInt((long) (doc + 1) << 2, position);
                }
                offsets.setValueCount(rowCount + 1);
                ordinals.setValueCount(total);
                loaded.put(entry.getKey(), new CachedKeywordArrayColumn(terms, offsets, ordinals, rowCount));
            }
            success = true;
            return new Loaded<>(loaded, Collections.emptyMap());
        } catch (OutOfMemoryException oom) {
            // Same situation as in loadKeyword: the rows are already
            // remapped to ordinals and the builders still hold the terms.
            return new Loaded<>(Collections.emptyMap(), finishKeywordArrayInHeap(missing.keySet(), builders, rowsByFragment));
        } finally {
            if (!success) {
                closeAll(allocated);
            }
        }
    }

    /**
     * {@link #finishKeywordInHeap} for a keyword array scan whose rows
     * were already remapped to sorted, duplicate-free ordinals.
     */
    private static Map<Integer, HeapKeywordArray> finishKeywordArrayInHeap(
        Iterable<Integer> fragmentIds,
        Map<Integer, KeywordDictionaryBuilder> builders,
        Map<Integer, int[][]> rowsByFragment
    ) {
        Map<Integer, HeapKeywordArray> heap = new HashMap<>();
        for (int fragmentId : fragmentIds) {
            heap.put(fragmentId, new HeapKeywordArray(builders.get(fragmentId).finish().terms(), rowsByFragment.get(fragmentId)));
        }
        return heap;
    }

    private static void closeAll(Iterable<? extends ValueVector> vectors) {
        for (ValueVector vector : vectors) {
            vector.close();
        }
    }

    /**
     * Bytes the allocator will account for a column of {@code rows} rows.
     * Arrow's fixed width vectors allocate the validity and value buffers
     * as one region (each part rounded up to 8 bytes) and the allocator
     * rounds that region up to a power of two below its chunk size and
     * leaves it exact above.
     */
    static long estimateBytes(int rows, boolean isBoolean) {
        long validity = roundUp8((rows + 7) / 8);
        long data = isBoolean ? validity : roundUp8((long) rows * Long.BYTES);
        return roundedAllocation(validity + data);
    }

    /** Bytes the allocator accounts for an {@link IntVector} of {@code count} values (validity and data as one region). */
    static long intVectorBytes(int count) {
        return roundedAllocation(roundUp8((count + 7) / 8) + roundUp8((long) count * Integer.BYTES));
    }

    /**
     * Bytes the allocator accounts for a {@link VarCharVector} of
     * {@code terms} values totalling {@code termBytes}: the data buffer
     * is one allocation, the validity and offset buffers ({@code terms +
     * 1} offsets) another.
     */
    static long dictionaryBytes(int terms, long termBytes) {
        long offsets = roundUp8((long) (terms + 1) * Integer.BYTES);
        long validity = roundUp8((terms + 1 + 7) / 8);
        return roundedAllocation(termBytes) + roundedAllocation(validity + offsets);
    }

    private static long roundUp8(long size) {
        return (size + 7) & ~7L;
    }

    private static long roundedAllocation(long size) {
        if (size == 0L || size >= ALLOCATOR_CHUNK_SIZE) {
            return size;
        }
        long rounded = 1L;
        while (rounded < size) {
            rounded <<= 1;
        }
        return rounded;
    }

    /** Drop every column of {@code snapshot}. Called when the snapshot closes, so nothing pins them. */
    synchronized void dropSnapshot(SnapshotKey snapshot) {
        Iterator<Map.Entry<ColumnKey, StoreEntry>> it = columns.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ColumnKey, StoreEntry> entry = it.next();
            if (entry.getKey().snapshot().equals(snapshot)) {
                it.remove();
                entry.getValue().close();
            }
        }
    }

    /** Off-heap bytes currently held, as the child allocator accounts them. */
    public long allocatedBytes() {
        return allocator.getAllocatedMemory();
    }

    /** Budget the store was created with. */
    public long limitBytes() {
        return limitBytes;
    }

    /** Number of (snapshot, column, fragment) entries held. */
    public synchronized int entryCount() {
        return columns.size();
    }

    /** Whether {@code (snapshot, column, fragmentId)} is held, for tests. */
    synchronized boolean contains(SnapshotKey snapshot, String column, int fragmentId) {
        // get() would refresh the access order; use containsKey.
        return columns.containsKey(new ColumnKey(snapshot, column, fragmentId));
    }

    /** Requests answered without a scan. */
    public long hitCount() {
        return hits.get();
    }

    /** Scans run to fill missing slices whose result entered the store. */
    public long loadCount() {
        return loads.get();
    }

    /** Lance scans the store ran, whether their result entered the store or went to a request's heap, for tests. */
    long scanCount() {
        return scans.get();
    }

    /** Entries released to make room. */
    public long evictionCount() {
        return evictions.get();
    }

    /**
     * Requests whose missing fragments did not fit the budget: numeric
     * loads that were refused before their scan (the caller scanned into
     * heap for itself) and keyword loads whose scanned dictionaries were
     * handed to the caller's heap instead of being stored.
     */
    public long budgetMissCount() {
        return budgetMisses.get();
    }

    @Override
    public synchronized void close() {
        for (StoreEntry entry : columns.values()) {
            entry.close();
        }
        columns.clear();
        allocator.close();
    }
}
