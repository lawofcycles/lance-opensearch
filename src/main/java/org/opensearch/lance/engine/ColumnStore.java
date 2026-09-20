/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.engine.LanceWarmCache.SnapshotKey;

/**
 * Node-wide off-heap store of numeric and boolean columns, one
 * {@link CachedColumn} per (snapshot, column, fragment). Every entry is an
 * Arrow vector allocated from one child allocator whose limit is the
 * column cache budget, so {@link #allocatedBytes()} is the exact off-heap
 * footprint the {@code lance_native} circuit breaker has to account for.
 *
 * <p>Loading is one Lance scan per (snapshot, column) over the fragments
 * a request needs and does not have yet, without any filter, so the
 * loaded slice serves every later request over the same snapshot
 * regardless of its query. The scan's batches are read once and each
 * value is written, normalised, into the fragment's vector.
 *
 * <p>Eviction is least recently used at (column, fragment) granularity
 * and skips entries a running request has pinned. When the budget cannot
 * be met even after evicting everything unpinned, {@link #acquire}
 * returns {@code null} and the caller falls back to its request scoped
 * heap load.
 */
public final class ColumnStore implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(ColumnStore.class);

    /** Label the circuit breaker check reports when a column load trips it. */
    static final String BREAKER_LABEL = "lance_column_cache";

    /** Buffers at or above this size are allocated exactly; smaller ones round up to a power of two. */
    private static final long ALLOCATOR_CHUNK_SIZE = 16L * 1024 * 1024;

    private record ColumnKey(SnapshotKey snapshot, String column, int fragmentId) {
    }

    private final BufferAllocator allocator;
    private final long limitBytes;
    /** Access ordered so iteration starts at the least recently used entry. Guarded by {@code this}. */
    private final LinkedHashMap<ColumnKey, CachedColumn> columns = new LinkedHashMap<>(256, 0.75f, true);
    /** Serialises the load of one (snapshot, column) so two requests do not scan it twice. */
    private final Map<String, Object> loadLocks = new ConcurrentHashMap<>();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong loads = new AtomicLong();
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
     * Return the columns for {@code fragmentRows} (fragment id to row
     * count), loading the missing ones with one scan of {@code dataset},
     * and pin every returned column. The caller unpins them through
     * {@link #unpin} when its request ends.
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
        Map<Integer, CachedColumn> found = lookupAndPin(snapshot, column, fragmentRows.keySet());
        if (found.size() == fragmentRows.size()) {
            hits.incrementAndGet();
            return found;
        }
        Object lock = loadLocks.computeIfAbsent(snapshot.indexUuid() + '/' + snapshot.version() + '/' + column, k -> new Object());
        synchronized (lock) {
            boolean handedOut = false;
            try {
                // Another request may have loaded the same slice while
                // this one waited for the lock.
                found.putAll(lookupAndPin(snapshot, column, missingOf(fragmentRows.keySet(), found)));
                if (found.size() == fragmentRows.size()) {
                    hits.incrementAndGet();
                    handedOut = true;
                    return found;
                }
                Map<Integer, Integer> missing = new HashMap<>();
                for (Map.Entry<Integer, Integer> entry : fragmentRows.entrySet()) {
                    if (!found.containsKey(entry.getKey())) {
                        missing.put(entry.getKey(), entry.getValue());
                    }
                }
                LanceCircuitBreaker.checkAndTrip(BREAKER_LABEL);
                long needed = 0L;
                for (int rows : missing.values()) {
                    needed += estimateBytes(rows, isBoolean);
                }
                if (!makeRoom(needed)) {
                    budgetMisses.incrementAndGet();
                    return null;
                }
                Map<Integer, CachedColumn> loaded;
                try {
                    loaded = load(dataset, column, isBoolean, missing);
                } catch (OutOfMemoryException oom) {
                    // The estimate undershot the allocator's rounding, or
                    // a concurrent load of another column took the room.
                    // Serve this request from heap; the next one
                    // estimates against the new footprint.
                    budgetMisses.incrementAndGet();
                    LOGGER.debug("column cache has no room for [{}] of {} ({} bytes needed)", column, snapshot, needed, oom);
                    return null;
                }
                loads.incrementAndGet();
                synchronized (this) {
                    for (Map.Entry<Integer, CachedColumn> entry : loaded.entrySet()) {
                        CachedColumn previous = columns.put(new ColumnKey(snapshot, column, entry.getKey()), entry.getValue());
                        if (previous != null) {
                            // Cannot happen while the per-column lock is
                            // held, but never leak a vector if it does.
                            previous.close();
                        }
                        entry.getValue().pin();
                    }
                }
                found.putAll(loaded);
                LOGGER.debug("column cache loaded [{}] for {} fragments of {}", column, missing.size(), snapshot);
                handedOut = true;
                return found;
            } finally {
                if (!handedOut) {
                    unpin(found.values());
                }
            }
        }
    }

    private static List<Integer> missingOf(Iterable<Integer> wanted, Map<Integer, CachedColumn> found) {
        List<Integer> missing = new ArrayList<>();
        for (int fragmentId : wanted) {
            if (!found.containsKey(fragmentId)) {
                missing.add(fragmentId);
            }
        }
        return missing;
    }

    private synchronized Map<Integer, CachedColumn> lookupAndPin(SnapshotKey snapshot, String column, Iterable<Integer> fragmentIds) {
        Map<Integer, CachedColumn> found = new HashMap<>();
        for (int fragmentId : fragmentIds) {
            CachedColumn cached = columns.get(new ColumnKey(snapshot, column, fragmentId));
            if (cached != null) {
                cached.pin();
                found.put(fragmentId, cached);
            }
        }
        return found;
    }

    /** Release the pins {@link #acquire} took. Safe to call with columns that were already evicted. */
    public void unpin(Iterable<CachedColumn> pinned) {
        for (CachedColumn column : pinned) {
            column.unpin();
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
        Iterator<Map.Entry<ColumnKey, CachedColumn>> eldest = columns.entrySet().iterator();
        while (allocator.getAllocatedMemory() + needed > limitBytes) {
            CachedColumn victim = null;
            while (eldest.hasNext()) {
                Map.Entry<ColumnKey, CachedColumn> candidate = eldest.next();
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

    /**
     * One scan of {@code column} over the fragments in {@code missing},
     * bucketed by the fragment id carried in {@code _rowaddr}. Returns the
     * vectors as {@link CachedColumn}s (not yet in the map, not pinned).
     */
    private Map<Integer, CachedColumn> load(Dataset dataset, String column, boolean isBoolean, Map<Integer, Integer> missing)
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
            ScanOptions options = new ScanOptions.Builder().fragmentIds(new ArrayList<>(missing.keySet()))
                .columns(Collections.singletonList(column))
                .withRowAddress(true)
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
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
                        FieldVector target = vectors.get((int) (addr >>> 32));
                        if (target == null) {
                            continue;
                        }
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        if (isBoolean) {
                            ((BitVector) target).set(offset, ((BitVector) source).get(i));
                        } else {
                            ((BigIntVector) target).set(offset, LanceFragmentLeafReader.readAsLong(source, i));
                        }
                    }
                }
            } catch (OutOfMemoryException oom) {
                throw oom;
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            Map<Integer, CachedColumn> loaded = new HashMap<>(vectors.size() * 2);
            for (Map.Entry<Integer, FieldVector> entry : vectors.entrySet()) {
                loaded.put(entry.getKey(), new CachedColumn(entry.getValue(), missing.get(entry.getKey())));
            }
            success = true;
            return loaded;
        } finally {
            if (!success) {
                for (FieldVector vector : vectors.values()) {
                    vector.close();
                }
            }
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

    private static long roundUp8(long size) {
        return (size + 7) & ~7L;
    }

    private static long roundedAllocation(long size) {
        if (size >= ALLOCATOR_CHUNK_SIZE) {
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
        Iterator<Map.Entry<ColumnKey, CachedColumn>> it = columns.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ColumnKey, CachedColumn> entry = it.next();
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

    /** Scans run to fill missing slices. */
    public long loadCount() {
        return loads.get();
    }

    /** Entries released to make room. */
    public long evictionCount() {
        return evictions.get();
    }

    /** Requests that fell back to a heap load because the budget could not be met. */
    public long budgetMissCount() {
        return budgetMisses.get();
    }

    @Override
    public synchronized void close() {
        for (CachedColumn column : columns.values()) {
            column.close();
        }
        columns.clear();
        allocator.close();
    }
}
