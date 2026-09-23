/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.query.ScanAdmission;

/**
 * Per-{@link LanceDirectoryReader} coordinator that reads a single
 * Lance column once across the reader's fragment subset and hands each
 * {@link LanceFragmentLeafReader} its fragment's slice. Every
 * {@code ensureXxxLoaded} accessor in the leaf goes through this cache
 * on the fragment-dispatch path; the shard engine's whole-table
 * {@link LanceDirectoryReader#open} path also installs a cache so both
 * lifecycles benefit uniformly.
 *
 * <p>One scan per (column, cache) rather than one per (column, leaf):
 * each {@code dataset.newScan} costs 15-19 ms of FFI overhead on a
 * 20M-row table regardless of how many rows it returns, so a
 * three-column aggregation over ~20 fragments per node would spend
 * 0.9-1.1 s on scan startup alone.
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
 * <p>When the reader was opened over a {@link LanceWarmCache} snapshot the
 * cache also carries the node's {@link ColumnStore}: numeric, boolean and
 * keyword columns are then served from (or loaded into) the store's
 * off-heap vectors, published through
 * {@link LanceFragmentLeafReader#publishOffHeapColumn},
 * {@link LanceFragmentLeafReader#publishOffHeapKeywordColumn} and
 * {@link LanceFragmentLeafReader#publishOffHeapKeywordArrayColumn} and
 * pinned until the reader closes. The heap scan above runs when the store
 * has no room for a numeric or boolean column; a keyword column the
 * store cannot hold is still scanned by the store once and its
 * dictionaries arrive here as heap results, so the heap keyword scan runs
 * only when the store is not usable for keywords at all (no store, a
 * zero budget, or a top-level filter; see {@link #keywordStoreUsable}).
 *
 * <p>Every heap column is charged to the request circuit breaker before
 * its arrays are allocated ({@link #chargeHeap}) and given back when the
 * reader closes ({@link #release}). The breaker is the only bound on this
 * path: a column the store had no room for is as large as the store
 * estimated it, so on a large table a single load can be several
 * gigabytes, and the parent breaker samples real memory too slowly to
 * see an allocation that size before it fails. A refused charge ends the
 * request with a {@link CircuitBreakingException} (HTTP 429) and leaves
 * the node running.
 *
 * <p>Concurrency: a per-column {@code Object} lock serialises
 * concurrent loads of the same column. Different columns load in
 * parallel. The {@code loaded*} sets are used as short-circuit
 * guards after the lock releases so a second caller sees the
 * completion flag through {@code ConcurrentHashMap}'s happens-before.
 */
public final class LanceShardColumnCache {

    private static final Logger LOGGER = LogManager.getLogger(LanceShardColumnCache.class);

    /** Prefix of the label a heap column charge carries on the request breaker; the column name follows. */
    static final String HEAP_LABEL_PREFIX = "lance_heap_column:";

    private static final long FIXED_BIT_SET_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);
    private static final long BYTES_REF_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    private final Dataset dataset;
    private final String filterSql;
    private final Map<Integer, LanceFragmentLeafReader> leavesByFragmentId;
    /**
     * How the column scans of this reader are cut into fragment groups
     * that run side by side, for the heap loads here and for the store's
     * loads. {@link FragmentGroupScan#SEQUENTIAL} (one scan on the
     * calling thread) for readers opened without one.
     */
    private final FragmentGroupScan groupScan;
    /**
     * Request breaker every heap column of this reader is charged to
     * before allocation. A {@link NoopCircuitBreaker} when the reader was
     * opened without one (tests, the fragment path helpers that predate
     * the store).
     */
    private final CircuitBreaker requestBreaker;
    /** Bytes currently charged to {@link #requestBreaker}; given back by {@link #release}. */
    private final AtomicLong heapBytesCharged = new AtomicLong();
    /**
     * Off-heap column store of the node's {@link LanceWarmCache} and the
     * snapshot key the leaves belong to, or {@code null} when the reader
     * was opened outside the cache (shard path, cache disabled, tests).
     * When present, numeric, boolean and keyword loads go to the store
     * first and only fall back to the heap arrays when the store has no
     * room.
     */
    private final ColumnStore columnStore;
    private final LanceWarmCache.SnapshotKey snapshotKey;
    /** Entries pinned in the store on behalf of this reader's leaves; unpinned by {@link #release}. */
    private final List<StoreEntry> pinned = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, Object> columnLocks = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedNumericColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedBooleanColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedTextColumns = new ConcurrentHashMap<>();
    private final Map<String, Boolean> loadedKeywordArrayColumns = new ConcurrentHashMap<>();
    /** Lance scans this cache ran itself for heap loads (not the store's), for tests that count scans per path. */
    private final AtomicLong heapScans = new AtomicLong();

    /**
     * Build a cache scoped to {@code leaves} against {@code dataset}
     * without an off-heap store and without a request breaker: every
     * column loads into the leaves' heap arrays unchecked. For tests and
     * the fragment open paths that have no breaker to hand over.
     */
    LanceShardColumnCache(Dataset dataset, String filterSql, List<LanceFragmentLeafReader> leaves) {
        this(dataset, filterSql, leaves, null, null, new NoopCircuitBreaker(CircuitBreaker.REQUEST), FragmentGroupScan.SEQUENTIAL);
    }

    /**
     * Build a cache scoped to {@code leaves} against {@code dataset}.
     * The list is copied into a fragment-id map so per-leaf lookups
     * during scan iteration are constant time.
     *
     * @param dataset        the shared Lance dataset the reader was opened
     *                       against; every scan in the cache is issued
     *                       against this same handle.
     * @param filterSql      top-level filter to layer into every heap column
     *                       scan (the same value the leaves use in their own
     *                       {@code singleColumnScan}); may be {@code null}
     *                       when the reader was opened without a top-level
     *                       filter push-down. Never applied to store loads.
     * @param leaves         the {@link LanceFragmentLeafReader}s attached
     *                       to the reader, one per fragment in the
     *                       reader's subset.
     * @param columnStore    off-heap store to serve numeric and boolean
     *                       columns from, or {@code null}
     * @param snapshotKey    key of the snapshot the leaves read, required
     *                       when {@code columnStore} is set
     * @param requestBreaker breaker every heap column load is charged to
     *                       before it allocates; the request breaker of
     *                       the node's {@code CircuitBreakerService}, or a
     *                       {@link NoopCircuitBreaker} where none is at
     *                       hand
     * @param groupScan      how every column scan of this reader (heap
     *                       loads here, store loads) is cut into fragment
     *                       groups that run side by side;
     *                       {@link FragmentGroupScan#SEQUENTIAL} for one
     *                       scan on the calling thread
     */
    LanceShardColumnCache(
        Dataset dataset,
        String filterSql,
        List<LanceFragmentLeafReader> leaves,
        ColumnStore columnStore,
        LanceWarmCache.SnapshotKey snapshotKey,
        CircuitBreaker requestBreaker,
        FragmentGroupScan groupScan
    ) {
        this.dataset = dataset;
        this.filterSql = filterSql;
        this.columnStore = columnStore;
        this.snapshotKey = snapshotKey;
        this.requestBreaker = requestBreaker;
        this.groupScan = groupScan;
        Map<Integer, LanceFragmentLeafReader> byId = new HashMap<>(leaves.size() * 2);
        for (LanceFragmentLeafReader leaf : leaves) {
            byId.put(leaf.fragmentId(), leaf);
        }
        this.leavesByFragmentId = Collections.unmodifiableMap(byId);
    }

    /** The request breaker heap column loads of this reader are charged to. */
    CircuitBreaker requestBreaker() {
        return requestBreaker;
    }

    /** Bytes this reader currently has charged to the request breaker for heap columns, for tests and stats. */
    long heapBytesCharged() {
        return heapBytesCharged.get();
    }

    /**
     * Charge {@code bytes} of heap the load of {@code column} is about to
     * allocate to the request breaker. Called before the allocation, so a
     * refusal costs nothing but the exception: the
     * {@link CircuitBreakingException} the breaker raised is rethrown
     * with the column and the bytes it asked for added to its message,
     * same type, same bytes wanted and limit, so the fragment executor
     * reports it as HTTP 429 and an operator can tell which column and
     * how much heap the request wanted. Charged bytes are given back by
     * {@link #releaseHeap} (a load that failed after charging) or by
     * {@link #release} when the reader closes.
     */
    void chargeHeap(long bytes, String column) {
        try {
            requestBreaker.addEstimateBytesAndMaybeBreak(bytes, HEAP_LABEL_PREFIX + column);
        } catch (CircuitBreakingException refused) {
            HeapFallbackStats.rejected();
            // The charge is the admission gate's column_load kind: the
            // breaker judged it before the allocation, the gate records
            // the estimate and the refusal in its stats.
            ScanAdmission.recordColumnLoad(bytes, true);
            CircuitBreakingException reported = new CircuitBreakingException(
                refused.getMessage()
                    + "; the heap copy of column ["
                    + column
                    + "] this request needs ["
                    + bytes
                    + "/"
                    + new ByteSizeValue(bytes)
                    + "] because the column store had no room for it; raise indices.breaker.request.limit,"
                    + " raise lance.cache.column_share, or spread the fragments over more nodes",
                refused.getBytesWanted(),
                refused.getByteLimit(),
                refused.getDurability()
            );
            reported.initCause(refused);
            throw reported;
        }
        heapBytesCharged.addAndGet(bytes);
        HeapFallbackStats.charged(bytes);
        ScanAdmission.recordColumnLoad(bytes, false);
    }

    /** Give back {@code bytes} charged by {@link #chargeHeap} for a load that did not complete. */
    void releaseHeap(long bytes) {
        if (bytes == 0L) {
            return;
        }
        requestBreaker.addWithoutBreaking(-bytes);
        heapBytesCharged.addAndGet(-bytes);
        HeapFallbackStats.released(bytes);
    }

    /**
     * Heap bytes of the {@code long[maxDoc]} values array and the
     * {@code FixedBitSet} presence bitmap a numeric or boolean column of
     * a {@code maxDoc} row fragment occupies, array headers and object
     * alignment included.
     */
    static long numericHeapBytes(int maxDoc) {
        long values = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) maxDoc * Long.BYTES);
        long presenceWords = RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) FixedBitSet.bits2words(maxDoc) * Long.BYTES
        );
        return values + FIXED_BIT_SET_SHALLOW_BYTES + presenceWords;
    }

    /** Heap bytes of an {@code int[length]}. */
    static long intArrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * Integer.BYTES);
    }

    /** Heap bytes of the outer array of an {@code Object[length]} (the references only). */
    static long objectArrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    /**
     * Heap bytes of a sorted dictionary of {@code termCount} distinct
     * terms whose UTF-8 bytes total {@code termBytes}: the
     * {@code BytesRef[]}, one {@code BytesRef} per term and its own
     * {@code byte[]}. Each {@code byte[]} is counted with its header and
     * rounded up to the object alignment, so the figure is an upper
     * bound of what {@link KeywordDictionaryBuilder#finish} allocates and
     * can be computed before it runs.
     */
    static long termsHeapBytes(int termCount, long termBytes) {
        long perTerm = BYTES_REF_SHALLOW_BYTES + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT;
        return objectArrayBytes(termCount) + termCount * perTerm + termBytes;
    }

    /** {@link #termsHeapBytes(int, long)} of a dictionary that is already materialised. */
    static long termsHeapBytes(BytesRef[] terms) {
        long termBytes = 0L;
        for (BytesRef term : terms) {
            termBytes += term.length;
        }
        return termsHeapBytes(terms.length, termBytes);
    }

    /** Heap bytes of the per row ordinal arrays of a keyword array column ({@code null} rows cost nothing beyond their slot). */
    static long rowOrdinalBytes(int[][] rows) {
        long bytes = 0L;
        for (int[] row : rows) {
            if (row != null) {
                bytes += intArrayBytes(row.length);
            }
        }
        return bytes;
    }

    /**
     * Serve {@code name} to every leaf from the off-heap store, loading
     * the fragments the store does not hold yet in one scan. Returns
     * {@code false} when there is no store or the store has no room, in
     * which case the caller loads into heap.
     */
    private boolean publishFromStore(String name, boolean isBoolean) throws IOException {
        if (columnStore == null) {
            return false;
        }
        Map<Integer, Integer> fragmentRows = allFragmentRows();
        Map<Integer, CachedColumn> columns = columnStore.acquire(snapshotKey, dataset, name, isBoolean, fragmentRows, groupScan);
        if (columns == null) {
            LOGGER.debug(
                "column cache budget exhausted; loading [{}] of {} into heap for this request ({} fragments)",
                name,
                snapshotKey,
                fragmentRows.size()
            );
            return false;
        }
        pinned.addAll(columns.values());
        for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
            leaf.publishOffHeapColumn(name, columns.get(leaf.fragmentId()));
        }
        return true;
    }

    /**
     * Serve {@code name} to {@code leaf} alone from the off-heap store,
     * scanning that one fragment when the store does not hold it. Used
     * by a doc values instance that leaves its sparse hint for a doc
     * outside it: the fallback should cost one fragment, not one scan of
     * every fragment in the reader. Returns {@code false} when there is
     * no store or no room, in which case the leaf loads its fragment into
     * heap.
     */
    boolean publishFromStoreForLeaf(LanceFragmentLeafReader leaf, String name, boolean isBoolean) throws IOException {
        if (columnStore == null) {
            return false;
        }
        Map<Integer, CachedColumn> columns = columnStore.acquire(
            snapshotKey,
            dataset,
            name,
            isBoolean,
            Collections.singletonMap(leaf.fragmentId(), leaf.maxDoc()),
            groupScan.sequential()
        );
        if (columns == null) {
            LOGGER.debug(
                "column cache budget exhausted; loading [{}] of {} fragment {} into heap for this request",
                name,
                snapshotKey,
                leaf.fragmentId()
            );
            return false;
        }
        pinned.addAll(columns.values());
        leaf.publishOffHeapColumn(name, columns.get(leaf.fragmentId()));
        return true;
    }

    /**
     * Whether keyword columns may be served from the store for this
     * reader. A store entry has to hold every row of the fragment so any
     * later request over the snapshot can reuse it; the heap keyword load
     * applies {@link #filterSql} to its scan and produces ordinals for
     * the matching rows only, and the store cannot hold both shapes under
     * one key. Readers opened with a top-level filter therefore keep the
     * request scoped heap dictionary for keyword columns. A store with a
     * zero budget can never hold a dictionary, and its keyword loaders
     * only learn that after their scan, so it is skipped up front and the
     * heap load below scans the column once.
     */
    private boolean keywordStoreUsable() {
        return columnStore != null && columnStore.limitBytes() > 0L && filterSql == null;
    }

    /**
     * Whether the off-heap store holds the numeric or boolean column
     * {@code name} of {@code leaf}'s fragment for this reader's snapshot.
     * A lookup in the store's index only: nothing is pinned or loaded,
     * so a leaf can consult it while deciding whether to take its hinted
     * rows instead. {@code false} when the reader has no store.
     */
    boolean storeHoldsColumn(LanceFragmentLeafReader leaf, String name) {
        return columnStore != null && columnStore.contains(snapshotKey, name, leaf.fragmentId());
    }

    /**
     * Keyword counterpart of {@link #storeHoldsColumn}: whether the store
     * holds the dictionary and ordinals of the Utf8 or {@code List<Utf8>}
     * column {@code name} for {@code leaf}'s fragment, in a form this
     * reader may read. {@code false} when the reader carries a top-level
     * filter, because the keyword loaders then stay on the heap path
     * (see {@link #keywordStoreUsable}) and a held entry would not be
     * used.
     */
    boolean storeHoldsKeyword(LanceFragmentLeafReader leaf, String name) {
        return keywordStoreUsable() && columnStore.contains(snapshotKey, name, leaf.fragmentId());
    }

    private Map<Integer, Integer> allFragmentRows() {
        Map<Integer, Integer> fragmentRows = new HashMap<>(leavesByFragmentId.size() * 2);
        for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
            fragmentRows.put(leaf.fragmentId(), leaf.maxDoc());
        }
        return fragmentRows;
    }

    /**
     * The dictionary value encoder of {@code name}, read from the shared
     * schema (every leaf of this reader carries the same one). Non-null
     * only for {@code ip}-overridden columns, whose dictionary terms are
     * the {@code InetAddressPoint} encoding of the stored strings.
     */
    private KeywordDictionaryBuilder.TermEncoder encoderFor(String name) {
        Iterator<LanceFragmentLeafReader> leaves = leavesByFragmentId.values().iterator();
        return leaves.hasNext() ? leaves.next().schema().termEncoder(name) : null;
    }

    /**
     * Keyword counterpart of {@link #publishFromStore}: serve the Utf8
     * column {@code name} to every leaf from the store's dictionaries and
     * ordinals, loading the missing fragments in one scan. Fragments the
     * store could not hold arrive as heap dictionaries built from that
     * same scan and are published through
     * {@link LanceFragmentLeafReader#publishTextColumn}, so this returns
     * {@code true} in both cases and the caller never scans again.
     */
    private boolean publishKeywordFromStore(String name) throws IOException {
        if (!keywordStoreUsable()) {
            return false;
        }
        ColumnStore.KeywordLoad load = columnStore.acquireKeyword(
            snapshotKey,
            dataset,
            name,
            allFragmentRows(),
            groupScan,
            encoderFor(name)
        );
        if (load == null) {
            LOGGER.debug(
                "column cache could not scan [{}] of {}; building the dictionary in heap for this request ({} fragments)",
                name,
                snapshotKey,
                leavesByFragmentId.size()
            );
            return false;
        }
        publishKeywordLoad(name, load, leavesByFragmentId.values());
        return true;
    }

    /** {@link #publishKeywordFromStore} for a {@code List<Utf8>} column. */
    private boolean publishKeywordArrayFromStore(String name) throws IOException {
        if (!keywordStoreUsable()) {
            return false;
        }
        ColumnStore.KeywordArrayLoad load = columnStore.acquireKeywordArray(
            snapshotKey,
            dataset,
            name,
            allFragmentRows(),
            groupScan,
            encoderFor(name)
        );
        if (load == null) {
            LOGGER.debug(
                "column cache could not scan [{}] of {}; building the dictionary in heap for this request ({} fragments)",
                name,
                snapshotKey,
                leavesByFragmentId.size()
            );
            return false;
        }
        publishKeywordArrayLoad(name, load, leavesByFragmentId.values());
        return true;
    }

    /**
     * Keyword counterpart of {@link #publishFromStoreForLeaf}: serve the
     * Utf8 column {@code name} to {@code leaf} alone from the store,
     * scanning that one fragment when the store does not hold it. As in
     * {@link #publishKeywordFromStore}, a dictionary the store has no
     * room for is published to the leaf as a heap column.
     */
    boolean publishKeywordFromStoreForLeaf(LanceFragmentLeafReader leaf, String name) throws IOException {
        if (!keywordStoreUsable()) {
            return false;
        }
        ColumnStore.KeywordLoad load = columnStore.acquireKeyword(
            snapshotKey,
            dataset,
            name,
            Collections.singletonMap(leaf.fragmentId(), leaf.maxDoc()),
            groupScan.sequential(),
            encoderFor(name)
        );
        if (load == null) {
            LOGGER.debug(
                "column cache could not scan [{}] of {} fragment {}; building the dictionary in heap for this request",
                name,
                snapshotKey,
                leaf.fragmentId()
            );
            return false;
        }
        publishKeywordLoad(name, load, Collections.singletonList(leaf));
        return true;
    }

    /** {@link #publishKeywordFromStoreForLeaf} for a {@code List<Utf8>} column. */
    boolean publishKeywordArrayFromStoreForLeaf(LanceFragmentLeafReader leaf, String name) throws IOException {
        if (!keywordStoreUsable()) {
            return false;
        }
        ColumnStore.KeywordArrayLoad load = columnStore.acquireKeywordArray(
            snapshotKey,
            dataset,
            name,
            Collections.singletonMap(leaf.fragmentId(), leaf.maxDoc()),
            groupScan.sequential(),
            encoderFor(name)
        );
        if (load == null) {
            LOGGER.debug(
                "column cache could not scan [{}] of {} fragment {}; building the dictionary in heap for this request",
                name,
                snapshotKey,
                leaf.fragmentId()
            );
            return false;
        }
        publishKeywordArrayLoad(name, load, Collections.singletonList(leaf));
        return true;
    }

    /**
     * Hand each leaf in {@code leaves} its fragment's part of
     * {@code load}: the store entry, pinned until {@link #release}, or
     * the heap dictionary the store scanned but could not keep. The heap
     * dictionaries are charged to the request breaker as one sum before
     * any leaf receives them; the store built them during its scan, so
     * this is the earliest point the reader can account for them.
     */
    private void publishKeywordLoad(String name, ColumnStore.KeywordLoad load, Iterable<LanceFragmentLeafReader> leaves) {
        pinned.addAll(load.stored().values());
        if (!load.heap().isEmpty()) {
            LOGGER.debug(
                "column cache budget exhausted; serving the scanned dictionary of [{}] of {} from heap for this request ({} fragments)",
                name,
                snapshotKey,
                load.heap().size()
            );
            long heapBytes = 0L;
            for (ColumnStore.HeapKeyword heap : load.heap().values()) {
                heapBytes += termsHeapBytes(heap.terms()) + intArrayBytes(heap.ords().length);
            }
            chargeHeap(heapBytes, name);
        }
        for (LanceFragmentLeafReader leaf : leaves) {
            CachedKeywordColumn stored = load.stored().get(leaf.fragmentId());
            if (stored != null) {
                leaf.publishOffHeapKeywordColumn(name, stored);
                continue;
            }
            ColumnStore.HeapKeyword heap = load.heap().get(leaf.fragmentId());
            if (heap == null) {
                throw new IllegalStateException(
                    "column store returned neither a stored nor a heap dictionary of [" + name + "] for fragment " + leaf.fragmentId()
                );
            }
            leaf.publishTextColumn(name, heap.terms(), heap.ords());
        }
    }

    /** {@link #publishKeywordLoad} for a {@code List<Utf8>} column. */
    private void publishKeywordArrayLoad(String name, ColumnStore.KeywordArrayLoad load, Iterable<LanceFragmentLeafReader> leaves) {
        pinned.addAll(load.stored().values());
        if (!load.heap().isEmpty()) {
            LOGGER.debug(
                "column cache budget exhausted; serving the scanned dictionary of [{}] of {} from heap for this request ({} fragments)",
                name,
                snapshotKey,
                load.heap().size()
            );
            long heapBytes = 0L;
            for (ColumnStore.HeapKeywordArray heap : load.heap().values()) {
                heapBytes += termsHeapBytes(heap.terms()) + objectArrayBytes(heap.rowOrds().length) + rowOrdinalBytes(heap.rowOrds());
            }
            chargeHeap(heapBytes, name);
        }
        for (LanceFragmentLeafReader leaf : leaves) {
            CachedKeywordArrayColumn stored = load.stored().get(leaf.fragmentId());
            if (stored != null) {
                leaf.publishOffHeapKeywordArrayColumn(name, stored);
                continue;
            }
            ColumnStore.HeapKeywordArray heap = load.heap().get(leaf.fragmentId());
            if (heap == null) {
                throw new IllegalStateException(
                    "column store returned neither a stored nor a heap dictionary of [" + name + "] for fragment " + leaf.fragmentId()
                );
            }
            leaf.publishKeywordArrayColumn(name, heap.terms(), heap.rowOrds());
        }
    }

    /**
     * Unpin every store entry this reader's leaves were served and give
     * the request breaker back every heap byte the loaders charged
     * (including the charges the leaves routed here from their single
     * fragment loads). Called once from {@link LanceDirectoryReader#doClose};
     * the store may evict the entries afterwards and the heap arrays
     * become garbage with the leaves.
     */
    void release() {
        long charged = heapBytesCharged.getAndSet(0L);
        if (charged != 0L) {
            requestBreaker.addWithoutBreaking(-charged);
            HeapFallbackStats.released(charged);
        }
        if (columnStore == null) {
            return;
        }
        List<StoreEntry> toRelease;
        synchronized (pinned) {
            toRelease = new ArrayList<>(pinned);
            pinned.clear();
        }
        columnStore.unpin(toRelease);
    }

    /** Number of Lance scans (one per fragment group) the heap loaders below have run for this reader, for tests. */
    long heapScanCount() {
        return heapScans.get();
    }

    /** Receives one non-null cell of a heap column scan: the fragment id and row offset from {@code _rowaddr}, the column vector and the row inside it. */
    @FunctionalInterface
    private interface HeapCellConsumer {
        void accept(int fragmentId, int offset, FieldVector vector, int row);
    }

    /**
     * Scan {@code name} over every fragment of this reader, the cache's
     * top-level filter included, and hand every non-null cell to
     * {@code consumer}. The fragments are cut into contiguous groups by
     * {@link #groupScan} and the groups run side by side, so
     * {@code consumer} is called from several threads at once; the
     * loaders below write each cell into the array or dictionary builder
     * of its own fragment and a fragment is in exactly one group, so no
     * two threads write the same structure. A cell of a fragment outside
     * the reader is dropped before it reaches {@code consumer}: it should
     * not occur because the scan is scoped by fragment ids, but the guard
     * keeps an unexpected batch from throwing.
     */
    private void scanHeap(String name, HeapCellConsumer consumer) throws IOException {
        List<Integer> fragmentIds = new ArrayList<>(leavesByFragmentId.keySet());
        if (fragmentIds.isEmpty()) {
            return;
        }
        Collections.sort(fragmentIds);
        try {
            groupScan.run(fragmentIds, group -> {
                scanHeapGroup(name, group, consumer);
                return null;
            });
        } catch (IOException | TaskCancelledException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /** One Lance scan of {@link #scanHeap}: {@code name} over {@code fragmentIds} with {@code _rowaddr} and the top-level filter. */
    private void scanHeapGroup(String name, List<Integer> fragmentIds, HeapCellConsumer consumer) throws IOException {
        ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(fragmentIds)
            .columns(Collections.singletonList(name))
            .withRowAddress(true);
        if (filterSql != null) {
            builder = builder.filter(filterSql);
        }
        heapScans.incrementAndGet();
        try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                // A cancelled request stops at the next batch; the
                // arrays filled so far are dropped by the caller.
                groupScan.cancellation().checkCancelled();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                FieldVector vector = root.getVector(name);
                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount; i++) {
                    if (vector.isNull(i)) {
                        continue;
                    }
                    long addr = rowAddr.get(i);
                    int fragmentId = (int) (addr >>> 32);
                    if (!leavesByFragmentId.containsKey(fragmentId)) {
                        continue;
                    }
                    consumer.accept(fragmentId, (int) (addr & 0xFFFFFFFFL), vector, i);
                }
            }
        } catch (IOException | TaskCancelledException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Load {@code name} into every leaf's numeric column cache. No-op
     * when the column has already been loaded for this cache instance
     * (idempotent by design; callers hit this method every time they
     * fault into their own {@code ensureNumericLoaded} branch).
     *
     * <p>Charges the {@code long[maxDoc]} + {@code FixedBitSet} pair
     * of every leaf to the request breaker as one sum, then
     * preallocates them before opening the scan; the sizes come
     * from each leaf's {@code physicalRows} which was fixed at
     * fragment metadata read time and does not change. A refused
     * charge leaves nothing allocated.
     *
     * <p>The scan is {@code newScan(fragmentIds = group, columns =
     * [name], withRowAddress = true)} per fragment group of
     * {@link #scanHeap}, plus the cache's top-level filter if present.
     * Each row's {@code _rowaddr} splits into (fragmentId, offset); the
     * row's value goes into the fragment's leaf.
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
            if (publishFromStore(name, false)) {
                loadedNumericColumns.put(name, Boolean.TRUE);
                return;
            }
            long heapBytes = numericHeapBytesOfAllLeaves();
            chargeHeap(heapBytes, name);
            boolean published = false;
            try {
                Map<Integer, long[]> valuesByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                Map<Integer, FixedBitSet> presenceByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    int maxDoc = leaf.maxDoc();
                    valuesByFragment.put(leaf.fragmentId(), new long[maxDoc]);
                    presenceByFragment.put(leaf.fragmentId(), new FixedBitSet(maxDoc));
                }
                scanHeap(name, (fragmentId, offset, vector, row) -> {
                    valuesByFragment.get(fragmentId)[offset] = LanceFragmentLeafReader.readAsLong(vector, row);
                    presenceByFragment.get(fragmentId).set(offset);
                });
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    leaf.publishNumericColumn(name, valuesByFragment.get(leaf.fragmentId()), presenceByFragment.get(leaf.fragmentId()));
                }
                loadedNumericColumns.put(name, Boolean.TRUE);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(heapBytes);
                }
            }
        }
    }

    /** {@link #numericHeapBytes} summed over every leaf of the reader. */
    private long numericHeapBytesOfAllLeaves() {
        long bytes = 0L;
        for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
            bytes += numericHeapBytes(leaf.maxDoc());
        }
        return bytes;
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
            if (publishFromStore(name, true)) {
                loadedBooleanColumns.put(name, Boolean.TRUE);
                return;
            }
            long heapBytes = numericHeapBytesOfAllLeaves();
            chargeHeap(heapBytes, name);
            boolean published = false;
            try {
                Map<Integer, long[]> valuesByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                Map<Integer, FixedBitSet> presenceByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    int maxDoc = leaf.maxDoc();
                    valuesByFragment.put(leaf.fragmentId(), new long[maxDoc]);
                    presenceByFragment.put(leaf.fragmentId(), new FixedBitSet(maxDoc));
                }
                scanHeap(name, (fragmentId, offset, vector, row) -> {
                    valuesByFragment.get(fragmentId)[offset] = ((BitVector) vector).get(row);
                    presenceByFragment.get(fragmentId).set(offset);
                });
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    leaf.publishBooleanColumn(name, valuesByFragment.get(leaf.fragmentId()), presenceByFragment.get(leaf.fragmentId()));
                }
                loadedBooleanColumns.put(name, Boolean.TRUE);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(heapBytes);
                }
            }
        }
    }

    /**
     * Load a Utf8 column across the fragment subset and build each
     * fragment's keyword dictionary. The scan streams every value once
     * into a per-fragment {@link KeywordDictionaryBuilder} (distinct
     * terms interned into a byte pool, one {@code int} id per row);
     * after the scan each builder is sorted and the ids are remapped to
     * ordinals before the leaf receives them through
     * {@link LanceFragmentLeafReader#publishTextColumn}. No per-row
     * {@link String} is created; a {@code String[maxDoc]} intermediate
     * would cost about 2 GB per 20M-row query. Keyword dictionaries
     * stay per-fragment because that is
     * what {@link org.apache.lucene.index.SortedDocValues} expects for
     * ord-comparison semantics.
     *
     * <p>The request breaker is charged in two steps, each before the
     * allocation it covers: the {@code int[maxDoc]} ordinal arrays of
     * every leaf before the scan, and the sorted {@code BytesRef[]}
     * dictionaries once the scan has fixed each builder's term count and
     * byte total, before {@link KeywordDictionaryBuilder#finish} copies
     * the terms out of the pool. A refusal at the second step gives the
     * first step's charge back.
     *
     * <p>With a store and no top-level filter the dictionary and
     * ordinals come from (or go into) the store's off-heap vectors
     * instead, so later requests over the snapshot skip the scan and the
     * heap build; when the store scans the column but has no room for
     * the result, the leaves receive that scan's dictionaries as heap
     * columns and the loop below does not run. See
     * {@link #keywordStoreUsable} for why a filtered load or a zero
     * budget stays here.
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
            if (publishKeywordFromStore(name)) {
                loadedTextColumns.put(name, Boolean.TRUE);
                return;
            }
            long ordinalBytes = 0L;
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                ordinalBytes += intArrayBytes(leaf.maxDoc());
            }
            chargeHeap(ordinalBytes, name);
            long charged = ordinalBytes;
            boolean published = false;
            try {
                Map<Integer, int[]> idsByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                Map<Integer, KeywordDictionaryBuilder> buildersByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                KeywordDictionaryBuilder.TermEncoder encoder = encoderFor(name);
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    int[] ids = new int[leaf.maxDoc()];
                    java.util.Arrays.fill(ids, -1);
                    idsByFragment.put(leaf.fragmentId(), ids);
                    buildersByFragment.put(leaf.fragmentId(), new KeywordDictionaryBuilder(encoder));
                }
                scanHeap(name, (fragmentId, offset, vector, row) -> {
                    idsByFragment.get(fragmentId)[offset] = buildersByFragment.get(fragmentId).intern((VarCharVector) vector, row);
                });
                for (Map.Entry<Integer, KeywordDictionaryBuilder> entry : buildersByFragment.entrySet()) {
                    IpTermEncoder.logInvalid(name, entry.getKey(), entry.getValue().invalidCount());
                }
                long termBytes = 0L;
                for (KeywordDictionaryBuilder dictionaryBuilder : buildersByFragment.values()) {
                    termBytes += termsHeapBytes(dictionaryBuilder.size(), dictionaryBuilder.termBytes());
                }
                chargeHeap(termBytes, name);
                charged += termBytes;
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    int[] ids = idsByFragment.get(leaf.fragmentId());
                    KeywordDictionaryBuilder.Dictionary dictionary = buildersByFragment.get(leaf.fragmentId()).finish();
                    dictionary.remap(ids);
                    leaf.publishTextColumn(name, dictionary.terms(), ids);
                }
                loadedTextColumns.put(name, Boolean.TRUE);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(charged);
                }
            }
        }
    }

    /**
     * Load a {@code List<Utf8>} column across the fragment subset and
     * build each fragment's multi-valued keyword dictionary. Same
     * interning scheme as {@link #loadTextColumn}; each doc keeps an
     * {@code int[]} of element ids (null for an Arrow-null list) that
     * is remapped to a sorted, duplicate-free ordinal array before the
     * leaf receives it through
     * {@link LanceFragmentLeafReader#publishKeywordArrayColumn}. Served
     * from the store under the same conditions as {@link #loadTextColumn}.
     *
     * <p>Breaker charges: the {@code int[maxDoc][]} outer arrays before
     * the scan; the per row ordinal arrays and the dictionaries once the
     * rows are remapped and the builders are fixed, before the terms are
     * copied out. The per row arrays are created while the scan runs
     * (their lengths are only known row by row), so they are the one
     * allocation on this path that is charged after the fact.
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
            if (publishKeywordArrayFromStore(name)) {
                loadedKeywordArrayColumns.put(name, Boolean.TRUE);
                return;
            }
            long outerBytes = 0L;
            for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                outerBytes += objectArrayBytes(leaf.maxDoc());
            }
            chargeHeap(outerBytes, name);
            long charged = outerBytes;
            boolean published = false;
            try {
                Map<Integer, int[][]> rowsByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                Map<Integer, KeywordDictionaryBuilder> buildersByFragment = new HashMap<>(leavesByFragmentId.size() * 2);
                KeywordDictionaryBuilder.TermEncoder encoder = encoderFor(name);
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    rowsByFragment.put(leaf.fragmentId(), new int[leaf.maxDoc()][]);
                    buildersByFragment.put(leaf.fragmentId(), new KeywordDictionaryBuilder(encoder));
                }
                scanHeap(name, (fragmentId, offset, vector, row) -> {
                    ListVector list = (ListVector) vector;
                    rowsByFragment.get(fragmentId)[offset] = LanceFragmentLeafReader.internListElements(
                        buildersByFragment.get(fragmentId),
                        list,
                        (VarCharVector) list.getDataVector(),
                        row
                    );
                });
                for (Map.Entry<Integer, KeywordDictionaryBuilder> entry : buildersByFragment.entrySet()) {
                    IpTermEncoder.logInvalid(name, entry.getKey(), entry.getValue().invalidCount());
                }
                Map<Integer, KeywordDictionaryBuilder.Dictionary> dictionaries = new HashMap<>(leavesByFragmentId.size() * 2);
                long rowAndTermBytes = 0L;
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    int[][] rows = rowsByFragment.get(leaf.fragmentId());
                    KeywordDictionaryBuilder dictionaryBuilder = buildersByFragment.get(leaf.fragmentId());
                    int[] idToOrd = dictionaryBuilder.sort();
                    for (int r = 0; r < rows.length; r++) {
                        if (rows[r] != null) {
                            rows[r] = KeywordDictionaryBuilder.remapSortedUnique(idToOrd, rows[r]);
                        }
                    }
                    rowAndTermBytes += rowOrdinalBytes(rows) + termsHeapBytes(dictionaryBuilder.size(), dictionaryBuilder.termBytes());
                }
                chargeHeap(rowAndTermBytes, name);
                charged += rowAndTermBytes;
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    dictionaries.put(leaf.fragmentId(), buildersByFragment.get(leaf.fragmentId()).finish());
                }
                for (LanceFragmentLeafReader leaf : leavesByFragmentId.values()) {
                    leaf.publishKeywordArrayColumn(
                        name,
                        dictionaries.get(leaf.fragmentId()).terms(),
                        rowsByFragment.get(leaf.fragmentId())
                    );
                }
                loadedKeywordArrayColumns.put(name, Boolean.TRUE);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(charged);
                }
            }
        }
    }
}
