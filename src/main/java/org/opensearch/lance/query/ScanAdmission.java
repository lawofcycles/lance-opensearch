/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.lance.Dataset;
import org.lance.index.IndexType;
import org.lance.ipc.FullTextQuery;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.plan.metadata.ColumnStatistics;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.metadata.TableStatisticsCache;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.secure_sm.AccessController;

/**
 * Admission control for every native scan and index load of the
 * fragment path: one decision, several estimators.
 *
 * <p>Lance allocates native memory the plugin's breakers never see. The
 * {@code lance_native} breaker samples the Session caches, so an entry
 * heavier than one cache shard (an inverted index document set, the
 * matching pages of a large BTree) is rebuilt outside every budget; the
 * scan buffers Lance holds while a scan runs (the I/O queue, the decoded
 * batches in flight, the row addresses a scalar index materialises) are
 * the native allocator's and are never cached; the {@code request}
 * breaker counts the plugin's heap buffers only. On a table large enough
 * any scan that touches such an allocation ends the node with a kernel
 * OOM kill before any accounting sees it. The only lever the plugin has
 * is refusing the scan before it starts.
 *
 * <p>Every gated path estimates what its scan will make Lance allocate
 * ({@link Kind} names the paths, the {@code ...EstimateBytes} methods
 * hold the formulas with their coefficients as named constants) and
 * passes the estimate to one decision: the node's available physical
 * memory plus the memory earlier admitted scans retained, minus a
 * headroom, must hold it, else the request is refused with
 * {@link CircuitBreakingException} (HTTP 429) under the label
 * {@link #LABEL}. The metadata warm up's full text probe is judged by
 * the same decision through {@link #admitWarmUpProbe}, and skipped
 * instead of refused because no request waits for it ({@link Source}
 * tells the two apart in the stats). An estimate that fits the index
 * cache shard share is
 * zero: for the cached kinds because a fitting entry is loaded once and
 * kept, for the scan kinds because buffers smaller than one shard of the
 * cache the node already dedicates to Lance are within its sizing. Heap
 * terms (the filter's per fragment bit sets, an aggregate's group state)
 * are compared with the room left in the request breaker and reported
 * in the message, never against physical memory.
 *
 * <p>Available memory is the kernel's {@code MemAvailable} from
 * {@code /proc/meminfo}, which counts the reclaimable page cache and
 * slab the kernel would hand to the scan on top of the free pages;
 * {@code MemFree} alone reads close to zero on a node whose page cache
 * is warm from serving queries. Where the file or the line does not
 * exist (macOS, Windows) the probe falls back to {@link OsProbe}'s free
 * physical memory.
 *
 * <p>An admitted scan leaves memory behind: the entries Lance admits to
 * its index cache next to the refused one and the pages the native
 * allocator keeps after the refused entry is dropped. {@code MemAvailable}
 * therefore reads lower after the scan than before it, while the next
 * scan of the same table reuses that memory instead of allocating it
 * again. The gate keeps a per node {@link RetainedPool} of this memory:
 * the difference between {@code MemAvailable} sampled when a non zero
 * estimate is admitted with nothing else in flight and {@code MemAvailable}
 * sampled when that request's scan completes, at most the process's
 * resident set growth over the same interval, accumulated over scans,
 * bounded by the largest estimate admitted since the pool started, and
 * decayed as {@code MemAvailable} recovers. The decision adds the pool
 * to the available memory. The pool is credited only while no other
 * gated request is in flight and no gated scan is running (their memory
 * is in use, not reusable), and not when the process's resident set
 * exceeds {@code lance.native_memory.limit} plus the JVM heap by more
 * than the pool (something the plugin does not account holds memory).
 * Every gated scan reports itself through {@link #scanStarted} and
 * {@link #scanFinished}; {@link LanceHitsAccounting#close} reports the
 * end of the request through {@link #requestEnded(LanceHitsAccounting)},
 * so one request counts once in flight however many of its paths were
 * admitted, and is released whether or not its scans ran.
 *
 * <p>The settings ({@code lance.admission.enabled},
 * {@code lance.admission.headroom} and
 * {@code lance.admission.bounded_shapes_gated}) live in static holders
 * read by every decision, the same pattern as {@link LanceFtsQuery}'s
 * probe parameters. {@code lance.test.index_cache_shard_share} overrides
 * the shard share the decision compares with so integration tests can
 * declare a small fixture table as not fitting; zero (the default) reads
 * the installed Session's sizing. {@code lance.test.admission_available_memory}
 * scripts the available memory readings.
 */
public final class ScanAdmission {

    /** Label the 429 is reported under, and the prefix of its message. */
    public static final String LABEL = "lance_admission";

    /** Default for {@code lance.admission.headroom}. */
    public static final ByteSizeValue DEFAULT_HEADROOM = new ByteSizeValue(8, ByteSizeUnit.GB);

    /**
     * The gated paths. The key is the one the 429 message, the stats
     * block ({@code admission.rejections.<key>}, {@code admission.last_kind})
     * and the changelog use.
     */
    public enum Kind {
        /** A full text scan over an inverted index (the document set rebuild plus the hits scan buffers). */
        FTS("fts", "full text scan"),
        /** The load of a scalar index (BTree pages, bitmaps, a zone map) that answers a filter. */
        SCALAR_INDEX("scalar_index", "scalar index load"),
        /** The load of the IVF partitions a nearest scan probes, and its refine reads. */
        VECTOR_INDEX("vector_index", "vector index load"),
        /** The native scan of a filter (row addresses materialised and streamed) or of a sorted top-k page. */
        FILTER_SCAN("filter_scan", "filter scan"),
        /** The parallel scans of a pushed aggregate. */
        AGGREGATE_SCAN("aggregate_scan", "aggregate scan"),
        /** The heap copy of a column the off-heap store had no room for. */
        COLUMN_LOAD("column_load", "column load");

        private final String key;
        private final String description;

        Kind(String key, String description) {
            this.key = key;
            this.description = description;
        }

        /** The stats and message key: {@code fts}, {@code scalar_index}, ... */
        public String key() {
            return key;
        }

        /** Human wording for the 429 message. */
        public String description() {
            return description;
        }
    }

    /**
     * Where a decision came from. The key is the one the stats block
     * ({@code admission.last_source}) uses.
     */
    public enum Source {
        /** A gated path of a search request; a refusal answers 429. */
        REQUEST("request"),
        /** The metadata warm up's full text probe; a refusal skips the probe. */
        WARM_UP("warm_up");

        private final String key;

        Source(String key) {
            this.key = key;
        }

        /** The stats key: {@code request} or {@code warm_up}. */
        public String key() {
            return key;
        }
    }

    /**
     * The shape the metadata warm up's full text probe is judged as: a
     * bounded page of one row ({@code match(<token>) limit 1}). The
     * probe rebuilds the same document set a query does, so its
     * estimate is the {@link Kind#FTS} one for that page.
     */
    public static final Shape WARM_UP_PROBE_SHAPE = new Shape(true, false, 1L);

    /**
     * Native bytes one returned row of the hits scan occupies in its
     * Arrow batches: the {@code _score} Float4 (4 bytes) plus the
     * {@code _rowaddr} UInt8 (8 bytes). The hits scan projects no data
     * column (see {@code LanceFtsQuery.HITS_SCAN_COLUMNS}), so the
     * physical row width of the scan is these two columns.
     */
    static final long HITS_SCAN_ROW_BYTES = 12L;

    /**
     * Native bytes per table row a phrase query holds on top of the
     * document set. A phrase reads the positions of every query token's
     * posting list next to the postings ({@code posting_list} in Lance's
     * {@code PostingListReader} reads the positions column when the query
     * is a phrase), one {@code u32} per occurrence, for the whole
     * index whichever fragments the scan keeps. The phrase
     * {@code w000000 w000001 size 10} over 1B rows killed every node of
     * a 4 node 128 GB cluster after being admitted at 52 GB (the
     * document set alone) with 97 GB available, and on one 128 GB node
     * the same phrase peaked at 103 GB of resident set. 48 bytes per
     * row puts the phrase estimate over 1B rows at 100 GB, which
     * explains the single node peak and refuses the shape on a node
     * whose available memory is below it.
     */
    static final long PHRASE_POSITION_BYTES_PER_ROW = 48L;

    /**
     * Allowance over the modelled scan rows for the batches Lance and
     * the Arrow C data interface hold at once while the plugin drains
     * the scan (a produced batch and a consumed batch side by side).
     */
    static final double SCAN_BUFFER_FACTOR = 2.0;

    /**
     * Fraction of the table's rows an unbounded full-text scan is
     * assumed to match when nothing narrows it. The plugin has no
     * per-term statistics at admission time (reading the inverted
     * index's own statistics would load what the gate exists to keep
     * out of memory), so the estimate assumes one row in ten matches.
     */
    static final double UNBOUNDED_MATCH_RATIO = 0.1;

    /**
     * Fraction of the scanned rows a scalar filter is assumed to select
     * when the statistics carry no cardinality for its column (a BTree
     * reports pages and bounds, not distinct values; a column without
     * an index reports nothing). One row in five: the {@code term}
     * kill that motivates the filter gate selected one fifth of a 10B
     * row table through a BTree column, and an assumption below the
     * shape that killed the node would have admitted it. Twice the full
     * text assumption because a scalar predicate on a low cardinality
     * column selects more than a term does.
     */
    static final double FILTER_MATCH_RATIO_UNKNOWN = 0.2;

    /**
     * Native bytes per row address a filter scan materialises. Lance's
     * {@code MaterializeIndexExec} collects the scalar index result into
     * a {@code RowAddrTreeMap} (one roaring bitmap per fragment, about a
     * byte per dense row), turns it into a {@code Vec<u64>} of row
     * addresses (8 bytes per row) and hands the vector to a
     * {@code UInt64Array} the batches slice (no copy), so the code
     * derived floor is 16 bytes per matching row plus the per page
     * result sets held before their union. The measured resident set of
     * the shape that motivates this gate, {@code term rating=5 size 0}
     * over 10B rows with about 2B matching (QA round 16, r7gd.16xlarge),
     * exceeded 500 GB, which is 250 bytes per matching row: the
     * constant is pinned to that measurement so the gate refuses the
     * shape it exists for, and the gap between the floor and the
     * measurement is the first thing QA round 18 attributes.
     */
    static final long FILTER_SCAN_BYTES_PER_MATCHING_ROW = 256L;

    /** Bytes of one {@code _rowaddr} (UInt8) in a scan's Arrow batches. */
    public static final long ROW_ADDRESS_BYTES = 8L;

    /** Rows per Arrow batch Lance emits ({@code BATCH_SIZE_FALLBACK} in {@code lance/src/dataset/scanner.rs}). */
    static final long SCAN_BATCH_ROWS = 8192L;

    /**
     * Bytes of queued object store reads one Lance scan may hold
     * ({@code DEFAULT_IO_BUFFER_SIZE_VALUE}, 2 GiB, in
     * {@code lance/src/dataset/scanner.rs}: 256 reads of 8 MiB pages).
     * A scan holds at most the bytes it reads, so the term is the
     * smaller of this cap and the scanned bytes.
     */
    static final long IO_BUFFER_BYTES_PER_SCAN = 2L << 30;

    /**
     * Bytes per row of a BTree index when the manifest records no size:
     * the value (up to 8 bytes) and the {@code u64} row address of the
     * page schema ({@code [values, ids]}, {@code DEFAULT_BTREE_BATCH_SIZE}
     * 4096 rows per page).
     */
    static final long BTREE_BYTES_PER_ROW = 16L;

    /**
     * Bytes per row of a bitmap or label list index when the manifest
     * records no size: one roaring bitmap per value, about a bit per
     * row for a dense value plus container overhead, taken as one byte
     * per row over every value.
     */
    static final long BITMAP_BYTES_PER_ROW = 1L;

    /**
     * Bytes per zone of a zone map index when the manifest records no
     * size: the min, max, null count and row count of one zone.
     */
    static final long ZONEMAP_BYTES_PER_ZONE = 64L;

    /** Rows per zone Lance uses by default ({@code ROWS_PER_ZONE_DEFAULT} in {@code zonemap.rs}). */
    static final long ZONEMAP_ROWS_PER_ZONE = 8192L;

    /**
     * Factor over the bitmap's serialised bytes a bitmap lookup holds:
     * {@code BitmapIndex::load_bitmap} deserialises the bitmap into a
     * {@code RowAddrTreeMap} and inserts a clone of it into the index
     * cache, so the value's bitmap is resident twice while the search
     * uses it.
     */
    static final long BITMAP_LOAD_FACTOR = 2L;

    /**
     * Bytes per indexed row of an IVF_PQ index when the manifest records
     * no size: 16 product quantisation codes (the default for a 128
     * dimension vector, one byte per sub vector) plus the {@code u64}
     * row address of the partition storage.
     */
    static final long VECTOR_INDEX_BYTES_PER_ROW = 24L;

    /**
     * Factor over a partition's stored bytes its load holds: Lance's
     * {@code load_partition_entry} reads the partition's row range as a
     * stream, collects every batch and then {@code concat_batches} them
     * into one, so the partition is resident twice until the entry is
     * built. The probed partitions load in parallel up to the compute
     * CPU count, so the factor applies to the whole probed set.
     */
    static final long IVF_PARTITION_LOAD_FACTOR = 2L;

    /** Bytes of one {@code float32} vector component, the refine step's full vector reads. */
    static final long FLOAT32_BYTES = 4L;

    /** Heap bytes of one group of a pushed aggregate's state: its encoded key, count and slot bookkeeping. */
    static final long GROUP_STATE_BASE_BYTES = 64L;

    /** Heap bytes one metric adds per group (a double accumulator, its count and its slot). */
    static final long GROUP_STATE_BYTES_PER_METRIC = 24L;

    /** Bytes per row assumed for a Utf8 column in a scan's batches when the width is not known. */
    static final long UTF8_COLUMN_BYTES_PER_ROW = 32L;

    /** Bytes per row assumed for a column of a type the width table does not name. */
    static final long OTHER_COLUMN_BYTES_PER_ROW = 16L;

    /** The kernel's memory accounting on Linux. */
    private static final Path PROC_MEMINFO = Path.of("/proc/meminfo");

    /** The kernel's accounting of this process on Linux. */
    private static final Path PROC_SELF_STATUS = Path.of("/proc/self/status");

    private static volatile boolean enabled = true;
    private static volatile boolean boundedShapesGated = true;
    private static volatile long headroomBytes = DEFAULT_HEADROOM.getBytes();
    private static volatile long shardShareOverrideBytes = 0L;
    private static volatile LongSupplier memoryProbe = ScanAdmission::readAvailablePhysicalMemory;
    private static volatile LongSupplier residentSetProbe = ScanAdmission::readResidentSetBytes;
    private static volatile LongSupplier nativeLimitProbe = ScanAdmission::readNativeMemoryLimit;

    /**
     * The node's planner statistics cache, installed by the plugin so the
     * estimators can read index sizes and cardinalities of the table a
     * scan is about to read; {@code null} (tests, no plugin) leaves every
     * estimator on its per row fallbacks.
     */
    private static volatile TableStatisticsCache tableStatistics;

    /**
     * Scripted available memory readings installed by
     * {@code lance.test.admission_available_memory}: handed out one per
     * reading, the last one repeating; {@code null} when no override is
     * in force. Guarded by {@link #LOCK}.
     */
    private static ArrayDeque<Long> availableMemoryOverride;

    /** Scans this node refused since it started, per kind. */
    private static final Map<Kind, AtomicLong> REJECTIONS = new EnumMap<>(Kind.class);

    static {
        for (Kind kind : Kind.values()) {
            REJECTIONS.put(kind, new AtomicLong());
        }
    }

    /** Estimate of the last decision, admitted or not. */
    private static final AtomicLong LAST_ESTIMATE_BYTES = new AtomicLong();

    /** Kind of the last decision, admitted or not; {@code null} before the first. */
    private static final AtomicReference<Kind> LAST_KIND = new AtomicReference<>();

    /** Source of the last decision, admitted or not; {@code null} before the first. */
    private static final AtomicReference<Source> LAST_SOURCE = new AtomicReference<>();

    /** Guards {@link #POOL}, {@link #inFlight} and {@link #activeScans}. */
    private static final Object LOCK = new Object();

    /** Memory earlier admitted scans left behind that the next one reuses. */
    private static final RetainedPool POOL = new RetainedPool();

    /** Gated requests with a non zero estimate admitted on this node and not yet ended. */
    private static int inFlight;

    /** Gated scans running on this node, admitted or not. */
    private static int activeScans;

    /**
     * Set on the thread {@link #admit(String, long, Shape)} admitted a
     * non zero estimate on without a request ticket, cleared by
     * {@link #requestEnded()} on the same thread, so the two pair
     * without the executor carrying a token.
     */
    private static final ThreadLocal<Boolean> ADMITTED_ON_THREAD = new ThreadLocal<>();

    private ScanAdmission() {}

    /**
     * Memory that earlier admitted scans left in the process and that
     * the next scan reuses, measured as {@code MemAvailable}
     * differences. One scan's contribution is {@code MemAvailable} when
     * it was admitted ({@link #scanAdmitted}) minus {@code MemAvailable}
     * when its scan completed ({@link #scanCompleted}), and at most the
     * growth of the process's resident set over the same interval where
     * that is known (memory another process took meanwhile is not this
     * process's to reuse); the pool is the sum of those contributions,
     * never above the largest estimate admitted since the pool started
     * (one scan cannot leave behind more than it allocated) and never
     * below zero. {@link #creditBytes} gives the pool back reduced by
     * what {@code MemAvailable} has recovered since the last completion
     * sample, and never more than the whole drop since the pool
     * started, so memory the kernel or Lance gave back is not credited
     * twice and an unrelated drop after a completion is not credited at
     * all. An admission that finds {@code MemAvailable} at or above the
     * pool's starting point starts the pool over: nothing is retained
     * any more. Not thread safe; the caller holds the lock.
     */
    static final class RetainedPool {

        /** {@code MemAvailable} at the admission that started the pool; {@code -1} before the first. */
        private long baselineBytes = -1L;

        /** {@code MemAvailable} at the admission whose scan completion is awaited; {@code -1} when none is. */
        private long beforeBytes = -1L;

        /** Resident set at that admission; {@code -1} when unknown. */
        private long residentBeforeBytes = -1L;

        /** {@code MemAvailable} at the latest completion sample, the point recovery is measured from. */
        private long floorBytes = -1L;

        /** Sum of the recorded contributions, within {@code [0, boundBytes]}. */
        private long retainedBytes;

        /** Largest estimate admitted since the pool started. */
        private long boundBytes;

        /**
         * A non zero estimate was admitted with no gated request in
         * flight: sample {@code MemAvailable} and the resident set
         * before its scan. At or above the baseline nothing is retained
         * any more and the pool starts over from this reading; below it
         * the pool stands and the bound grows to this estimate if
         * larger.
         */
        void scanAdmitted(long availableNowBytes, long residentNowBytes, long estimateBytes) {
            if (baselineBytes < 0L || availableNowBytes >= baselineBytes) {
                baselineBytes = availableNowBytes;
                floorBytes = availableNowBytes;
                retainedBytes = 0L;
                boundBytes = Math.max(0L, estimateBytes);
            } else {
                boundBytes = Math.max(boundBytes, estimateBytes);
            }
            beforeBytes = availableNowBytes;
            residentBeforeBytes = residentNowBytes;
        }

        /** Whether an admitted scan's completion sample is still awaited. */
        boolean awaitingCompletion() {
            return beforeBytes >= 0L;
        }

        /**
         * The awaited scan completed and nothing else runs: record what
         * it left behind, at most what the process's resident set grew
         * by when both readings are known. A reading above the
         * admission's (memory freed during the scan) shrinks the pool.
         */
        void scanCompleted(long availableNowBytes, long residentNowBytes) {
            if (beforeBytes < 0L) {
                return;
            }
            long delta = beforeBytes - availableNowBytes;
            if (residentBeforeBytes >= 0L && residentNowBytes >= 0L) {
                delta = Math.min(delta, residentNowBytes - residentBeforeBytes);
            }
            retainedBytes = clamp(retainedBytes + delta, 0L, boundBytes);
            floorBytes = availableNowBytes;
            beforeBytes = -1L;
            residentBeforeBytes = -1L;
        }

        /**
         * What a decision made at {@code availableNowBytes} may add to
         * the available memory: the recorded pool less what
         * {@code MemAvailable} recovered since the last completion, at
         * most the whole drop since the pool started, within
         * {@code [0, boundBytes]}.
         */
        long creditBytes(long availableNowBytes) {
            if (baselineBytes < 0L || retainedBytes <= 0L) {
                return 0L;
            }
            long recovered = Math.max(0L, availableNowBytes - floorBytes);
            long dropSinceBaseline = baselineBytes - availableNowBytes;
            return clamp(Math.min(retainedBytes - recovered, dropSinceBaseline), 0L, boundBytes);
        }

        /** The recorded pool before recovery is applied, for tests. */
        long retainedBytes() {
            return retainedBytes;
        }

        /** The bound the pool is clamped to, for tests. */
        long boundBytes() {
            return boundBytes;
        }

        void reset() {
            baselineBytes = -1L;
            beforeBytes = -1L;
            residentBeforeBytes = -1L;
            floorBytes = -1L;
            retainedBytes = 0L;
            boundBytes = 0L;
        }

        private static long clamp(long value, long low, long high) {
            return Math.max(low, Math.min(high, value));
        }
    }

    // ---- settings holders, probes and overrides ----

    /** Current value of the {@code lance.admission.enabled} setting. */
    public static boolean enabled() {
        return enabled;
    }

    /** Install a new enabled flag; the next decision picks it up. */
    public static void setEnabled(boolean value) {
        enabled = value;
    }

    /** Current value of the {@code lance.admission.bounded_shapes_gated} setting. */
    public static boolean boundedShapesGated() {
        return boundedShapesGated;
    }

    /** Install a new bounded gating flag; the next decision picks it up. */
    public static void setBoundedShapesGated(boolean value) {
        boundedShapesGated = value;
    }

    /** Current value of the {@code lance.admission.headroom} setting, in bytes. */
    public static long headroomBytes() {
        return headroomBytes;
    }

    /** Install a new headroom; the next decision picks it up. */
    public static void setHeadroom(ByteSizeValue value) {
        headroomBytes = value.getBytes();
    }

    /**
     * Install the {@code lance.test.index_cache_shard_share} override.
     * Zero clears it and the decisions read the installed Session's
     * sizing again.
     */
    public static void setIndexCacheShardShareOverride(ByteSizeValue value) {
        shardShareOverrideBytes = value.getBytes();
    }

    /**
     * Install the node's planner statistics cache, the source of index
     * sizes, cardinalities and fragment row counts for the estimators.
     * {@code null} uninstalls it.
     */
    public static void setTableStatistics(TableStatisticsCache cache) {
        tableStatistics = cache;
    }

    /**
     * The planner statistics of {@code dataset} at its open version from
     * the installed cache, collected on a miss (metadata calls only);
     * empty when no cache is installed or the collection fails, in
     * which case the estimators fall back to their per row constants.
     */
    public static Optional<TableStatistics> statisticsOf(Dataset dataset) {
        TableStatisticsCache cache = tableStatistics;
        if (cache == null || dataset == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(cache.forVersion(dataset.uri(), dataset.version(), dataset));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Install the {@code lance.test.admission_available_memory}
     * override: the readings the gate hands out in place of
     * {@code MemAvailable}, one per read with the last one repeating.
     * While the override is in force the resident set is not consulted
     * (its readings would describe the host, not the script). An empty
     * list clears the override.
     */
    public static void setAvailableMemoryOverride(List<String> readings) {
        ArrayDeque<Long> queue = null;
        if (readings != null && !readings.isEmpty()) {
            queue = new ArrayDeque<>(readings.size());
            for (String reading : readings) {
                queue.addLast(ByteSizeValue.parseBytesSizeValue(reading, "lance.test.admission_available_memory").getBytes());
            }
        }
        synchronized (LOCK) {
            availableMemoryOverride = queue;
        }
    }

    /**
     * One available memory reading: the next scripted value while the
     * test override is in force, else the probe.
     */
    private static long readAvailableMemoryNow() {
        synchronized (LOCK) {
            ArrayDeque<Long> queue = availableMemoryOverride;
            if (queue != null && !queue.isEmpty()) {
                return queue.size() > 1 ? queue.pollFirst() : queue.peekFirst();
            }
        }
        return memoryProbe.getAsLong();
    }

    /**
     * One resident set reading, {@code -1} (unknown) while the test
     * override of the available memory is in force.
     */
    private static long readResidentSetNow() {
        synchronized (LOCK) {
            if (availableMemoryOverride != null) {
                return -1L;
            }
        }
        return residentSetProbe.getAsLong();
    }

    /** Cumulative rejection count over every kind, for tests and the stats. */
    public static long rejections() {
        long total = 0L;
        for (AtomicLong counter : REJECTIONS.values()) {
            total += counter.get();
        }
        return total;
    }

    /** Cumulative rejection count of one kind. */
    public static long rejections(Kind kind) {
        return REJECTIONS.get(kind).get();
    }

    /**
     * Cumulative rejections per kind in declaration order, every kind
     * present even at zero, keyed by {@link Kind#key()}, for
     * {@code GET /_lance/stats}.
     */
    public static Map<String, Long> rejectionsByKind() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Kind kind : Kind.values()) {
            counts.put(kind.key(), REJECTIONS.get(kind).get());
        }
        return counts;
    }

    /** Estimate of the last decision on this node, for {@code GET /_lance/stats}. */
    public static long lastEstimateBytes() {
        return LAST_ESTIMATE_BYTES.get();
    }

    /** Kind key of the last decision on this node, {@code "none"} before the first, for {@code GET /_lance/stats}. */
    public static String lastKind() {
        Kind kind = LAST_KIND.get();
        return kind == null ? "none" : kind.key();
    }

    /**
     * Source key of the last decision on this node ({@code request} or
     * {@code warm_up}), {@code "none"} before the first, for
     * {@code GET /_lance/stats}.
     */
    public static String lastSource() {
        Source source = LAST_SOURCE.get();
        return source == null ? "none" : source.key();
    }

    /**
     * The available physical memory the next decision would be judged
     * against: the kernel's {@code MemAvailable} where
     * {@code /proc/meminfo} reports it, else {@link OsProbe}'s free
     * physical memory. Also reported as {@code admission.available_bytes}
     * in {@code GET /_lance/stats}.
     */
    public static long availablePhysicalMemoryBytes() {
        return readAvailableMemoryNow();
    }

    /**
     * The retained memory the next decision would add to the available
     * memory, computed at the current {@code MemAvailable}: zero while a
     * gated request is in flight or a gated scan runs, zero when the
     * resident set guard blocks it, else the pool's credit. Also
     * reported as {@code admission.retained_bytes} in
     * {@code GET /_lance/stats}.
     */
    public static long retainedCreditBytes() {
        return retainedCreditBytes(readAvailableMemoryNow());
    }

    /**
     * {@link #retainedCreditBytes()} at the given {@code MemAvailable}
     * reading, so a decision and its message, or the stats collector's
     * two figures, use one sample.
     */
    public static long retainedCreditBytes(long availableNowBytes) {
        long credit;
        synchronized (LOCK) {
            credit = inFlight == 0 && activeScans == 0 ? POOL.creditBytes(availableNowBytes) : 0L;
        }
        if (credit > 0L && residentSetExcessBytes() > credit) {
            return 0L;
        }
        return credit;
    }

    /**
     * How far the process's resident set exceeds what the plugin
     * accounts, {@code lance.native_memory.limit} plus the JVM heap:
     * memory above that is held by something the gate does not see, and
     * a pool smaller than the excess is not credited. {@link Long#MIN_VALUE}
     * where the resident set or the limit cannot be read (no
     * {@code /proc}, no breaker installed), which never blocks.
     */
    static long residentSetExcessBytes() {
        long rss = readResidentSetNow();
        long limit = nativeLimitProbe.getAsLong();
        if (rss < 0L || limit < 0L) {
            return Long.MIN_VALUE;
        }
        return rss - limit - Runtime.getRuntime().maxMemory();
    }

    /** The installed {@code lance_native} breaker's limit, {@code -1} when no breaker is installed. */
    private static long readNativeMemoryLimit() {
        CircuitBreaker breaker = LanceCircuitBreaker.getBreaker();
        return breaker == null ? -1L : breaker.getLimit();
    }

    /**
     * {@code VmRSS} of {@code /proc/self/status} in bytes, {@code -1}
     * where the file or the line does not exist (macOS, Windows).
     */
    static long readResidentSetBytes() {
        return AccessController.doPrivileged(() -> {
            if (!Files.isReadable(PROC_SELF_STATUS)) {
                return -1L;
            }
            try {
                return parseKibibyteLine(Files.readAllLines(PROC_SELF_STATUS), "VmRSS:");
            } catch (IOException | SecurityException e) {
                return -1L;
            }
        });
    }

    /**
     * {@code MemAvailable} of {@code /proc/meminfo} in bytes, or the
     * {@link OsProbe} free physical memory where the file or the line
     * does not exist (macOS, Windows, kernels before 3.14).
     */
    static long readAvailablePhysicalMemory() {
        long memAvailable = memAvailableBytes();
        return memAvailable >= 0L ? memAvailable : OsProbe.getInstance().getFreePhysicalMemorySize();
    }

    /** {@code MemAvailable} in bytes, {@code -1} when it cannot be read. */
    private static long memAvailableBytes() {
        // The agent's permission check intersects every protection
        // domain on the stack, and the server domain has no grant for
        // /proc/meminfo (its policy lists specific /proc paths only).
        // doPrivileged cuts the walk at this frame so the plugin's own
        // file grant decides, the way the bundled cloud plugins read
        // their credentials files.
        return AccessController.doPrivileged(() -> {
            if (!Files.isReadable(PROC_MEMINFO)) {
                return -1L;
            }
            try {
                return parseMemAvailableBytes(Files.readAllLines(PROC_MEMINFO));
            } catch (IOException | SecurityException e) {
                return -1L;
            }
        });
    }

    /**
     * The {@code MemAvailable} line of {@code /proc/meminfo} content,
     * converted from the kernel's kibibytes to bytes; {@code -1} when
     * the line is absent or malformed. Package private for tests.
     */
    static long parseMemAvailableBytes(List<String> memInfoLines) {
        return parseKibibyteLine(memInfoLines, "MemAvailable:");
    }

    /**
     * The value of the line starting with {@code label} in a
     * {@code /proc} file that reports kibibytes ({@code Label:  123 kB}),
     * in bytes; {@code -1} when the line is absent or malformed.
     */
    static long parseKibibyteLine(List<String> lines, String label) {
        for (String line : lines) {
            if (line.startsWith(label) == false) {
                continue;
            }
            String[] fields = line.split("\\s+");
            if (fields.length < 2) {
                return -1L;
            }
            try {
                return Long.parseLong(fields[1]) * 1024L;
            } catch (NumberFormatException e) {
                return -1L;
            }
        }
        return -1L;
    }

    // ---- full text shape classification ----

    /**
     * Full-text shape of one resolved query tree as the gate sees it:
     * whether the tree carries a {@link LanceFtsQuery} at all, whether
     * any of its scans is unbounded (the resolver leaves the scan
     * limit unbounded for sort, aggregations, {@code post_filter},
     * {@code size 0}, for every clause nested under another scoring
     * query and under a security reader wrapper; an exact match count
     * also runs the unbounded count-only scan), the largest bounded
     * scan limit otherwise, which is the rows the top-k page's scan
     * returns at most, how many full text clauses the tree searches
     * (every {@code match}, {@code match_phrase} and column of a
     * {@code multi_match} across every {@link LanceFtsQuery} and every
     * clause of a Lance boolean or boost query: Lance runs one search
     * per clause, each over the whole index, and joins their results)
     * and whether any of them is a phrase, which reads positions.
     */
    public record Shape(boolean hasFtsClause, boolean unbounded, long boundedScanRows, int clauses, boolean phrase) {

        /** A query tree without a full-text clause; never gated. */
        public static final Shape NONE = new Shape(false, false, 0L, 0, false);

        /** A shape of one non phrase clause (a single {@code match}). */
        public Shape(boolean hasFtsClause, boolean unbounded, long boundedScanRows) {
            this(hasFtsClause, unbounded, boundedScanRows, hasFtsClause ? 1 : 0, false);
        }
    }

    /**
     * Classify {@code query} for the gate. A tree without a
     * {@link LanceFtsQuery} is {@link Shape#NONE}. A tree where any
     * full-text clause carries no scan limit, or whose exact match
     * count ({@code track_total_hits: true}) would run the unbounded
     * count-only scan, is unbounded. Everything else is a bounded
     * top-k page whose scan returns at most the largest clause limit.
     * The clause count and the phrase flag are read off every
     * {@link LanceFtsQuery}'s Lance query tree whatever the bound.
     */
    public static Shape classify(Query query, boolean trackTotalHitsAccurate) {
        List<LanceFtsQuery> found = new ArrayList<>();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query leaf) {
                if (leaf instanceof LanceFtsQuery fts) {
                    found.add(fts);
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                // The default visitor skips MUST_NOT subtrees; a
                // must_not full-text clause still runs its scan, so
                // every occur is walked.
                return this;
            }
        });
        if (found.isEmpty()) {
            return Shape.NONE;
        }
        int clauses = 0;
        boolean phrase = false;
        boolean unbounded = trackTotalHitsAccurate;
        long boundedScanRows = 0L;
        for (LanceFtsQuery fts : found) {
            clauses += leafClauses(fts.fullTextQuery());
            phrase |= hasPhrase(fts.fullTextQuery());
            if (fts.scanLimit() == LanceFtsQuery.SCAN_LIMIT_UNBOUNDED) {
                unbounded = true;
            } else {
                boundedScanRows = Math.max(boundedScanRows, fts.scanLimit());
            }
        }
        return new Shape(true, unbounded, unbounded ? 0L : boundedScanRows, Math.max(1, clauses), phrase);
    }

    /**
     * The searches Lance runs for {@code query}: one per {@code match}
     * or phrase, one per column of a {@code multi_match} (each column
     * has its own inverted index), the sum over the clauses of a
     * boolean query and over both sides of a boost query.
     */
    static int leafClauses(FullTextQuery query) {
        switch (query.getType()) {
            case MULTI_MATCH:
                return Math.max(1, ((FullTextQuery.MultiMatchQuery) query).getColumns().size());
            case BOOST: {
                FullTextQuery.BoostQuery boost = (FullTextQuery.BoostQuery) query;
                return leafClauses(boost.getPositive()) + leafClauses(boost.getNegative());
            }
            case BOOLEAN: {
                int sum = 0;
                for (FullTextQuery.BooleanClause clause : ((FullTextQuery.BooleanQuery) query).getClauses()) {
                    sum += leafClauses(clause.getQuery());
                }
                return Math.max(1, sum);
            }
            default:
                return 1;
        }
    }

    /** Whether {@code query} is, or contains, a phrase, which reads the positions of its tokens' postings. */
    static boolean hasPhrase(FullTextQuery query) {
        switch (query.getType()) {
            case MATCH_PHRASE:
                return true;
            case BOOST: {
                FullTextQuery.BoostQuery boost = (FullTextQuery.BoostQuery) query;
                return hasPhrase(boost.getPositive()) || hasPhrase(boost.getNegative());
            }
            case BOOLEAN:
                for (FullTextQuery.BooleanClause clause : ((FullTextQuery.BooleanQuery) query).getClauses()) {
                    if (hasPhrase(clause.getQuery())) {
                        return true;
                    }
                }
                return false;
            default:
                return false;
        }
    }

    /**
     * Whether serving {@code query} runs an unbounded full-text scan.
     * See {@link #classify}; a query without a full-text clause never
     * runs one.
     */
    public static boolean runsUnboundedFtsScan(Query query, boolean trackTotalHitsAccurate) {
        Shape shape = classify(query, trackTotalHitsAccurate);
        return shape.hasFtsClause() && shape.unbounded();
    }

    /**
     * Whether the gate judges {@code shape} before its scan starts:
     * every unbounded shape, and the bounded top-k pages while
     * {@code lance.admission.bounded_shapes_gated} is {@code true}.
     */
    public static boolean gates(Shape shape) {
        return shape.hasFtsClause() && (shape.unbounded() || boundedShapesGated);
    }

    // ---- estimators, one per kind ----

    /**
     * Native bytes the hits scan of {@code shape} over a table of
     * {@code rows} rows is expected to hold beyond the document set:
     * the rows the scan returns ({@link #UNBOUNDED_MATCH_RATIO} of the
     * table for an unbounded shape, the top-k limit for a bounded
     * page) times the physical row width of the scan's projection
     * ({@link #HITS_SCAN_ROW_BYTES}) times
     * {@link #SCAN_BUFFER_FACTOR}.
     */
    static long scanBufferEstimateBytes(long rows, Shape shape) {
        long returnedRows = shape.unbounded() ? (long) (Math.max(0L, rows) * UNBOUNDED_MATCH_RATIO) : Math.max(0L, shape.boundedScanRows());
        return (long) (returnedRows * HITS_SCAN_ROW_BYTES * SCAN_BUFFER_FACTOR);
    }

    /**
     * The {@link Kind#FTS} estimate: the document set rebuild
     * ({@link NativeMemoryLimit#invertedIndexEntryEstimateBytes}, which
     * Lance performs whole whichever fragments the scan keeps) once per
     * full text clause (Lance searches each clause of a boolean query on
     * its own and holds every clause's result while it joins them, so a
     * {@code bool} of two {@code match} clauses rebuilds and holds two
     * document sets' worth), plus {@code rows × PHRASE_POSITION_BYTES_PER_ROW}
     * when a clause is a phrase (the positions of its tokens' postings,
     * read for the whole index), plus {@code scanBufferBytes}; zero when
     * one document set fits {@code shardShareBytes}, because a document
     * set the cache holds is not rebuilt per scan.
     */
    static long ftsEstimateBytes(long rows, int clauses, boolean phrase, long scanBufferBytes, long shardShareBytes) {
        long entry = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        if (entry <= shardShareBytes) {
            return 0L;
        }
        long positions = phrase ? Math.max(0L, rows) * PHRASE_POSITION_BYTES_PER_ROW : 0L;
        return entry * Math.max(1, clauses) + positions + Math.max(0L, scanBufferBytes);
    }

    /**
     * The {@link Kind#SCALAR_INDEX} estimate: what Lance loads of the
     * index over a column to answer a predicate that selects
     * {@code selectivity} of the table's {@code rows}, zero when that
     * load fits {@code shardShareBytes} (the pages or bitmaps are then
     * kept in the index cache and the next query finds them).
     *
     * <ul>
     *   <li>BTree: {@code BTreeIndex::search_with_options} looks the
     *       predicate up in the page lookup and reads only the pages
     *       whose value range it meets, each page a cache entry of its
     *       own, so the load is the matching share of the index:
     *       {@code size × selectivity}.</li>
     *   <li>Bitmap and label list: {@code BitmapIndex::load_bitmap} reads
     *       one serialised bitmap per matching value, deserialises it and
     *       inserts a clone into the cache: {@code size × selectivity ×
     *       BITMAP_LOAD_FACTOR}.</li>
     *   <li>Zone map and every other type: the whole index is read to
     *       answer any predicate: {@code size}.</li>
     * </ul>
     *
     * {@code indexSizeBytes} is the manifest's total file size of the
     * index ({@code ColumnStatistics.IndexSummary.sizeBytes}); when the
     * manifest records none, the size is {@code rows} times the per row
     * constant of the type ({@link #BTREE_BYTES_PER_ROW},
     * {@link #BITMAP_BYTES_PER_ROW}, {@link #ZONEMAP_BYTES_PER_ZONE} per
     * {@link #ZONEMAP_ROWS_PER_ZONE} rows).
     */
    static long scalarIndexEstimateBytes(IndexType type, OptionalLong indexSizeBytes, long rows, double selectivity, long shardShareBytes) {
        long safeRows = Math.max(0L, rows);
        double share = Math.max(0d, Math.min(1d, selectivity));
        long size;
        long loaded;
        if (type == IndexType.BTREE) {
            size = indexSizeBytes.orElse(safeRows * BTREE_BYTES_PER_ROW);
            loaded = (long) (size * share);
        } else if (type == IndexType.BITMAP || type == IndexType.LABEL_LIST) {
            size = indexSizeBytes.orElse(safeRows * BITMAP_BYTES_PER_ROW);
            loaded = (long) (size * share) * BITMAP_LOAD_FACTOR;
        } else if (type == IndexType.ZONEMAP) {
            loaded = indexSizeBytes.orElse(((safeRows + ZONEMAP_ROWS_PER_ZONE - 1) / ZONEMAP_ROWS_PER_ZONE) * ZONEMAP_BYTES_PER_ZONE);
        } else {
            loaded = indexSizeBytes.orElse(safeRows * BTREE_BYTES_PER_ROW);
        }
        return loaded <= shardShareBytes ? 0L : loaded;
    }

    /**
     * Rows a filter scan is expected to return over {@code nodeRows}
     * physical rows: {@code selectivity} of them, capped by
     * {@code boundedRows} when the scan carries a limit and
     * {@code capAtBound} is set ({@code lance.admission.bounded_shapes_gated}
     * {@code false}). With the setting on a bounded page is judged on
     * every matching row, because Lance's {@code MaterializeIndexExec}
     * materialises the whole scalar index result before the limit
     * applies, the same way the full text document set does not
     * shrink with the page size.
     */
    static long filterScanMatchingRows(long nodeRows, double selectivity, long boundedRows, boolean capAtBound) {
        long matching = (long) (Math.max(0L, nodeRows) * Math.max(0d, Math.min(1d, selectivity)));
        if (boundedRows > 0L && capAtBound) {
            matching = Math.min(matching, boundedRows);
        }
        return matching;
    }

    /**
     * The {@link Kind#FILTER_SCAN} estimate, the native memory of one
     * scan that evaluates a filter over {@code nodeRows} physical rows
     * and streams rows of {@code rowWidthBytes} back (8 bytes for the
     * {@code _rowaddr} only projection of a filter, plus the sort
     * columns of a sorted page):
     *
     * <ul>
     *   <li>{@code matchingRows × FILTER_SCAN_BYTES_PER_MATCHING_ROW}:
     *       the row addresses the scalar index materialises before the
     *       {@code Take} and the batches ({@link #FILTER_SCAN_BYTES_PER_MATCHING_ROW}
     *       says what the code accounts for and what the measurement
     *       pins);</li>
     *   <li>{@code batchReadahead × SCAN_BATCH_ROWS × rowWidthBytes ×
     *       SCAN_BUFFER_FACTOR}: the decoded batches in flight
     *       ({@code batch_readahead} defaults to the compute CPU count,
     *       {@link #SCAN_BATCH_ROWS} rows each, a produced and a consumed
     *       batch side by side);</li>
     *   <li>{@code min(IO_BUFFER_BYTES_PER_SCAN, nodeRows × rowWidthBytes)}:
     *       the queued object store reads of the scan.</li>
     * </ul>
     *
     * Zero when the sum fits {@code shardShareBytes}: buffers smaller
     * than one shard of the cache the node dedicates to Lance are within
     * its sizing (and they are what every small table's filter costs).
     */
    static long filterScanEstimateBytes(long nodeRows, long matchingRows, long rowWidthBytes, int batchReadahead, long shardShareBytes) {
        long width = Math.max(1L, rowWidthBytes);
        long materialised = Math.max(0L, matchingRows) * FILTER_SCAN_BYTES_PER_MATCHING_ROW;
        long batches = (long) (Math.max(1, batchReadahead) * SCAN_BATCH_ROWS * width * SCAN_BUFFER_FACTOR);
        long io = Math.min(IO_BUFFER_BYTES_PER_SCAN, Math.max(0L, nodeRows) * width);
        long total = materialised + batches + io;
        return total <= shardShareBytes ? 0L : total;
    }

    /**
     * Heap bytes the filter scan's per fragment bit sets take: one bit
     * per physical row of the node's fragments plus a
     * {@code FixedBitSet} header per fragment. Compared with the request
     * breaker's room, not with physical memory.
     */
    static long filterScanHeapBytes(long nodeRows, int fragments) {
        return Math.max(0L, nodeRows) / 8L + Math.max(0, fragments) * 64L;
    }

    /**
     * The {@link Kind#VECTOR_INDEX} estimate of a nearest scan that
     * probes {@code nprobes} of the index's {@code partitions} (the
     * table statistics' {@code num_partitions}, 0 when they report
     * none) with {@code k × refineFactor} candidates over
     * {@code dimension} components:
     *
     * <ul>
     *   <li>the probed partitions: the index's bytes scaled by
     *       {@code nprobes / partitions} (the whole index when the count
     *       is unknown, since the plugin cannot then tell how much of it
     *       the probes touch), times {@link #IVF_PARTITION_LOAD_FACTOR} for
     *       the read then concatenated copy of each partition;</li>
     *   <li>the refine step: {@code k × refineFactor × dimension × 4}
     *       bytes of full vectors read back when {@code refineFactor} is
     *       above one.</li>
     * </ul>
     *
     * {@code indexSizeBytes} is the manifest's size of the vector index;
     * when absent, {@code rows × VECTOR_INDEX_BYTES_PER_ROW}. Zero when
     * the probed bytes fit {@code shardShareBytes}: probed partitions
     * are cache entries and the next scan finds them.
     */
    static long vectorIndexEstimateBytes(
        OptionalLong indexSizeBytes,
        long rows,
        int nprobes,
        long partitions,
        int k,
        int refineFactor,
        int dimension,
        long shardShareBytes
    ) {
        long size = indexSizeBytes.orElse(Math.max(0L, rows) * VECTOR_INDEX_BYTES_PER_ROW);
        double probedShare = partitions > 0L ? Math.min(1d, Math.max(1, nprobes) / (double) partitions) : 1d;
        long probed = (long) (size * probedShare) * IVF_PARTITION_LOAD_FACTOR;
        long refine = refineFactor > 1 ? (long) Math.max(0, k) * refineFactor * Math.max(0, dimension) * FLOAT32_BYTES : 0L;
        long total = probed + refine;
        return total <= shardShareBytes ? 0L : total;
    }

    /**
     * The {@link Kind#AGGREGATE_SCAN} estimate: {@code scans} parallel
     * Lance scans (the {@code pushdown_parallelism} fragment groups)
     * over {@code scannedRows} rows in total, each streaming rows of
     * {@code rowWidthBytes} (the aggregate's key and measure columns):
     * per scan the queued reads, {@code min(IO_BUFFER_BYTES_PER_SCAN,
     * groupRows × rowWidthBytes)}, plus the decoded batches in flight,
     * {@code batchReadahead × SCAN_BATCH_ROWS × rowWidthBytes ×
     * SCAN_BUFFER_FACTOR}. Zero when the sum fits
     * {@code shardShareBytes}, as for a filter scan. The hash aggregate
     * state DataFusion keeps per group is not in this term; the group
     * state on the plugin's heap is {@link #aggregateScanHeapBytes}.
     */
    static long aggregateScanEstimateBytes(int scans, long scannedRows, long rowWidthBytes, int batchReadahead, long shardShareBytes) {
        int count = Math.max(1, scans);
        long width = Math.max(1L, rowWidthBytes);
        long groupRows = (Math.max(0L, scannedRows) + count - 1) / count;
        long perScan = Math.min(IO_BUFFER_BYTES_PER_SCAN, groupRows * width) + (long) (Math.max(1, batchReadahead) * SCAN_BATCH_ROWS * width
            * SCAN_BUFFER_FACTOR);
        long total = perScan * count;
        return total <= shardShareBytes ? 0L : total;
    }

    /**
     * Heap bytes of a pushed aggregate's group state: {@code groups}
     * (the resolver's estimate, bounded by
     * {@code lance.aggregation.pushdown_max_groups}) times a base of
     * {@link #GROUP_STATE_BASE_BYTES} plus {@link #GROUP_STATE_BYTES_PER_METRIC}
     * per metric, once per parallel scan since every fragment group
     * keeps its own partial until the merge.
     */
    static long aggregateScanHeapBytes(long groups, int metrics, int scans) {
        return Math.max(0L, groups) * (GROUP_STATE_BASE_BYTES + GROUP_STATE_BYTES_PER_METRIC * Math.max(0, metrics)) * Math.max(1, scans);
    }

    /** Lance's {@code batch_readahead} default: the compute CPU count, which is what the JVM sees as available processors. */
    static int batchReadahead() {
        return NativeMemoryLimit.availableCpus();
    }

    /**
     * Bytes one value of an Arrow column occupies in a scan's batches:
     * the fixed width of a numeric, boolean, date or timestamp type,
     * {@link #UTF8_COLUMN_BYTES_PER_ROW} for a string,
     * {@code dimension × 4} for a {@code FixedSizeList<float32>} vector,
     * {@link #OTHER_COLUMN_BYTES_PER_ROW} otherwise.
     */
    public static long columnWidthBytes(Field field) {
        if (field == null) {
            return OTHER_COLUMN_BYTES_PER_ROW;
        }
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int intType) {
            return Math.max(1, intType.getBitWidth() / 8);
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == FloatingPointPrecision.DOUBLE ? 8L : 4L;
        }
        if (type instanceof ArrowType.Bool) {
            return 1L;
        }
        if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
            return 8L;
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return UTF8_COLUMN_BYTES_PER_ROW;
        }
        if (type instanceof ArrowType.FixedSizeList list && !field.getChildren().isEmpty()) {
            return (long) list.getListSize() * columnWidthBytes(field.getChildren().get(0));
        }
        return OTHER_COLUMN_BYTES_PER_ROW;
    }

    /** Width of the column named {@code column} in {@code fields}, {@link #OTHER_COLUMN_BYTES_PER_ROW} when absent. */
    public static long columnWidthBytes(List<Field> fields, String column) {
        if (fields != null && column != null) {
            for (Field field : fields) {
                if (column.equals(field.getName())) {
                    return columnWidthBytes(field);
                }
            }
        }
        return OTHER_COLUMN_BYTES_PER_ROW;
    }

    /** Dimension of a {@code FixedSizeList} column in {@code fields}, 0 when absent or not a list. */
    public static int vectorDimension(List<Field> fields, String column) {
        if (fields != null && column != null) {
            for (Field field : fields) {
                if (column.equals(field.getName()) && field.getType() instanceof ArrowType.FixedSizeList list) {
                    return list.getListSize();
                }
            }
        }
        return 0;
    }

    // ---- what the statistics say about a filter ----

    /**
     * The indexed columns of {@code statistics} that {@code filterSql}
     * references, by matching each column name as a whole identifier
     * (the SQL printer writes identifiers bare; a backtick quoted form
     * is accepted too). A string literal that spells a column name
     * counts as a reference, which can only lower the selectivity the
     * gate assumes for that column and is accepted as the price of not
     * parsing DataFusion SQL here.
     */
    static List<ColumnStatistics> referencedIndexedColumns(String filterSql, TableStatistics statistics) {
        List<ColumnStatistics> referenced = new ArrayList<>();
        if (filterSql == null || filterSql.isEmpty() || statistics == null) {
            return referenced;
        }
        for (ColumnStatistics column : statistics.columns().values()) {
            if (identifierPattern(column.column()).matcher(filterSql).find()) {
                referenced.add(column);
            }
        }
        return referenced;
    }

    /** A regex that matches {@code column} as a whole identifier, bare or backtick quoted. */
    private static Pattern identifierPattern(String column) {
        return Pattern.compile("(?<![\\w.`])`?" + Pattern.quote(column) + "`?(?![\\w.`])");
    }

    /** Whether {@code filterSql} compares {@code column} for equality ({@code column = value}, not {@code !=}, {@code <>}, {@code >=}, {@code <=}). */
    static boolean isEquality(String filterSql, String column) {
        Matcher matcher = Pattern.compile("(?<![\\w.`])`?" + Pattern.quote(column) + "`?\\s*(?<![!<>])=(?!=)").matcher(filterSql);
        return matcher.find();
    }

    /**
     * The share of the rows {@code filterSql} is expected to select:
     * for an equality on a column whose index reports a distinct count
     * (a bitmap's {@code num_bitmaps}), one over that count, the largest
     * such share when several columns qualify (an {@code AND} selects
     * less, an {@code OR} at most their sum, and the larger single term
     * is the conservative choice between the two); else
     * {@link #FILTER_MATCH_RATIO_UNKNOWN}.
     */
    static double filterSelectivity(String filterSql, TableStatistics statistics) {
        double best = -1d;
        for (ColumnStatistics column : referencedIndexedColumns(filterSql, statistics)) {
            OptionalLong distinct = column.distinctCount();
            if (distinct.isPresent() && distinct.getAsLong() > 0L && isEquality(filterSql, column.column())) {
                best = Math.max(best, 1d / distinct.getAsLong());
            }
        }
        return best < 0d ? FILTER_MATCH_RATIO_UNKNOWN : Math.min(1d, best);
    }

    /**
     * The scalar index Lance would answer {@code filterSql} from: the
     * first referenced column's first non full text, non vector index
     * (a column with several such indexes is answered by whichever
     * Lance picks; the first summarised one is the estimate's).
     */
    static Optional<ColumnStatistics.IndexSummary> scalarIndexFor(String filterSql, TableStatistics statistics) {
        for (ColumnStatistics column : referencedIndexedColumns(filterSql, statistics)) {
            for (ColumnStatistics.IndexSummary index : column.indexes()) {
                if (index.type().isPresent() && isScalarIndex(index.type().get())) {
                    return Optional.of(index);
                }
            }
        }
        return Optional.empty();
    }

    /** The vector index over {@code column} in {@code statistics}, if any. */
    static Optional<ColumnStatistics.IndexSummary> vectorIndexFor(String column, TableStatistics statistics) {
        if (statistics == null || column == null) {
            return Optional.empty();
        }
        Optional<ColumnStatistics> stats = statistics.column(column);
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        for (ColumnStatistics.IndexSummary index : stats.get().indexes()) {
            if (index.type().isPresent() && isVectorIndex(index.type().get())) {
                return Optional.of(index);
            }
        }
        return Optional.empty();
    }

    /** Whether {@code type} is a scalar index the filter path loads (not full text, not vector, not the internal kinds). */
    static boolean isScalarIndex(IndexType type) {
        switch (type) {
            case BTREE:
            case BITMAP:
            case LABEL_LIST:
            case ZONEMAP:
            case BLOOM_FILTER:
            case RTREE:
            case NGRAM:
                return true;
            default:
                return false;
        }
    }

    /** Whether {@code type} is a vector index ({@code VECTOR} or any {@code IVF_*}). */
    static boolean isVectorIndex(IndexType type) {
        return type == IndexType.VECTOR || type.name().startsWith("IVF");
    }

    /**
     * Live rows of {@code fragmentIds} of {@code dataset} (null: every
     * fragment) from the installed statistics, {@code 0} when they are
     * not available; the rows a count or an aggregate scans.
     */
    public static long fragmentRows(Dataset dataset, List<Integer> fragmentIds) {
        Optional<TableStatistics> statistics = statisticsOf(dataset);
        if (statistics.isEmpty()) {
            return 0L;
        }
        if (fragmentIds == null) {
            return statistics.get().rowCount();
        }
        Set<Integer> wanted = new HashSet<>(fragmentIds);
        long rows = 0L;
        for (TableStatistics.FragmentStats fragment : statistics.get().fragments()) {
            if (wanted.contains(fragment.id())) {
                rows += fragment.rows();
            }
        }
        return rows;
    }

    /** Physical rows of the table {@code statistics} describe, {@code 0} when unknown. */
    static long tableRows(Optional<TableStatistics> statistics) {
        return statistics.map(s -> s.rowCount() + s.deletedRows()).orElse(0L);
    }

    // ---- the decision ----

    /**
     * One admission decision: whether the scan may start, the native
     * estimate it was judged on ({@code 0} when the load fits the shard
     * share), the heap estimate judged against the request breaker, the
     * available memory left after the headroom (which can be negative
     * when the headroom exceeds the node's available memory), the
     * retained memory credited on top of it, and whether it was the
     * heap term that refused.
     */
    public record Decision(boolean admitted, long estimateBytes, long heapEstimateBytes, long availableBytes, long retainedCreditBytes,
        boolean heapRefused) {
    }

    /**
     * Decide whether a scan with a native {@code estimateBytes} and a
     * heap {@code heapEstimateBytes} may start. A non-zero native
     * estimate is admitted only when the available physical memory
     * minus the headroom plus {@code retainedCreditBytes} (the memory
     * earlier admitted scans left behind, see {@link RetainedPool}) is
     * positive and holds it; a non-zero heap estimate only when
     * {@code heapAvailableBytes} (the request breaker's room) holds it.
     * A disabled gate admits everything.
     */
    public static Decision decide(
        long estimateBytes,
        long heapEstimateBytes,
        long availableMemoryBytes,
        long heapAvailableBytes,
        boolean enabled,
        long headroomBytes,
        long retainedCreditBytes
    ) {
        long estimate = Math.max(0L, estimateBytes);
        long heap = Math.max(0L, heapEstimateBytes);
        long available = availableMemoryBytes - headroomBytes;
        long credit = Math.max(0L, retainedCreditBytes);
        long judged = available + credit;
        boolean nativeAdmitted = estimate == 0L || (judged > 0L && estimate <= judged);
        boolean heapAdmitted = heap == 0L || heap <= heapAvailableBytes;
        boolean admitted = !enabled || (nativeAdmitted && heapAdmitted);
        return new Decision(admitted, estimate, heap, available, credit, enabled && nativeAdmitted && !heapAdmitted);
    }

    /**
     * The full text decision as the executor's tests exercise it: the
     * document set rebuild of one non phrase clause plus
     * {@code scanBufferBytes}, zero when the document set fits
     * {@code shardShareBytes}, judged with no heap term.
     */
    public static Decision decideFts(
        long rows,
        long scanBufferBytes,
        long shardShareBytes,
        long availableMemoryBytes,
        boolean enabled,
        long headroomBytes,
        long retainedCreditBytes
    ) {
        return decide(
            ftsEstimateBytes(rows, 1, false, scanBufferBytes, shardShareBytes),
            0L,
            availableMemoryBytes,
            Long.MAX_VALUE,
            enabled,
            headroomBytes,
            retainedCreditBytes
        );
    }

    // ---- admit entry points ----

    /**
     * Gate one full-text scan of {@code shape} over {@code indexName},
     * a table of {@code rows} physical rows, without a request ticket:
     * an admitted non zero estimate is counted in flight on the calling
     * thread until {@link #requestEnded()} runs there.
     */
    public static void admit(String indexName, long rows, Shape shape) {
        admit(indexName, rows, shape, null);
    }

    /**
     * Gate one full-text scan of {@code shape} over {@code indexName},
     * a table of {@code rows} physical rows. Reads the shard share
     * from the installed Session (or the test override), the available
     * physical memory from {@link #availablePhysicalMemoryBytes}, the
     * retained credit from the pool and the settings from the static
     * holders, records the estimate, and throws
     * {@link CircuitBreakingException} on a rejection. An admitted non
     * zero estimate is counted in flight on {@code ticket} (the
     * request's {@link LanceHitsAccounting}, released by its close) or,
     * without one, on the calling thread until {@link #requestEnded()}
     * runs there.
     */
    public static void admit(String indexName, long rows, Shape shape, LanceHitsAccounting ticket) {
        long shardShare = shardShareBytes();
        long estimate = ftsEstimateBytes(rows, shape.clauses(), shape.phrase(), scanBufferEstimateBytes(rows, shape), shardShare);
        int clauses = Math.max(1, shape.clauses());
        String what = (shape.unbounded() ? "unbounded full text scan" : "bounded full text page")
            + " over ["
            + indexName
            + "]: inverted index document set of ["
            + NativeMemoryLimit.humanReadable(NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows))
            + "]"
            + (clauses > 1 ? " rebuilt for each of " + clauses + " full text clauses" : "")
            + (shape.phrase()
                ? " plus the positions of the phrase's tokens ["
                    + NativeMemoryLimit.humanReadable(Math.max(0L, rows) * PHRASE_POSITION_BYTES_PER_ROW)
                    + "]"
                : "")
            + " against an index cache shard of ["
            + NativeMemoryLimit.humanReadable(shardShare)
            + "] plus the hits scan buffers";
        String remedy = shape.unbounded()
            ? "Bound the shape (a top k page without sort or aggregations), attach the table to a node with a larger index cache, "
                + "or relax lance.admission.headroom / lance.admission.enabled."
            : "Attach the table to a node with a larger index cache, or relax lance.admission.bounded_shapes_gated / "
                + "lance.admission.headroom / lance.admission.enabled.";
        judge(Kind.FTS, estimate, 0L, Long.MAX_VALUE, what, remedy, ticket);
    }

    /**
     * Gate the scalar filter {@code filterSql} the scan over
     * {@code dataset}'s fragments is about to evaluate: first the
     * {@link Kind#SCALAR_INDEX} load of the index that answers it (when
     * the statistics name one), then the {@link Kind#FILTER_SCAN} of the
     * matching rows over the node's {@code nodeRows} physical rows in
     * {@code fragments} fragments, streaming rows of
     * {@code rowWidthBytes}. {@code boundedRows} is the scan's limit,
     * 0 when unbounded. The filter's heap term is compared with
     * {@code heapAvailableBytes}. Each path is judged in turn and either
     * counts the request once in flight through {@code ticket}.
     */
    public static void admitFilterScan(
        String indexName,
        Dataset dataset,
        String filterSql,
        long nodeRows,
        int fragments,
        long boundedRows,
        long rowWidthBytes,
        long heapAvailableBytes,
        LanceHitsAccounting ticket
    ) {
        if (!enabled) {
            return;
        }
        long shardShare = shardShareBytes();
        Optional<TableStatistics> statistics = statisticsOf(dataset);
        double selectivity = statistics.map(s -> filterSelectivity(filterSql, s)).orElse(FILTER_MATCH_RATIO_UNKNOWN);
        Optional<ColumnStatistics.IndexSummary> index = statistics.flatMap(s -> scalarIndexFor(filterSql, s));
        if (index.isPresent()) {
            long tableRows = tableRows(statistics);
            IndexType type = index.get().type().orElse(IndexType.BTREE);
            long estimate = scalarIndexEstimateBytes(type, index.get().sizeBytes(), tableRows, selectivity, shardShare);
            String what = type.name().toLowerCase(Locale.ROOT)
                + " index ["
                + index.get().name()
                + "] over ["
                + indexName
                + "] answering ["
                + filterSql
                + "]: index of ["
                + NativeMemoryLimit.humanReadable(index.get().sizeBytes().orElse(tableRows * BTREE_BYTES_PER_ROW))
                + "], selectivity "
                + String.format(Locale.ROOT, "%.4f", selectivity)
                + ", against an index cache shard of ["
                + NativeMemoryLimit.humanReadable(shardShare)
                + "]";
            judge(Kind.SCALAR_INDEX, estimate, 0L, heapAvailableBytes, what, filterRemedy(), ticket);
        }
        long matching = filterScanMatchingRows(nodeRows, selectivity, boundedRows, !boundedShapesGated);
        long estimate = filterScanEstimateBytes(nodeRows, matching, rowWidthBytes, batchReadahead(), shardShare);
        long heap = filterScanHeapBytes(nodeRows, fragments);
        String what = (boundedRows > 0L ? "bounded" : "unbounded")
            + " filter scan over ["
            + indexName
            + "] of ["
            + filterSql
            + "]: "
            + matching
            + " of "
            + nodeRows
            + " rows expected to match on this node at ["
            + NativeMemoryLimit.humanReadable(FILTER_SCAN_BYTES_PER_MATCHING_ROW)
            + "] per matching row plus the batches in flight and the read queue; the per fragment bit sets take ["
            + NativeMemoryLimit.humanReadable(heap)
            + "] of heap";
        judge(Kind.FILTER_SCAN, estimate, heap, heapAvailableBytes, what, filterRemedy(), ticket);
    }

    /**
     * Gate a filtered scan the executor runs on its own thread: the
     * sorted, limited scan of a pushed top-k page, or the count only
     * scan of a scalar filter. First the {@link Kind#SCALAR_INDEX} load
     * of {@code filterSql}'s index when the scan is filtered and the
     * statistics name one, then the {@link Kind#FILTER_SCAN} of the rows
     * the filter keeps (every row for an empty filter: Lance sorts after
     * the scan, so a page reads the sort columns of all of them),
     * streaming rows of {@code rowWidthBytes} (the row address plus the
     * sort columns). {@code fetch} is the page's limit, 0 for a count.
     * No heap term: the page keeps {@code fetch} rows, the count none.
     * {@code shape} names the scan in the message. The admission is
     * counted on the calling thread and released by the request's end
     * on that thread.
     */
    public static void admitExecutorFilterScan(
        String indexName,
        Dataset dataset,
        String filterSql,
        long nodeRows,
        long fetch,
        long rowWidthBytes,
        String shape
    ) {

        if (!enabled) {
            return;
        }
        long shardShare = shardShareBytes();
        Optional<TableStatistics> statistics = statisticsOf(dataset);
        boolean filtered = filterSql != null && !filterSql.isEmpty();
        double selectivity = filtered ? statistics.map(s -> filterSelectivity(filterSql, s)).orElse(FILTER_MATCH_RATIO_UNKNOWN) : 1d;
        Optional<ColumnStatistics.IndexSummary> index = filtered ? statistics.flatMap(s -> scalarIndexFor(filterSql, s)) : Optional.empty();
        if (index.isPresent()) {
            long tableRows = tableRows(statistics);
            IndexType type = index.get().type().orElse(IndexType.BTREE);
            long estimate = scalarIndexEstimateBytes(type, index.get().sizeBytes(), tableRows, selectivity, shardShare);
            String what = type.name().toLowerCase(Locale.ROOT)
                + " index ["
                + index.get().name()
                + "] over ["
                + indexName
                + "] answering the scan's filter ["
                + filterSql
                + "] with selectivity "
                + String.format(Locale.ROOT, "%.4f", selectivity);
            judge(Kind.SCALAR_INDEX, estimate, 0L, Long.MAX_VALUE, what, filterRemedy(), null);
        }
        long matching = filterScanMatchingRows(nodeRows, selectivity, fetch, !boundedShapesGated);
        long estimate = filterScanEstimateBytes(nodeRows, matching, rowWidthBytes, batchReadahead(), shardShare);
        String what = shape
            + " over ["
            + indexName
            + "]"
            + (filtered ? " of [" + filterSql + "]" : "")
            + ": "
            + matching
            + " of "
            + nodeRows
            + " rows read on this node at ["
            + NativeMemoryLimit.humanReadable(rowWidthBytes)
            + "] per row for the sort columns, plus ["
            + NativeMemoryLimit.humanReadable(FILTER_SCAN_BYTES_PER_MATCHING_ROW)
            + "] per matching row, the batches in flight and the read queue";
        judge(Kind.FILTER_SCAN, estimate, 0L, Long.MAX_VALUE, what, filterRemedy(), null);
    }

    private static String filterRemedy() {
        return "Narrow the filter, spread the table over more data nodes, or relax lance.admission.headroom / lance.admission.enabled.";
    }

    /**
     * Gate the nearest scan of {@code column} over {@code dataset}: the
     * {@link Kind#VECTOR_INDEX} load of the partitions it probes.
     * {@code nprobes} and {@code refineFactor} are the query's (0 for
     * Lance's defaults), {@code dimension} the vector column's. The
     * partition count comes from the table statistics
     * ({@link ColumnStatistics.IndexSummary#partitions}); when they
     * report none the whole index is taken as probed.
     */
    public static void admitVectorSearch(
        String indexName,
        Dataset dataset,
        String column,
        int k,
        int nprobes,
        int refineFactor,
        int dimension,
        LanceHitsAccounting ticket
    ) {
        if (!enabled) {
            return;
        }
        long shardShare = shardShareBytes();
        Optional<TableStatistics> statistics = statisticsOf(dataset);
        Optional<ColumnStatistics.IndexSummary> index = statistics.flatMap(s -> vectorIndexFor(column, s));
        long tableRows = tableRows(statistics);
        OptionalLong size = index.isPresent() ? index.get().sizeBytes() : OptionalLong.empty();
        long partitions = index.isPresent() ? index.get().partitions().orElse(0L) : 0L;
        int probes = nprobes > 0 ? nprobes : 1;
        long estimate = vectorIndexEstimateBytes(size, tableRows, probes, partitions, k, refineFactor, dimension, shardShare);
        String what = "nearest scan on ["
            + column
            + "] over ["
            + indexName
            + "]: "
            + index.map(i -> i.type().map(t -> t.name().toLowerCase(Locale.ROOT)).orElse("vector") + " index [" + i.name() + "] of [")
                .orElse("vector index (not in the statistics) of [")
            + NativeMemoryLimit.humanReadable(size.orElse(tableRows * VECTOR_INDEX_BYTES_PER_ROW))
            + "] probed with nprobes "
            + probes
            + (partitions > 0L ? " over " + partitions + " partitions" : " over an unknown partition count")
            + ", loaded twice while its partitions are concatenated, against an index cache shard of ["
            + NativeMemoryLimit.humanReadable(shardShare)
            + "]";
        String remedy = "Lower nprobes, attach the table to a node with a larger index cache, or relax lance.admission.headroom / "
            + "lance.admission.enabled.";
        judge(Kind.VECTOR_INDEX, estimate, 0L, Long.MAX_VALUE, what, remedy, ticket);
    }

    /**
     * Gate the {@code scans} parallel scans of a pushed aggregate over
     * {@code dataset}: the {@link Kind#SCALAR_INDEX} load of
     * {@code filterSql}'s index when the aggregate is filtered and the
     * statistics name one, then the {@link Kind#AGGREGATE_SCAN} of
     * {@code scannedRows} rows of {@code rowWidthBytes} with a group
     * state of {@code groups} groups and {@code metrics} metrics on the
     * heap, compared with {@code heapAvailableBytes}. Without a request
     * ticket (the runner has none) the admission is counted on the
     * calling thread and released by its next admission or
     * {@link #requestEnded()}.
     */
    public static void admitAggregateScan(
        String indexName,
        Dataset dataset,
        String filterSql,
        int scans,
        long scannedRows,
        long rowWidthBytes,
        long groups,
        int metrics,
        long heapAvailableBytes
    ) {
        if (!enabled) {
            return;
        }
        long shardShare = shardShareBytes();
        Optional<TableStatistics> statistics = statisticsOf(dataset);
        long rows = scannedRows;
        if (filterSql != null && !filterSql.isEmpty()) {
            double selectivity = statistics.map(s -> filterSelectivity(filterSql, s)).orElse(FILTER_MATCH_RATIO_UNKNOWN);
            Optional<ColumnStatistics.IndexSummary> index = statistics.flatMap(s -> scalarIndexFor(filterSql, s));
            if (index.isPresent()) {
                long tableRows = tableRows(statistics);
                IndexType type = index.get().type().orElse(IndexType.BTREE);
                long estimate = scalarIndexEstimateBytes(type, index.get().sizeBytes(), tableRows, selectivity, shardShare);
                String what = type.name().toLowerCase(Locale.ROOT)
                    + " index ["
                    + index.get().name()
                    + "] over ["
                    + indexName
                    + "] answering the aggregate's filter ["
                    + filterSql
                    + "] with selectivity "
                    + String.format(Locale.ROOT, "%.4f", selectivity);
                judge(Kind.SCALAR_INDEX, estimate, 0L, heapAvailableBytes, what, filterRemedy(), null);
            }
            rows = (long) (scannedRows * selectivity);
        }
        long estimate = aggregateScanEstimateBytes(scans, rows, rowWidthBytes, batchReadahead(), shardShare);
        long heap = aggregateScanHeapBytes(groups, metrics, scans);
        String what = "pushed aggregate over ["
            + indexName
            + "]: "
            + scans
            + " parallel scans over "
            + rows
            + " rows of ["
            + NativeMemoryLimit.humanReadable(rowWidthBytes)
            + "] each (read queue and batches in flight per scan); the group state of "
            + groups
            + " groups takes ["
            + NativeMemoryLimit.humanReadable(heap)
            + "] of heap";
        String remedy = "Lower lance.aggregation.pushdown_parallelism, spread the table over more data nodes, or relax "
            + "lance.admission.headroom / lance.admission.enabled.";
        judge(Kind.AGGREGATE_SCAN, estimate, heap, heapAvailableBytes, what, remedy, null);
    }

    /**
     * Record a {@link Kind#COLUMN_LOAD}: the heap copy of a column the
     * off-heap store could not hold is charged to the request breaker
     * before it is allocated ({@code LanceShardColumnCache.chargeHeap}),
     * which refuses with the same 429 the gate answers; the charge is
     * recorded here as the kind's estimate, and {@code refused} counts
     * it as a rejection of the kind.
     */
    public static void recordColumnLoad(long bytes, boolean refused) {
        LAST_ESTIMATE_BYTES.set(Math.max(0L, bytes));
        LAST_KIND.set(Kind.COLUMN_LOAD);
        LAST_SOURCE.set(Source.REQUEST);
        if (refused) {
            REJECTIONS.get(Kind.COLUMN_LOAD).incrementAndGet();
        }
    }

    /**
     * Judge the metadata warm up's full text probe over a table of
     * {@code rows} physical rows: the {@link Kind#FTS} estimate of
     * {@link #WARM_UP_PROBE_SHAPE}, the same formula a request's
     * bounded page is judged on, recorded under {@link Source#WARM_UP}.
     * Nothing is thrown: the warm up has no caller to answer 429 to, so
     * a refusal is returned as a {@link Decision} that is not admitted
     * (and counted as an {@code fts} rejection) and the caller skips the
     * probe. An admitted non zero estimate is counted in flight on the
     * calling thread like a ticketless request: the caller brackets the
     * probe's scan with {@link #scanStarted} and {@link #scanFinished}
     * and ends it with {@link #requestEnded()}, so what the probe leaves
     * behind is credited to the retained pool.
     */
    public static Decision admitWarmUpProbe(long rows) {
        long shardShare = shardShareBytes();
        long estimate = ftsEstimateBytes(
            rows,
            WARM_UP_PROBE_SHAPE.clauses(),
            WARM_UP_PROBE_SHAPE.phrase(),
            scanBufferEstimateBytes(rows, WARM_UP_PROBE_SHAPE),
            shardShare
        );
        return judge(Kind.FTS, Source.WARM_UP, estimate, 0L, Long.MAX_VALUE, null);
    }

    /**
     * Judge one path of a request: read the available memory and the
     * credit, decide, record, and either throw the 429 or count the
     * admission in flight.
     */
    private static void judge(
        Kind kind,
        long estimateBytes,
        long heapEstimateBytes,
        long heapAvailableBytes,
        String what,
        String remedy,
        LanceHitsAccounting ticket
    ) {
        Decision decision = judge(kind, Source.REQUEST, estimateBytes, heapEstimateBytes, heapAvailableBytes, ticket);
        if (!decision.admitted()) {
            throw rejection(kind, decision, heapAvailableBytes, headroomBytes, what, remedy);
        }
    }

    /**
     * Judge one path from {@code source}: read the available memory and
     * the credit, decide, record the estimate, kind and source, count a
     * refusal under the kind, and count an admitted non zero estimate in
     * flight. Returns the decision; the caller answers a refusal as its
     * source requires.
     */
    private static Decision judge(
        Kind kind,
        Source source,
        long estimateBytes,
        long heapEstimateBytes,
        long heapAvailableBytes,
        LanceHitsAccounting ticket
    ) {
        long availableNow = readAvailableMemoryNow();
        long credit = retainedCreditBytes(availableNow);
        Decision decision = decide(estimateBytes, heapEstimateBytes, availableNow, heapAvailableBytes, enabled, headroomBytes, credit);
        LAST_ESTIMATE_BYTES.set(decision.estimateBytes());
        LAST_KIND.set(kind);
        LAST_SOURCE.set(source);
        if (!decision.admitted()) {
            REJECTIONS.get(kind).incrementAndGet();
            return decision;
        }
        if (decision.estimateBytes() > 0L) {
            countInFlight(availableNow, decision.estimateBytes(), ticket);
        }
        return decision;
    }

    /**
     * Count an admitted non zero estimate in flight, once per request:
     * a ticket already counted (an earlier path of the same request)
     * adds nothing; a thread whose previous ticketless request never
     * reported its end is released by this admission. The pool samples
     * its before reading when this is the only request in flight.
     */
    private static void countInFlight(long availableNow, long estimateBytes, LanceHitsAccounting ticket) {
        if (ticket != null && ticket.markAdmitted() == false) {
            return;
        }
        long residentNow = readResidentSetNow();
        synchronized (LOCK) {
            if (ticket == null && Boolean.TRUE.equals(ADMITTED_ON_THREAD.get())) {
                // The previous request on this thread never reported
                // its end; do not let it hold the pool shut.
                inFlight = Math.max(0, inFlight - 1);
            }
            if (inFlight == 0) {
                POOL.scanAdmitted(availableNow, residentNow, estimateBytes);
            }
            inFlight++;
        }
        if (ticket == null) {
            ADMITTED_ON_THREAD.set(Boolean.TRUE);
        }
    }

    /**
     * The request that {@link #admit(String, long, Shape)} admitted on
     * this thread is over, whether or not its scan ran. A thread
     * without an admission is a no-op.
     */
    public static void requestEnded() {
        if (!Boolean.TRUE.equals(ADMITTED_ON_THREAD.get())) {
            return;
        }
        ADMITTED_ON_THREAD.remove();
        synchronized (LOCK) {
            inFlight = Math.max(0, inFlight - 1);
        }
    }

    /**
     * The request {@code ticket} belongs to is over; called by
     * {@link LanceHitsAccounting#close} when the executor closes the
     * request's search context. Releases the ticket's in flight count if
     * any of its paths was admitted, and the calling thread's ticketless
     * admission if one is pending there.
     */
    public static void requestEnded(LanceHitsAccounting ticket) {
        if (ticket != null && ticket.clearAdmitted()) {
            synchronized (LOCK) {
                inFlight = Math.max(0, inFlight - 1);
            }
        }
        requestEnded();
    }

    /** A gated shard scan starts on this node; the pool is not credited while it runs. */
    public static void scanStarted() {
        synchronized (LOCK) {
            activeScans++;
        }
    }

    /**
     * A gated shard scan ended on this node. When it was the last one
     * running and exactly one gated request is in flight, that request's
     * scan is the one that just completed: sample {@code MemAvailable}
     * and record what it left behind. With more in flight the reading
     * cannot be attributed and nothing is recorded.
     */
    public static void scanFinished() {
        boolean sample;
        synchronized (LOCK) {
            activeScans = Math.max(0, activeScans - 1);
            sample = activeScans == 0 && inFlight == 1 && POOL.awaitingCompletion();
        }
        if (!sample) {
            return;
        }
        long availableNow = readAvailableMemoryNow();
        long residentNow = readResidentSetNow();
        synchronized (LOCK) {
            if (activeScans == 0 && inFlight == 1) {
                POOL.scanCompleted(availableNow, residentNow);
            }
        }
    }

    /** Gated requests admitted with a non zero estimate and not yet ended, for tests. */
    static int inFlightForTests() {
        synchronized (LOCK) {
            return inFlight;
        }
    }

    /** Gated shard scans running, for tests. */
    static int activeScansForTests() {
        synchronized (LOCK) {
            return activeScans;
        }
    }

    /**
     * The shard share the decision compares the estimate with: the test
     * override when one is set, else the installed Session's sizing,
     * else zero (no Session installed means no cache can hold the
     * entry, so every estimate counts in full).
     */
    static long shardShareBytes() {
        long override = shardShareOverrideBytes;
        if (override > 0L) {
            return override;
        }
        NativeMemoryLimit.IndexCacheSizing sizing = LanceRegistry.indexCacheSizing();
        return sizing == null ? 0L : sizing.shardShareBytes();
    }

    /**
     * The 429 of one rejection: {@code [lance_admission] <kind> estimate
     * [X] exceeds available [Y] minus headroom [Z] ...} for a native
     * refusal, {@code ... heap estimate [X] exceeds the request breaker's
     * room [Y]} for a heap one, followed by what the scan is and what to
     * do. {@code bytesWanted} is the refused estimate and
     * {@code byteLimit} the memory it was judged against (clamped at
     * zero for the wire, which expects a non-negative limit).
     */
    static CircuitBreakingException rejection(
        Kind kind,
        Decision decision,
        long heapAvailableBytes,
        long headroomBytes,
        String what,
        String remedy
    ) {
        long availableForDisplay = Math.max(0L, decision.availableBytes());
        long credit = Math.max(0L, decision.retainedCreditBytes());
        String message;
        long wanted;
        long limit;
        if (decision.heapRefused()) {
            wanted = decision.heapEstimateBytes();
            limit = Math.max(0L, heapAvailableBytes);
            message = "["
                + LABEL
                + "] "
                + kind.key()
                + " heap estimate ["
                + NativeMemoryLimit.humanReadable(wanted)
                + "] exceeds the request breaker's room ["
                + NativeMemoryLimit.humanReadable(limit)
                + "]: "
                + what
                + ". "
                + remedy;
        } else {
            wanted = decision.estimateBytes();
            limit = Math.max(0L, availableForDisplay + credit);
            message = "["
                + LABEL
                + "] "
                + kind.key()
                + " estimate ["
                + NativeMemoryLimit.humanReadable(wanted)
                + "] exceeds available ["
                + NativeMemoryLimit.humanReadable(availableForDisplay)
                + "] minus headroom ["
                + NativeMemoryLimit.humanReadable(headroomBytes)
                + "] plus ["
                + NativeMemoryLimit.humanReadable(credit)
                + "] retained by earlier admitted scans: "
                + what
                + ". "
                + remedy;
        }
        return new CircuitBreakingException(message, wanted, limit, CircuitBreaker.Durability.TRANSIENT);
    }

    /** Reset the static holders, counters and the retained pool to their defaults, for tests. */
    static void resetForTests() {
        enabled = true;
        boundedShapesGated = true;
        headroomBytes = DEFAULT_HEADROOM.getBytes();
        shardShareOverrideBytes = 0L;
        memoryProbe = ScanAdmission::readAvailablePhysicalMemory;
        residentSetProbe = ScanAdmission::readResidentSetBytes;
        nativeLimitProbe = ScanAdmission::readNativeMemoryLimit;
        tableStatistics = null;
        for (AtomicLong counter : REJECTIONS.values()) {
            counter.set(0L);
        }
        LAST_ESTIMATE_BYTES.set(0L);
        LAST_KIND.set(null);
        LAST_SOURCE.set(null);
        ADMITTED_ON_THREAD.remove();
        synchronized (LOCK) {
            POOL.reset();
            inFlight = 0;
            activeScans = 0;
            availableMemoryOverride = null;
        }
    }

    /** Replace the available-memory probe, for tests; {@link #resetForTests} restores the real one. */
    static void setMemoryProbeForTests(LongSupplier probe) {
        memoryProbe = probe;
    }

    /** Replace the resident set probe, for tests; {@code -1} disables the guard. */
    static void setResidentSetProbeForTests(LongSupplier probe) {
        residentSetProbe = probe;
    }

    /** Replace the native memory limit the guard compares with, for tests; {@code -1} disables the guard. */
    static void setNativeLimitProbeForTests(LongSupplier probe) {
        nativeLimitProbe = probe;
    }

    /** The retained pool, for tests of its arithmetic through the static entry points. */
    static RetainedPool poolForTests() {
        return POOL;
    }
}
