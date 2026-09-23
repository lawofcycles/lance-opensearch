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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.secure_sm.AccessController;

/**
 * Admission control for full-text scans.
 *
 * <p>A full-text scan over an inverted index that does not fit one
 * shard of the index cache makes Lance rebuild the index's document
 * set in native memory, and that memory is outside the JVM and
 * outside every budget the plugin reads: the {@code lance_native}
 * breaker samples the Session caches the entry never enters, and the
 * {@code request} breaker counts the heap buffers, not the native
 * rebuild. Lance rebuilds the whole set for a bounded top-k page just
 * as it does for an unbounded shape (sort by a field, aggregations,
 * {@code size 0}, {@code _count}, {@code track_total_hits: true}),
 * so on a table whose document set is larger than the node's
 * available memory either kind of scan can end the node with a kernel
 * OOM kill before any accounting sees it.
 *
 * <p>The only lever the plugin has is refusing the scan before it
 * starts. {@link #admit} estimates the native memory of the scan as
 * the document set rebuild
 * ({@link NativeMemoryLimit#invertedIndexEntryEstimateBytes}) plus a
 * scan buffer term that grows with the rows the scan is expected to
 * return ({@link #scanBufferEstimateBytes}); the whole estimate is
 * zero when the document set fits the shard share, because a cached
 * index is not rebuilt per scan. It answers
 * {@link CircuitBreakingException} (HTTP 429) when the node's
 * available physical memory minus a headroom cannot hold the estimate.
 * Available memory is the kernel's {@code MemAvailable} from
 * {@code /proc/meminfo}, which counts the reclaimable page cache and
 * slab the kernel would hand to the scan on top of the free pages;
 * {@code MemFree} alone reads close to zero on a node whose page cache
 * is warm from serving queries, and a gate judging it refused scans
 * the node could serve. Where the file or the line does not exist
 * (macOS, Windows) the probe falls back to {@link OsProbe}'s free
 * physical memory. The check runs before {@code Dataset.newScan} is
 * created for the scan, so a refusal allocates nothing native.
 * Bounded top-k pages are judged by the same comparison because they
 * carry the same document set cost;
 * {@code lance.fts.admission.bounded_shapes_gated} set to
 * {@code false} restores the old pass-through for them.
 *
 * <p>An admitted scan leaves memory behind: the entries Lance admits
 * to its index cache next to the refused one (posting lists and the
 * per partition document row ids, which the {@code lance_native}
 * breaker accounts) and the pages the native allocator keeps after
 * the refused entry is dropped. {@code MemAvailable} therefore reads
 * lower after the scan than before it, while the next scan of the
 * same table reuses that memory instead of allocating it again. The
 * gate keeps a per node {@link RetainedPool} of this memory: the
 * difference between {@code MemAvailable} sampled when a non zero
 * estimate is admitted with nothing else in flight and
 * {@code MemAvailable} sampled when that request's scan completes, at
 * most the process's resident set growth over the same interval,
 * accumulated over scans, bounded by the largest estimate admitted
 * since the pool started, and decayed as {@code MemAvailable}
 * recovers. The decision adds the pool to the available memory. The
 * pool is credited only while no other gated request is in flight
 * and no full text scan is running (their memory is in use, not
 * reusable), and not when the process's resident set exceeds
 * {@code lance.native_memory.limit} plus the JVM heap by more than
 * the pool (something the plugin does not account holds memory).
 * {@link LanceFtsQuery} reports its shard scans through
 * {@link #scanStarted} and {@link #scanFinished};
 * {@link LanceHitsAccounting#close} reports the end of the request
 * through {@link #requestEnded}, on the thread {@link #admit} ran
 * on, so an in flight admission is released whether or not its scan
 * ran.
 *
 * <p>The settings ({@code lance.fts.admission.enabled},
 * {@code lance.fts.admission.headroom} and
 * {@code lance.fts.admission.bounded_shapes_gated}) live in static
 * holders read by every decision, the same pattern as
 * {@link LanceFtsQuery}'s probe parameters.
 * {@code lance.test.index_cache_shard_share} overrides the shard
 * share the decision compares with so integration tests can declare a
 * small fixture table as not fitting; zero (the default) reads the
 * installed Session's sizing.
 */
public final class FtsAdmission {

    /** Label the 429 is reported under, and the prefix of its message. */
    public static final String LABEL = "lance_fts_admission";

    /** Default for {@code lance.fts.admission.headroom}. */
    public static final ByteSizeValue DEFAULT_HEADROOM = new ByteSizeValue(8, ByteSizeUnit.GB);

    /**
     * Native bytes one returned row of the hits scan occupies in its
     * Arrow batches: the {@code _score} Float4 (4 bytes) plus the
     * {@code _rowaddr} UInt8 (8 bytes). The hits scan projects no data
     * column (see {@code LanceFtsQuery.HITS_SCAN_COLUMNS}), so the
     * physical row width of the scan is these two columns.
     */
    static final long HITS_SCAN_ROW_BYTES = 12L;

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

    /** The kernel's memory accounting on Linux. */
    private static final Path PROC_MEMINFO = Path.of("/proc/meminfo");

    /** The kernel's accounting of this process on Linux. */
    private static final Path PROC_SELF_STATUS = Path.of("/proc/self/status");

    private static volatile boolean enabled = true;
    private static volatile boolean boundedShapesGated = true;
    private static volatile long headroomBytes = DEFAULT_HEADROOM.getBytes();
    private static volatile long shardShareOverrideBytes = 0L;
    private static volatile LongSupplier memoryProbe = FtsAdmission::readAvailablePhysicalMemory;
    private static volatile LongSupplier residentSetProbe = FtsAdmission::readResidentSetBytes;
    private static volatile LongSupplier nativeLimitProbe = FtsAdmission::readNativeMemoryLimit;

    /**
     * Scripted available memory readings installed by
     * {@code lance.test.fts_admission_available_memory}: handed out one
     * per reading, the last one repeating; {@code null} when no override
     * is in force. Guarded by {@link #LOCK}.
     */
    private static ArrayDeque<Long> availableMemoryOverride;

    /** Full-text scans this node refused since it started. */
    private static final AtomicLong REJECTIONS = new AtomicLong();

    /** Estimate of the last decision, admitted or not. */
    private static final AtomicLong LAST_ESTIMATE_BYTES = new AtomicLong();

    /** Guards {@link #POOL}, {@link #inFlight} and {@link #activeScans}. */
    private static final Object LOCK = new Object();

    /** Memory earlier admitted scans left behind that the next one reuses. */
    private static final RetainedPool POOL = new RetainedPool();

    /** Gated requests with a non zero estimate admitted on this node and not yet ended. */
    private static int inFlight;

    /** Full text shard scans running on this node, gated or not. */
    private static int activeScans;

    /**
     * Set on the thread {@link #admit} admitted a non zero estimate on,
     * cleared by {@link #requestEnded} on the same thread, so the two
     * pair without the executor carrying a token.
     */
    private static final ThreadLocal<Boolean> ADMITTED_ON_THREAD = new ThreadLocal<>();

    /**
     * Memory that earlier admitted scans left in the process and that
     * the next scan of the kind reuses, measured as {@code MemAvailable}
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

    private FtsAdmission() {}

    /** Current value of the {@code lance.fts.admission.enabled} setting. */
    public static boolean enabled() {
        return enabled;
    }

    /** Install a new enabled flag; the next decision picks it up. */
    public static void setEnabled(boolean value) {
        enabled = value;
    }

    /** Current value of the {@code lance.fts.admission.bounded_shapes_gated} setting. */
    public static boolean boundedShapesGated() {
        return boundedShapesGated;
    }

    /** Install a new bounded gating flag; the next decision picks it up. */
    public static void setBoundedShapesGated(boolean value) {
        boundedShapesGated = value;
    }

    /** Current value of the {@code lance.fts.admission.headroom} setting, in bytes. */
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
     * Install the {@code lance.test.fts_admission_available_memory}
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
                queue.addLast(ByteSizeValue.parseBytesSizeValue(reading, "lance.test.fts_admission_available_memory").getBytes());
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

    /** Cumulative rejection count, for {@code GET /_lance/stats}. */
    public static long rejections() {
        return REJECTIONS.get();
    }

    /** Estimate of the last decision on this node, for {@code GET /_lance/stats}. */
    public static long lastEstimateBytes() {
        return LAST_ESTIMATE_BYTES.get();
    }

    /**
     * The available physical memory the next decision would be judged
     * against: the kernel's {@code MemAvailable} where
     * {@code /proc/meminfo} reports it, else {@link OsProbe}'s free
     * physical memory. Also reported as {@code fts.admission.available_bytes}
     * in {@code GET /_lance/stats}.
     */
    public static long availablePhysicalMemoryBytes() {
        return readAvailableMemoryNow();
    }

    /**
     * The retained memory the next decision would add to the available
     * memory, computed at the current {@code MemAvailable}: zero while a
     * gated request is in flight or a full text scan runs, zero when the
     * resident set guard blocks it, else the pool's credit. Also
     * reported as {@code fts.admission.retained_bytes} in
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

    /**
     * Full-text shape of one resolved query tree as the gate sees it:
     * whether the tree carries a {@link LanceFtsQuery} at all, whether
     * any of its scans is unbounded (the resolver leaves the scan
     * limit unbounded for sort, aggregations, {@code post_filter},
     * {@code size 0}, for every clause nested under another scoring
     * query and under a security reader wrapper; an exact match count
     * also runs the unbounded count-only scan), and the largest
     * bounded scan limit otherwise, which is the rows the top-k page's
     * scan returns at most.
     */
    public record Shape(boolean hasFtsClause, boolean unbounded, long boundedScanRows) {

        /** A query tree without a full-text clause; never gated. */
        public static final Shape NONE = new Shape(false, false, 0L);
    }

    /**
     * Classify {@code query} for the gate. A tree without a
     * {@link LanceFtsQuery} is {@link Shape#NONE}. A tree where any
     * full-text clause carries no scan limit, or whose exact match
     * count ({@code track_total_hits: true}) would run the unbounded
     * count-only scan, is unbounded. Everything else is a bounded
     * top-k page whose scan returns at most the largest clause limit.
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
        long boundedScanRows = 0L;
        for (LanceFtsQuery fts : found) {
            if (fts.scanLimit() == LanceFtsQuery.SCAN_LIMIT_UNBOUNDED) {
                return new Shape(true, true, 0L);
            }
            boundedScanRows = Math.max(boundedScanRows, fts.scanLimit());
        }
        if (trackTotalHitsAccurate) {
            return new Shape(true, true, 0L);
        }
        return new Shape(true, false, boundedScanRows);
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
     * {@code lance.fts.admission.bounded_shapes_gated} is {@code true}.
     */
    public static boolean gates(Shape shape) {
        return shape.hasFtsClause() && (shape.unbounded() || boundedShapesGated);
    }

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
     * One admission decision: whether the scan may start, the estimate
     * it was judged on ({@code 0} when the document set fits the shard
     * share), the available memory left after the headroom (which can
     * be negative when the headroom exceeds the node's available
     * memory) and the retained memory credited on top of it.
     */
    public record Decision(boolean admitted, long estimateBytes, long availableBytes, long retainedCreditBytes) {
    }

    /**
     * Decide whether a full-text scan over a table of {@code rows}
     * rows, carrying {@code scanBufferBytes} of expected scan buffers,
     * may start. The document set term is
     * {@link NativeMemoryLimit#invertedIndexEntryEstimateBytes}; when
     * it fits {@code shardShareBytes} (the same comparison the
     * attach-time WARN makes) the whole estimate is zero, because a
     * document set the cache holds is not rebuilt per scan. A non-zero
     * estimate is admitted only when the available physical memory
     * minus the headroom plus {@code retainedCreditBytes} (the memory
     * earlier admitted scans left behind, see {@link RetainedPool}) is
     * positive and holds it; a disabled gate admits everything.
     */
    public static Decision decide(
        long rows,
        long scanBufferBytes,
        long shardShareBytes,
        long availableMemoryBytes,
        boolean enabled,
        long headroomBytes,
        long retainedCreditBytes
    ) {
        long entry = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        long estimate = entry <= shardShareBytes ? 0L : entry + Math.max(0L, scanBufferBytes);
        long available = availableMemoryBytes - headroomBytes;
        long credit = Math.max(0L, retainedCreditBytes);
        long judged = available + credit;
        boolean admitted = !enabled || estimate == 0L || (judged > 0L && estimate <= judged);
        return new Decision(admitted, estimate, available, credit);
    }

    /**
     * Gate one full-text scan of {@code shape} over {@code indexName},
     * a table of {@code rows} physical rows. Reads the shard share
     * from the installed Session (or the test override), the available
     * physical memory from {@link #availablePhysicalMemoryBytes}, the
     * retained credit from the pool and the settings from the static
     * holders, records the estimate, and throws
     * {@link CircuitBreakingException} on a rejection. An admitted non
     * zero estimate is counted in flight on the calling thread until
     * {@link #requestEnded} runs there, and samples the pool's before
     * reading when it is the only one in flight.
     */
    public static void admit(String indexName, long rows, Shape shape) {
        long shardShare = shardShareBytes();
        long availableNow = readAvailableMemoryNow();
        long credit = retainedCreditBytes(availableNow);
        Decision decision = decide(rows, scanBufferEstimateBytes(rows, shape), shardShare, availableNow, enabled, headroomBytes, credit);
        LAST_ESTIMATE_BYTES.set(decision.estimateBytes());
        if (!decision.admitted()) {
            REJECTIONS.incrementAndGet();
            throw rejection(
                indexName,
                shape.unbounded(),
                decision.estimateBytes(),
                shardShare,
                decision.availableBytes(),
                headroomBytes,
                decision.retainedCreditBytes()
            );
        }
        if (decision.estimateBytes() > 0L) {
            long residentNow = readResidentSetNow();
            synchronized (LOCK) {
                if (Boolean.TRUE.equals(ADMITTED_ON_THREAD.get())) {
                    // The previous request on this thread never reported
                    // its end; do not let it hold the pool shut.
                    inFlight = Math.max(0, inFlight - 1);
                }
                if (inFlight == 0) {
                    POOL.scanAdmitted(availableNow, residentNow, decision.estimateBytes());
                }
                inFlight++;
            }
            ADMITTED_ON_THREAD.set(Boolean.TRUE);
        }
    }

    /**
     * The request that {@link #admit} admitted on this thread is over,
     * whether or not its scan ran; called by
     * {@link LanceHitsAccounting#close} when the executor closes the
     * request's search context. A thread without an admission is a
     * no-op, so searchers outside the fragment path cost nothing here.
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

    /** A full text shard scan starts on this node; the pool is not credited while it runs. */
    public static void scanStarted() {
        synchronized (LOCK) {
            activeScans++;
        }
    }

    /**
     * A full text shard scan ended on this node. When it was the last
     * one running and exactly one gated request is in flight, that
     * request's scan is the one that just completed: sample
     * {@code MemAvailable} and record what it left behind. With more in
     * flight the reading cannot be attributed and nothing is recorded.
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

    /** Full text shard scans running, for tests. */
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
     * The 429 of one rejection. {@code bytesWanted} is the estimate and
     * {@code byteLimit} the available memory plus the retained credit
     * (clamped at zero for the wire, which expects a non-negative
     * limit).
     */
    static CircuitBreakingException rejection(
        String indexName,
        boolean unbounded,
        long estimateBytes,
        long shardShareBytes,
        long availableBytes,
        long headroomBytes,
        long retainedCreditBytes
    ) {
        long availableForDisplay = Math.max(0L, availableBytes);
        long credit = Math.max(0L, retainedCreditBytes);
        String remedy = unbounded
            ? "Bound the shape (a top k page without sort or aggregations), attach the table to a node with a larger "
                + "index cache, or relax lance.fts.admission.headroom / lance.fts.admission.enabled."
            : "Attach the table to a node with a larger index cache, or relax lance.fts.admission.bounded_shapes_gated / "
                + "lance.fts.admission.headroom / lance.fts.admission.enabled.";
        String message = "["
            + LABEL
            + "] "
            + (unbounded ? "unbounded full text scan" : "bounded full text page")
            + " over ["
            + indexName
            + "] needs an estimated ["
            + NativeMemoryLimit.humanReadable(estimateBytes)
            + "] of native memory for the inverted index document set that does not fit the index cache shard (["
            + NativeMemoryLimit.humanReadable(shardShareBytes)
            + "]) and the scan buffers; the node has ["
            + NativeMemoryLimit.humanReadable(availableForDisplay)
            + "] available after ["
            + NativeMemoryLimit.humanReadable(headroomBytes)
            + "] headroom plus ["
            + NativeMemoryLimit.humanReadable(credit)
            + "] retained by earlier full text scans. "
            + remedy;
        return new CircuitBreakingException(
            message,
            estimateBytes,
            Math.max(0L, availableForDisplay + credit),
            CircuitBreaker.Durability.TRANSIENT
        );
    }

    /** Reset the static holders, counters and the retained pool to their defaults, for tests. */
    static void resetForTests() {
        enabled = true;
        boundedShapesGated = true;
        headroomBytes = DEFAULT_HEADROOM.getBytes();
        shardShareOverrideBytes = 0L;
        memoryProbe = FtsAdmission::readAvailablePhysicalMemory;
        residentSetProbe = FtsAdmission::readResidentSetBytes;
        nativeLimitProbe = FtsAdmission::readNativeMemoryLimit;
        REJECTIONS.set(0L);
        LAST_ESTIMATE_BYTES.set(0L);
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
