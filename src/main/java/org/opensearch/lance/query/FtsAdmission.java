/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private static volatile boolean enabled = true;
    private static volatile boolean boundedShapesGated = true;
    private static volatile long headroomBytes = DEFAULT_HEADROOM.getBytes();
    private static volatile long shardShareOverrideBytes = 0L;
    private static volatile LongSupplier memoryProbe = FtsAdmission::readAvailablePhysicalMemory;

    /** Full-text scans this node refused since it started. */
    private static final AtomicLong REJECTIONS = new AtomicLong();

    /** Estimate of the last decision, admitted or not. */
    private static final AtomicLong LAST_ESTIMATE_BYTES = new AtomicLong();

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
        return memoryProbe.getAsLong();
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
        for (String line : memInfoLines) {
            if (line.startsWith("MemAvailable:") == false) {
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
        long returnedRows = shape.unbounded()
            ? (long) (Math.max(0L, rows) * UNBOUNDED_MATCH_RATIO)
            : Math.max(0L, shape.boundedScanRows());
        return (long) (returnedRows * HITS_SCAN_ROW_BYTES * SCAN_BUFFER_FACTOR);
    }

    /**
     * One admission decision: whether the scan may start, the estimate
     * it was judged on ({@code 0} when the document set fits the shard
     * share) and the available memory left after the headroom (which
     * can be negative when the headroom exceeds the node's available
     * memory).
     */
    public record Decision(boolean admitted, long estimateBytes, long availableBytes) {
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
     * minus the headroom is positive and holds it; a disabled gate
     * admits everything.
     */
    public static Decision decide(
        long rows,
        long scanBufferBytes,
        long shardShareBytes,
        long availableMemoryBytes,
        boolean enabled,
        long headroomBytes
    ) {
        long entry = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        long estimate = entry <= shardShareBytes ? 0L : entry + Math.max(0L, scanBufferBytes);
        long available = availableMemoryBytes - headroomBytes;
        boolean admitted = !enabled || estimate == 0L || (available > 0L && estimate <= available);
        return new Decision(admitted, estimate, available);
    }

    /**
     * Gate one full-text scan of {@code shape} over {@code indexName},
     * a table of {@code rows} physical rows. Reads the shard share
     * from the installed Session (or the test override), the available
     * physical memory from {@link #availablePhysicalMemoryBytes} and
     * the settings from the static holders, records the estimate, and
     * throws {@link CircuitBreakingException} on a rejection.
     */
    public static void admit(String indexName, long rows, Shape shape) {
        long shardShare = shardShareBytes();
        Decision decision = decide(rows, scanBufferEstimateBytes(rows, shape), shardShare, memoryProbe.getAsLong(), enabled, headroomBytes);
        LAST_ESTIMATE_BYTES.set(decision.estimateBytes());
        if (decision.admitted()) {
            return;
        }
        REJECTIONS.incrementAndGet();
        throw rejection(indexName, shape.unbounded(), decision.estimateBytes(), shardShare, decision.availableBytes(), headroomBytes);
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
     * {@code byteLimit} the available memory (clamped at zero for the
     * wire, which expects a non-negative limit).
     */
    static CircuitBreakingException rejection(
        String indexName,
        boolean unbounded,
        long estimateBytes,
        long shardShareBytes,
        long availableBytes,
        long headroomBytes
    ) {
        long availableForDisplay = Math.max(0L, availableBytes);
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
            + "] headroom. "
            + remedy;
        return new CircuitBreakingException(message, estimateBytes, availableForDisplay, CircuitBreaker.Durability.TRANSIENT);
    }

    /** Reset the static holders and counters to their defaults, for tests. */
    static void resetForTests() {
        enabled = true;
        boundedShapesGated = true;
        headroomBytes = DEFAULT_HEADROOM.getBytes();
        shardShareOverrideBytes = 0L;
        memoryProbe = FtsAdmission::readAvailablePhysicalMemory;
        REJECTIONS.set(0L);
        LAST_ESTIMATE_BYTES.set(0L);
    }

    /** Replace the available-memory probe, for tests; {@link #resetForTests} restores the real one. */
    static void setMemoryProbeForTests(LongSupplier probe) {
        memoryProbe = probe;
    }
}
