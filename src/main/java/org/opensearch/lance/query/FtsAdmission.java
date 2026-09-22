/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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

/**
 * Admission control for unbounded full-text scans.
 *
 * <p>An unbounded full-text shape (sort by a field, aggregations,
 * {@code size 0}, {@code _count}, {@code track_total_hits: true})
 * makes Lance walk the whole inverted index. When that index does not
 * fit one shard of the index cache, Lance rebuilds its document set in
 * native memory for the scan, and that memory is outside the JVM and
 * outside every budget the plugin reads: the {@code lance_native}
 * breaker samples the Session caches the entry never enters, and the
 * {@code request} breaker counts the heap buffers, not the native
 * rebuild. On a table whose document set is larger than the node's
 * free memory the scan therefore ends the node with a kernel OOM kill
 * before any accounting sees it.
 *
 * <p>The only lever the plugin has is refusing the scan before it
 * starts. {@link #admit} estimates the document set as
 * {@link NativeMemoryLimit#invertedIndexEntryEstimateBytes} (zero when
 * the index fits the shard share, so a cached index passes without
 * arithmetic against free memory) and answers
 * {@link CircuitBreakingException} (HTTP 429) when the node's free
 * physical memory minus a headroom cannot hold it. The check runs
 * before {@code Dataset.newScan} is created for the scan, so a refusal
 * allocates nothing native. Bounded top-k pages (no sort, no
 * aggregations, no {@code post_filter}) are not gated: they rebuild
 * the same document set, but they are the interactive shape and their
 * behaviour is kept, tracked as a follow-up.
 *
 * <p>The two settings ({@code lance.fts.admission.enabled} and
 * {@code lance.fts.admission.headroom}) live in static holders read by
 * every decision, the same pattern as {@link LanceFtsQuery}'s probe
 * parameters. {@code lance.test.index_cache_shard_share} overrides the
 * shard share the decision compares with so integration tests can
 * declare a small fixture table as not fitting; zero (the default)
 * reads the installed Session's sizing.
 */
public final class FtsAdmission {

    /** Label the 429 is reported under, and the prefix of its message. */
    public static final String LABEL = "lance_fts_admission";

    /** Default for {@code lance.fts.admission.headroom}. */
    public static final ByteSizeValue DEFAULT_HEADROOM = new ByteSizeValue(8, ByteSizeUnit.GB);

    private static volatile boolean enabled = true;
    private static volatile long headroomBytes = DEFAULT_HEADROOM.getBytes();
    private static volatile long shardShareOverrideBytes = 0L;

    /** Unbounded full-text scans this node refused since it started. */
    private static final AtomicLong REJECTIONS = new AtomicLong();

    /** Document set estimate of the last decision, admitted or not. */
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
     * One admission decision: whether the scan may start, the document
     * set estimate it was judged on ({@code 0} when the index fits the
     * shard share) and the free memory left after the headroom (which
     * can be negative when the headroom exceeds free memory).
     */
    public record Decision(boolean admitted, long estimateBytes, long availableBytes) {
    }

    /**
     * Decide whether an unbounded full-text scan over a table of
     * {@code rows} rows may start. The estimate is
     * {@link NativeMemoryLimit#invertedIndexEntryEstimateBytes}, zeroed
     * when it fits {@code shardShareBytes} (the same comparison the
     * attach-time WARN makes): a document set the cache holds is not
     * rebuilt per scan. A non-zero estimate is admitted only when the
     * free physical memory minus the headroom is positive and holds it;
     * a disabled gate admits everything.
     */
    public static Decision decide(long rows, long shardShareBytes, long freePhysicalMemoryBytes, boolean enabled, long headroomBytes) {
        long entry = NativeMemoryLimit.invertedIndexEntryEstimateBytes(rows);
        long estimate = entry <= shardShareBytes ? 0L : entry;
        long available = freePhysicalMemoryBytes - headroomBytes;
        boolean admitted = !enabled || estimate == 0L || (available > 0L && estimate <= available);
        return new Decision(admitted, estimate, available);
    }

    /**
     * Gate one unbounded full-text scan over {@code indexName}, a table
     * of {@code rows} physical rows. Reads the shard share from the
     * installed Session (or the test override), the free physical
     * memory from {@link OsProbe} and the settings from the static
     * holders, records the estimate, and throws
     * {@link CircuitBreakingException} on a rejection.
     */
    public static void admit(String indexName, long rows) {
        long shardShare = shardShareBytes();
        Decision decision = decide(rows, shardShare, OsProbe.getInstance().getFreePhysicalMemorySize(), enabled, headroomBytes);
        LAST_ESTIMATE_BYTES.set(decision.estimateBytes());
        if (decision.admitted()) {
            return;
        }
        REJECTIONS.incrementAndGet();
        throw rejection(indexName, decision.estimateBytes(), shardShare, decision.availableBytes(), headroomBytes);
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
        long estimateBytes,
        long shardShareBytes,
        long availableBytes,
        long headroomBytes
    ) {
        long availableForDisplay = Math.max(0L, availableBytes);
        String message = "["
            + LABEL
            + "] unbounded full text scan over ["
            + indexName
            + "] needs an estimated ["
            + NativeMemoryLimit.humanReadable(estimateBytes)
            + "] of native memory for the inverted index document set that does not fit the index cache shard (["
            + NativeMemoryLimit.humanReadable(shardShareBytes)
            + "]); the node has ["
            + NativeMemoryLimit.humanReadable(availableForDisplay)
            + "] available after ["
            + NativeMemoryLimit.humanReadable(headroomBytes)
            + "] headroom. Bound the shape (a top k page without sort or aggregations), attach the table to a node with a larger "
            + "index cache, or relax lance.fts.admission.headroom / lance.fts.admission.enabled.";
        return new CircuitBreakingException(message, estimateBytes, availableForDisplay, CircuitBreaker.Durability.TRANSIENT);
    }

    /**
     * Whether serving {@code query} runs an unbounded full-text scan.
     * True when any {@link LanceFtsQuery} in the tree carries no scan
     * limit (the resolver leaves it unbounded for sort, aggregations,
     * {@code post_filter}, {@code size 0}, and for every clause nested
     * under another scoring query, and under a security reader
     * wrapper), and for a bounded top-k page whose exact match count
     * ({@code track_total_hits: true}) would run the unbounded
     * count-only scan. A query without a full-text clause never runs
     * one.
     */
    public static boolean runsUnboundedFtsScan(Query query, boolean trackTotalHitsAccurate) {
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
            return false;
        }
        for (LanceFtsQuery fts : found) {
            if (fts.scanLimit() == LanceFtsQuery.SCAN_LIMIT_UNBOUNDED) {
                return true;
            }
        }
        return trackTotalHitsAccurate;
    }

    /** Reset the static holders and counters to their defaults, for tests. */
    static void resetForTests() {
        enabled = true;
        headroomBytes = DEFAULT_HEADROOM.getBytes();
        shardShareOverrideBytes = 0L;
        REJECTIONS.set(0L);
        LAST_ESTIMATE_BYTES.set(0L);
    }
}
