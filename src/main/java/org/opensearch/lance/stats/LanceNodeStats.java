/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.query.ScanAdmission;

/**
 * One node's view of the plugin's caches at the moment
 * {@link LanceStatsCollector#collect()} ran: the snapshot cache, the
 * off-heap column store and the heap columns its misses put on the
 * request breaker, the native memory the {@code lance_native}
 * breaker accounts for, the index cache's capacity and shard layout and
 * the full-text probe limit in force. Read only; every number is a plain
 * counter or gauge read from the owning component.
 *
 * <p>Rendered as the {@code snapshots}, {@code column_store},
 * {@code request_cache}, {@code native_memory}, {@code fts},
 * {@code admission}, {@code warm_up}, {@code plan}, {@code freshness},
 * {@code fetch}, {@code fetch_cache} and {@code indices} objects of one node in
 * {@code GET /_lance/stats}. {@code request_cache}
 * is the coordinator result cache ({@link RequestCacheStats}); {@code admission} carries the admission
 * gate's settings in force, its rejections per kind, the estimate,
 * kind and source (a request or the warm up) of its last decision, the
 * node's available memory and the memory earlier admitted scans
 * retained together with the identity of the scans it is credited to;
 * {@code warm_up} carries the mode in force and one
 * {@link LanceWarmUpStatus} per Lance-backed index the node has seen
 * since it started; {@code plan.statistics} the planner's table
 * statistics cache (entries held, milliseconds spent collecting, the
 * collections queued or running, and how many plans were made without
 * statistics because their version was not collected yet);
 * {@code plan.refinements} how often the fragment executor moved a
 * pushed operation of a shipped plan to the Lucene side, per reason;
 * {@code plan.executed} how many fragment requests the Lance scan and
 * Lucene each answered; {@code plan.pruned} how many fragments the
 * executor left out of its scans because the shipped plan's zone map
 * pruning excluded them;
 * {@code freshness} the node's checks of the Lance backed shards it
 * holds against their tables ({@link FreshnessStats}), with the mapping
 * updates the cluster manager refused per index under
 * {@code mapping_errors};
 * {@code fetch} the take scans the node's fragment executors issued for
 * the rows behind hits and for the sort or aggregation columns of small
 * hit sets ({@link FetchStats});
 * {@code fetch_cache} the node's cache of the rows behind hits
 * ({@link FetchCacheStats});
 * {@code indices} the shard reader of every Lance-backed shard the node
 * hosts.
 *
 * <p>The stream opens with {@link #WIRE_VERSION} (see
 * {@link WireVersion}): the stats are the whole payload of the per node
 * response {@link LanceStatsNodeResponse} carries back to the
 * coordinator. Version 1 laid out every figure but the pruned fragment
 * counter, which version 2 added as a block a coordinator of the
 * previous plugin version steps over, so a mixed version cluster
 * answers {@code GET /_lance/stats} without that counter instead of
 * failing; version 3 added the source of the last admission decision
 * the same way, and an older coordinator shows it as {@code none};
 * version 4 added the freshness service's refused mapping updates per
 * index as another such block, shown as empty by an older coordinator;
 * version 5 added the statistics collections pending and the plans
 * made without statistics, shown as zero by an older coordinator;
 * version 6 added the identity of the scans that filled the admission
 * gate's retained pool, shown as {@code none} by an older coordinator;
 * version 7 added the coordinator result cache's figures
 * ({@code request_cache}), shown as disabled with zero counters by an
 * older coordinator; version 8 added the fetch take counters, shown as
 * zero by an older coordinator; version 9 added the fetch cache's
 * figures ({@code fetch_cache}), shown as disabled with zero counters by
 * an older coordinator.
 */
public final class LanceNodeStats implements Writeable, ToXContentFragment {

    /**
     * The wire format's version, the first field written and the first
     * read; 2 added the pruned fragment counter, 3 the source of the
     * last admission decision, 4 the refused mapping updates of the
     * freshness checks, 5 the pending statistics collections and the
     * plans made without statistics, 6 the identity of the scans the
     * retained pool was filled by, 7 the result cache figures, 8 the fetch
     * take counters, 9 the fetch cache figures.
     */
    public static final int WIRE_VERSION = 9;

    private final boolean cacheEnabled;
    private final int snapshotCount;
    private final int retiredSnapshotCount;
    private final long datasetOpenCount;
    private final long snapshotBuildCount;
    private final long snapshotHitCount;

    private final long columnStoreBytes;
    private final long columnStoreLimitBytes;
    private final int columnStoreEntries;
    private final long columnStoreHits;
    private final long columnStoreLoads;
    private final long columnStoreEvictions;
    private final long columnStoreBudgetMisses;
    private final long heapFallbackBytes;
    private final long heapFallbackRejections;

    private final long nativeEstimatedBytes;
    private final long sessionBytes;
    private final long indexCacheCapacityBytes;
    private final int indexCacheShards;
    private final long indexCacheShardShareBytes;

    private final int ftsSubsetProbeLimit;
    /**
     * Scans the node's admission gate refused since it started, keyed by
     * kind ({@code fts}, {@code scalar_index}, {@code vector_index},
     * {@code filter_scan}, {@code aggregate_scan}, {@code column_load});
     * every kind present, zero when it never refused.
     */
    private final Map<String, Long> admissionRejections;
    private final long admissionLastEstimateBytes;
    private final String admissionLastKind;
    /** {@code request}, {@code warm_up}, or {@code none} before the first decision and from a node that does not report it. */
    private final String admissionLastSource;
    private final long admissionAvailableBytes;
    private final long admissionRetainedBytes;
    /**
     * Identity of the scans that filled the retained pool
     * ({@code kind:table:columns}, the only scans {@code admissionRetainedBytes}
     * is credited to), {@code none} before the first admission and from a
     * node that does not report it.
     */
    private final String admissionRetainedScope;

    private final String warmUpMode;
    private final List<LanceWarmUpStatus> warmUps;
    private final List<IndexReaderStats> indices;
    private final List<LocalCloneStats> localClones;
    /**
     * How many times this node's fragment executor moved a pushed
     * operation of a shipped plan to the Lucene side, per reason
     * ({@code security_wrapper}, {@code sort_field_type},
     * {@code aggregate_resolution}); every reason is present, zero when
     * it never fired.
     */
    private final Map<String, Long> planRefinements;
    /**
     * How many fragment requests this node's executor answered through
     * the Lance scan ({@code pushed_scan}: an ordered page or a Substrait
     * aggregate) and through Lucene's collector and aggregators
     * ({@code lucene}); both keys always present.
     */
    private final Map<String, Long> planExecuted;
    /**
     * How many fragments this node's executor left out of its scans
     * because the shipped plan's zone map pruning excluded them, summed
     * over every request since the node started.
     */
    private final long planPrunedFragments;

    private final int planStatisticsTables;
    private final long planStatisticsCollectMillisTotal;
    /** Statistics collections queued or running on this node. */
    private final int planStatisticsPending;
    /** Plans this node made without table statistics because their version was not collected yet. */
    private final long planStatisticsPlannedWithout;
    private final FreshnessStats freshness;
    /** The coordinator result cache of this node ({@link RequestCacheStats}); {@link RequestCacheStats#NONE} from an older node. */
    private final RequestCacheStats requestCache;
    private final FetchStats fetch;
    /** The fetch cache of this node ({@link FetchCacheStats}); {@link FetchCacheStats#NONE} from an older node. */
    private final FetchCacheStats fetchCache;

    /**
     * The fetch cache's figures: whether it is enabled, the bytes it
     * holds against its limit, the entries (one per cell), and since the
     * node started the cells served from it ({@code hits}), the cells
     * looked up and not held ({@code misses}), the entries dropped for
     * room or age ({@code evictions}), the entries dropped because their
     * table version's snapshot closed, their index was deleted or its
     * cache was cleared ({@code invalidations}), the rows not cached
     * because their index has a reader wrapper ({@code skipped}) and the
     * rows rendered without a take because every projected cell was
     * held ({@code rowsServed}). Travels in the version 9 block of
     * {@link LanceNodeStats}.
     */
    public record FetchCacheStats(boolean enabled, long sizeBytes, long limitBytes, int entries, long hits, long misses, long evictions,
        long invalidations, long skipped, long rowsServed) implements Writeable {

        /** What an older node stands for: disabled, nothing held, nothing counted. */
        public static final FetchCacheStats NONE = new FetchCacheStats(false, 0L, 0L, 0, 0L, 0L, 0L, 0L, 0L, 0L);

        public FetchCacheStats(StreamInput in) throws IOException {
            this(
                in.readBoolean(),
                in.readVLong(),
                in.readVLong(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
            out.writeVLong(sizeBytes);
            out.writeVLong(limitBytes);
            out.writeVInt(entries);
            out.writeVLong(hits);
            out.writeVLong(misses);
            out.writeVLong(evictions);
            out.writeVLong(invalidations);
            out.writeVLong(skipped);
            out.writeVLong(rowsServed);
        }
    }

    /**
     * The coordinator result cache's figures: whether it is enabled, the
     * bytes it holds against its limit, the entries, and since the node
     * started the requests served from it ({@code hits}), the eligible
     * requests it did not hold ({@code misses}), the entries dropped
     * for room or age ({@code evictions}), the entries dropped because
     * their index was deleted or its cache cleared
     * ({@code invalidations}) and the requests not cached because of
     * their shape, their target, an opt out, a reader wrapper, a
     * partial answer or their size ({@code skipped}). Travels in the
     * version 7 block of {@link LanceNodeStats}.
     */
    public record RequestCacheStats(boolean enabled, long sizeBytes, long limitBytes, int entries, long hits, long misses, long evictions,
        long invalidations, long skipped) implements Writeable {

        /** What an older node stands for: disabled, nothing held, nothing counted. */
        public static final RequestCacheStats NONE = new RequestCacheStats(false, 0L, 0L, 0, 0L, 0L, 0L, 0L, 0L);

        public RequestCacheStats(StreamInput in) throws IOException {
            this(
                in.readBoolean(),
                in.readVLong(),
                in.readVLong(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
            out.writeVLong(sizeBytes);
            out.writeVLong(limitBytes);
            out.writeVInt(entries);
            out.writeVLong(hits);
            out.writeVLong(misses);
            out.writeVLong(evictions);
            out.writeVLong(invalidations);
            out.writeVLong(skipped);
        }
    }

    /**
     * The take scans this node's fragment executors issued since it
     * started, for the rows behind the hits of a page ({@code _id},
     * {@code _source}) and for the sort or aggregation column of a
     * small full text or vector hit set: how many scans ran
     * ({@code takeCount}, split by caller into {@code storedFieldsTakes}
     * and {@code columnTakes}), how many row addresses they carried
     * ({@code takeRows}), how many columns they projected summed over
     * the scans ({@code takeColumns}), their wall time summed
     * ({@code takeMillisTotal}) and the longest single scan
     * ({@code takeMaxMillis}). Travels in the version 8 block, so a
     * node of an older plugin version reports {@link #NONE}.
     */
    public record FetchStats(long takeCount, long takeRows, long takeColumns, long takeMillisTotal, long takeMaxMillis,
        long storedFieldsTakes, long columnTakes) implements Writeable {

        public static final FetchStats NONE = new FetchStats(0L, 0L, 0L, 0L, 0L, 0L, 0L);

        public FetchStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(takeCount);
            out.writeVLong(takeRows);
            out.writeVLong(takeColumns);
            out.writeVLong(takeMillisTotal);
            out.writeVLong(takeMaxMillis);
            out.writeVLong(storedFieldsTakes);
            out.writeVLong(columnTakes);
        }
    }

    /**
     * This node's freshness checks of the Lance backed shards it holds:
     * how many indexes it tracks, how many checks ran, how many found
     * the table (or its tag) at another version than the shard serves,
     * how many mapping updates were sent and how many were skipped
     * because the derived mapping equalled the current one, how many
     * indexes were rebuilt for a keyword to lance_text flip, how many
     * checks failed, when the last check ran (epoch millis, 0 when
     * none ran), and per index the message of the last mapping update
     * the cluster manager refused (empty once a later check applied a
     * mapping or found nothing to apply). The counters are base fields
     * of {@link LanceNodeStats}; the refused updates travel in its
     * version 4 block, so {@link #FreshnessStats(StreamInput)} reads
     * the counters alone and {@link #withMappingErrors} adds the map.
     */
    public record FreshnessStats(int tracked, long checks, long moves, long mappingUpdates, long mappingUnchanged, long rebuilds,
        long failures, long lastCheckMillis, Map<String, String> mappingErrors) implements Writeable {

        public static final FreshnessStats NONE = new FreshnessStats(0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        public FreshnessStats(
            int tracked,
            long checks,
            long moves,
            long mappingUpdates,
            long mappingUnchanged,
            long rebuilds,
            long failures,
            long lastCheckMillis
        ) {
            this(tracked, checks, moves, mappingUpdates, mappingUnchanged, rebuilds, failures, lastCheckMillis, Map.of());
        }

        public FreshnessStats {
            mappingErrors = mappingErrors == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(mappingErrors));
        }

        /** The counters alone, as the base fields of {@link LanceNodeStats} carry them. */
        public FreshnessStats(StreamInput in) throws IOException {
            this(
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        /** These counters with {@code mappingErrors} as the refused updates. */
        public FreshnessStats withMappingErrors(Map<String, String> mappingErrors) {
            return new FreshnessStats(
                tracked,
                checks,
                moves,
                mappingUpdates,
                mappingUnchanged,
                rebuilds,
                failures,
                lastCheckMillis,
                mappingErrors
            );
        }

        /** The counters alone; the refused updates are written by {@link LanceNodeStats} as its version 4 block. */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(tracked);
            out.writeVLong(checks);
            out.writeVLong(moves);
            out.writeVLong(mappingUpdates);
            out.writeVLong(mappingUnchanged);
            out.writeVLong(rebuilds);
            out.writeVLong(failures);
            out.writeVLong(lastCheckMillis);
        }
    }

    /**
     * One node-local shallow clone directory on this node (an index
     * attached with {@code index.lance.index_placement = node_local}):
     * the bytes its files occupy under the node's data path (manifests
     * and search-index files only; data files stay in the source) and
     * the source manifest version the clone was created at.
     */
    public record LocalCloneStats(String index, long bytes, long sourceVersion) implements Writeable {

        public LocalCloneStats(StreamInput in) throws IOException {
            this(in.readString(), in.readVLong(), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeVLong(bytes);
            out.writeLong(sourceVersion);
        }
    }

    /**
     * The shard reader of one Lance-backed index this node hosts: the
     * live rows of the table version it was opened over, the live rows
     * it holds, the hidden nested child docs its leaves carry beyond
     * those rows (0 unless the table has {@code List<Struct>} columns),
     * whether the rows differ because the table is above the Lucene
     * document bound, the Lance index types present per column
     * (from one {@code describeIndices} on the reader's dataset; empty
     * when the read failed), and the column renames the index's mapping
     * records (a stale name marked {@code lance_dropped} whose Lance
     * field id lives on under a new name), so operators learn which
     * field names their clients must move to. {@code _stats} counts the
     * reader's docs.
     */
    public record IndexReaderStats(String index, long rows, long shardReaderRows, long nestedDocs, boolean luceneBoundExceeded, Map<
        String,
        List<String>> indexTypes, List<LanceMappingMeta.RenamedField> renamedFields) implements Writeable {

        public IndexReaderStats(
            String index,
            long rows,
            long shardReaderRows,
            long nestedDocs,
            boolean luceneBoundExceeded,
            Map<String, List<String>> indexTypes
        ) {
            this(index, rows, shardReaderRows, nestedDocs, luceneBoundExceeded, indexTypes, List.of());
        }

        public IndexReaderStats(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readBoolean(),
                in.readMap(StreamInput::readString, StreamInput::readStringList),
                in.readList(LanceMappingMeta.RenamedField::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeVLong(rows);
            out.writeVLong(shardReaderRows);
            out.writeVLong(nestedDocs);
            out.writeBoolean(luceneBoundExceeded);
            out.writeMap(indexTypes, StreamOutput::writeString, StreamOutput::writeStringCollection);
            out.writeList(renamedFields);
        }
    }

    public LanceNodeStats(
        boolean cacheEnabled,
        int snapshotCount,
        int retiredSnapshotCount,
        long datasetOpenCount,
        long snapshotBuildCount,
        long snapshotHitCount,
        long columnStoreBytes,
        long columnStoreLimitBytes,
        int columnStoreEntries,
        long columnStoreHits,
        long columnStoreLoads,
        long columnStoreEvictions,
        long columnStoreBudgetMisses,
        long heapFallbackBytes,
        long heapFallbackRejections,
        long nativeEstimatedBytes,
        long sessionBytes,
        long indexCacheCapacityBytes,
        int indexCacheShards,
        long indexCacheShardShareBytes,
        int ftsSubsetProbeLimit
    ) {
        this(
            cacheEnabled,
            snapshotCount,
            retiredSnapshotCount,
            datasetOpenCount,
            snapshotBuildCount,
            snapshotHitCount,
            columnStoreBytes,
            columnStoreLimitBytes,
            columnStoreEntries,
            columnStoreHits,
            columnStoreLoads,
            columnStoreEvictions,
            columnStoreBudgetMisses,
            heapFallbackBytes,
            heapFallbackRejections,
            nativeEstimatedBytes,
            sessionBytes,
            indexCacheCapacityBytes,
            indexCacheShards,
            indexCacheShardShareBytes,
            ftsSubsetProbeLimit,
            ScanAdmission.rejectionsByKind(),
            0L,
            "none",
            "none",
            0L,
            0L,
            "none",
            "none",
            List.of(),
            List.of(),
            List.of(),
            0,
            0L,
            Map.of(),
            Map.of(),
            0L
        );
    }

    public LanceNodeStats(
        boolean cacheEnabled,
        int snapshotCount,
        int retiredSnapshotCount,
        long datasetOpenCount,
        long snapshotBuildCount,
        long snapshotHitCount,
        long columnStoreBytes,
        long columnStoreLimitBytes,
        int columnStoreEntries,
        long columnStoreHits,
        long columnStoreLoads,
        long columnStoreEvictions,
        long columnStoreBudgetMisses,
        long heapFallbackBytes,
        long heapFallbackRejections,
        long nativeEstimatedBytes,
        long sessionBytes,
        long indexCacheCapacityBytes,
        int indexCacheShards,
        long indexCacheShardShareBytes,
        int ftsSubsetProbeLimit,
        Map<String, Long> admissionRejections,
        long admissionLastEstimateBytes,
        String admissionLastKind,
        String admissionLastSource,
        long admissionAvailableBytes,
        long admissionRetainedBytes,
        String admissionRetainedScope,
        String warmUpMode,
        List<LanceWarmUpStatus> warmUps,
        List<IndexReaderStats> indices,
        List<LocalCloneStats> localClones,
        int planStatisticsTables,
        long planStatisticsCollectMillisTotal,
        Map<String, Long> planRefinements,
        Map<String, Long> planExecuted,
        long planPrunedFragments
    ) {
        this(
            cacheEnabled,
            snapshotCount,
            retiredSnapshotCount,
            datasetOpenCount,
            snapshotBuildCount,
            snapshotHitCount,
            columnStoreBytes,
            columnStoreLimitBytes,
            columnStoreEntries,
            columnStoreHits,
            columnStoreLoads,
            columnStoreEvictions,
            columnStoreBudgetMisses,
            heapFallbackBytes,
            heapFallbackRejections,
            nativeEstimatedBytes,
            sessionBytes,
            indexCacheCapacityBytes,
            indexCacheShards,
            indexCacheShardShareBytes,
            ftsSubsetProbeLimit,
            admissionRejections,
            admissionLastEstimateBytes,
            admissionLastKind,
            admissionLastSource,
            admissionAvailableBytes,
            admissionRetainedBytes,
            admissionRetainedScope,
            warmUpMode,
            warmUps,
            indices,
            localClones,
            planStatisticsTables,
            planStatisticsCollectMillisTotal,
            0,
            0L,
            planRefinements,
            planExecuted,
            planPrunedFragments,
            FreshnessStats.NONE
        );
    }

    public LanceNodeStats(
        boolean cacheEnabled,
        int snapshotCount,
        int retiredSnapshotCount,
        long datasetOpenCount,
        long snapshotBuildCount,
        long snapshotHitCount,
        long columnStoreBytes,
        long columnStoreLimitBytes,
        int columnStoreEntries,
        long columnStoreHits,
        long columnStoreLoads,
        long columnStoreEvictions,
        long columnStoreBudgetMisses,
        long heapFallbackBytes,
        long heapFallbackRejections,
        long nativeEstimatedBytes,
        long sessionBytes,
        long indexCacheCapacityBytes,
        int indexCacheShards,
        long indexCacheShardShareBytes,
        int ftsSubsetProbeLimit,
        Map<String, Long> admissionRejections,
        long admissionLastEstimateBytes,
        String admissionLastKind,
        String admissionLastSource,
        long admissionAvailableBytes,
        long admissionRetainedBytes,
        String admissionRetainedScope,
        String warmUpMode,
        List<LanceWarmUpStatus> warmUps,
        List<IndexReaderStats> indices,
        List<LocalCloneStats> localClones,
        int planStatisticsTables,
        long planStatisticsCollectMillisTotal,
        int planStatisticsPending,
        long planStatisticsPlannedWithout,
        Map<String, Long> planRefinements,
        Map<String, Long> planExecuted,
        long planPrunedFragments,
        FreshnessStats freshness
    ) {
        this(
            cacheEnabled,
            snapshotCount,
            retiredSnapshotCount,
            datasetOpenCount,
            snapshotBuildCount,
            snapshotHitCount,
            columnStoreBytes,
            columnStoreLimitBytes,
            columnStoreEntries,
            columnStoreHits,
            columnStoreLoads,
            columnStoreEvictions,
            columnStoreBudgetMisses,
            heapFallbackBytes,
            heapFallbackRejections,
            nativeEstimatedBytes,
            sessionBytes,
            indexCacheCapacityBytes,
            indexCacheShards,
            indexCacheShardShareBytes,
            ftsSubsetProbeLimit,
            admissionRejections,
            admissionLastEstimateBytes,
            admissionLastKind,
            admissionLastSource,
            admissionAvailableBytes,
            admissionRetainedBytes,
            admissionRetainedScope,
            warmUpMode,
            warmUps,
            indices,
            localClones,
            planStatisticsTables,
            planStatisticsCollectMillisTotal,
            planStatisticsPending,
            planStatisticsPlannedWithout,
            planRefinements,
            planExecuted,
            planPrunedFragments,
            freshness,
            FetchStats.NONE
        );
    }

    public LanceNodeStats(
        boolean cacheEnabled,
        int snapshotCount,
        int retiredSnapshotCount,
        long datasetOpenCount,
        long snapshotBuildCount,
        long snapshotHitCount,
        long columnStoreBytes,
        long columnStoreLimitBytes,
        int columnStoreEntries,
        long columnStoreHits,
        long columnStoreLoads,
        long columnStoreEvictions,
        long columnStoreBudgetMisses,
        long heapFallbackBytes,
        long heapFallbackRejections,
        long nativeEstimatedBytes,
        long sessionBytes,
        long indexCacheCapacityBytes,
        int indexCacheShards,
        long indexCacheShardShareBytes,
        int ftsSubsetProbeLimit,
        Map<String, Long> admissionRejections,
        long admissionLastEstimateBytes,
        String admissionLastKind,
        String admissionLastSource,
        long admissionAvailableBytes,
        long admissionRetainedBytes,
        String admissionRetainedScope,
        String warmUpMode,
        List<LanceWarmUpStatus> warmUps,
        List<IndexReaderStats> indices,
        List<LocalCloneStats> localClones,
        int planStatisticsTables,
        long planStatisticsCollectMillisTotal,
        int planStatisticsPending,
        long planStatisticsPlannedWithout,
        Map<String, Long> planRefinements,
        Map<String, Long> planExecuted,
        long planPrunedFragments,
        FreshnessStats freshness,
        FetchStats fetch
    ) {
        this.cacheEnabled = cacheEnabled;
        this.snapshotCount = snapshotCount;
        this.retiredSnapshotCount = retiredSnapshotCount;
        this.datasetOpenCount = datasetOpenCount;
        this.snapshotBuildCount = snapshotBuildCount;
        this.snapshotHitCount = snapshotHitCount;
        this.columnStoreBytes = columnStoreBytes;
        this.columnStoreLimitBytes = columnStoreLimitBytes;
        this.columnStoreEntries = columnStoreEntries;
        this.columnStoreHits = columnStoreHits;
        this.columnStoreLoads = columnStoreLoads;
        this.columnStoreEvictions = columnStoreEvictions;
        this.columnStoreBudgetMisses = columnStoreBudgetMisses;
        this.heapFallbackBytes = heapFallbackBytes;
        this.heapFallbackRejections = heapFallbackRejections;
        this.nativeEstimatedBytes = nativeEstimatedBytes;
        this.sessionBytes = sessionBytes;
        this.indexCacheCapacityBytes = indexCacheCapacityBytes;
        this.indexCacheShards = indexCacheShards;
        this.indexCacheShardShareBytes = indexCacheShardShareBytes;
        this.ftsSubsetProbeLimit = ftsSubsetProbeLimit;
        this.admissionRejections = Collections.unmodifiableMap(new LinkedHashMap<>(admissionRejections));
        this.admissionLastEstimateBytes = admissionLastEstimateBytes;
        this.admissionLastKind = admissionLastKind;
        this.admissionLastSource = admissionLastSource;
        this.admissionAvailableBytes = admissionAvailableBytes;
        this.admissionRetainedBytes = admissionRetainedBytes;
        this.admissionRetainedScope = admissionRetainedScope == null ? "none" : admissionRetainedScope;
        this.warmUpMode = warmUpMode;
        this.warmUps = List.copyOf(warmUps);
        this.indices = List.copyOf(indices);
        this.localClones = List.copyOf(localClones);
        this.planStatisticsTables = planStatisticsTables;
        this.planStatisticsCollectMillisTotal = planStatisticsCollectMillisTotal;
        this.planStatisticsPending = planStatisticsPending;
        this.planStatisticsPlannedWithout = planStatisticsPlannedWithout;
        this.planRefinements = Collections.unmodifiableMap(new LinkedHashMap<>(planRefinements));
        this.planExecuted = Collections.unmodifiableMap(new LinkedHashMap<>(planExecuted));
        this.planPrunedFragments = planPrunedFragments;
        this.freshness = freshness == null ? FreshnessStats.NONE : freshness;
        this.requestCache = RequestCacheStats.NONE;
        this.fetch = fetch == null ? FetchStats.NONE : fetch;
        this.fetchCache = FetchCacheStats.NONE;
    }

    /** A copy carrying {@code requestCache} as the result cache figures ({@link RequestCacheStats#NONE} for null). */
    public LanceNodeStats withRequestCache(RequestCacheStats requestCache) {
        return new LanceNodeStats(this, requestCache == null ? RequestCacheStats.NONE : requestCache, this.fetchCache);
    }

    /** A copy carrying {@code fetchCache} as the fetch cache figures ({@link FetchCacheStats#NONE} for null). */
    public LanceNodeStats withFetchCache(FetchCacheStats fetchCache) {
        return new LanceNodeStats(this, this.requestCache, fetchCache == null ? FetchCacheStats.NONE : fetchCache);
    }

    private LanceNodeStats(LanceNodeStats copy, RequestCacheStats requestCache, FetchCacheStats fetchCache) {
        this.cacheEnabled = copy.cacheEnabled;
        this.snapshotCount = copy.snapshotCount;
        this.retiredSnapshotCount = copy.retiredSnapshotCount;
        this.datasetOpenCount = copy.datasetOpenCount;
        this.snapshotBuildCount = copy.snapshotBuildCount;
        this.snapshotHitCount = copy.snapshotHitCount;
        this.columnStoreBytes = copy.columnStoreBytes;
        this.columnStoreLimitBytes = copy.columnStoreLimitBytes;
        this.columnStoreEntries = copy.columnStoreEntries;
        this.columnStoreHits = copy.columnStoreHits;
        this.columnStoreLoads = copy.columnStoreLoads;
        this.columnStoreEvictions = copy.columnStoreEvictions;
        this.columnStoreBudgetMisses = copy.columnStoreBudgetMisses;
        this.heapFallbackBytes = copy.heapFallbackBytes;
        this.heapFallbackRejections = copy.heapFallbackRejections;
        this.nativeEstimatedBytes = copy.nativeEstimatedBytes;
        this.sessionBytes = copy.sessionBytes;
        this.indexCacheCapacityBytes = copy.indexCacheCapacityBytes;
        this.indexCacheShards = copy.indexCacheShards;
        this.indexCacheShardShareBytes = copy.indexCacheShardShareBytes;
        this.ftsSubsetProbeLimit = copy.ftsSubsetProbeLimit;
        this.admissionRejections = copy.admissionRejections;
        this.admissionLastEstimateBytes = copy.admissionLastEstimateBytes;
        this.admissionLastKind = copy.admissionLastKind;
        this.admissionLastSource = copy.admissionLastSource;
        this.admissionAvailableBytes = copy.admissionAvailableBytes;
        this.admissionRetainedBytes = copy.admissionRetainedBytes;
        this.admissionRetainedScope = copy.admissionRetainedScope;
        this.warmUpMode = copy.warmUpMode;
        this.warmUps = copy.warmUps;
        this.indices = copy.indices;
        this.localClones = copy.localClones;
        this.planStatisticsTables = copy.planStatisticsTables;
        this.planStatisticsCollectMillisTotal = copy.planStatisticsCollectMillisTotal;
        this.planStatisticsPending = copy.planStatisticsPending;
        this.planStatisticsPlannedWithout = copy.planStatisticsPlannedWithout;
        this.planRefinements = copy.planRefinements;
        this.planExecuted = copy.planExecuted;
        this.planPrunedFragments = copy.planPrunedFragments;
        this.freshness = copy.freshness;
        this.fetch = copy.fetch;
        this.requestCache = requestCache;
        this.fetchCache = fetchCache;
    }

    /**
     * {@code counts} in the gate's kind order (the wire does not keep
     * the order of a map), any key the gate does not name last.
     */
    private static Map<String, Long> inKindOrder(Map<String, Long> counts) {
        Map<String, Long> ordered = new LinkedHashMap<>();
        for (ScanAdmission.Kind kind : ScanAdmission.Kind.values()) {
            Long count = counts.get(kind.key());
            if (count != null) {
                ordered.put(kind.key(), count);
            }
        }
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            ordered.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(ordered);
    }

    public LanceNodeStats(StreamInput in) throws IOException {
        this(in, WIRE_VERSION);
    }

    /**
     * Reads the stats as a coordinator whose plugin is at wire version
     * {@code asVersion} would: the blocks of later versions are stepped
     * over as {@link WireVersion.Reader} describes. The transport reads
     * with {@link #WIRE_VERSION}; the mixed version tests read with the
     * versions before it.
     */
    static LanceNodeStats read(StreamInput in, int asVersion) throws IOException {
        return new LanceNodeStats(in, asVersion);
    }

    private LanceNodeStats(StreamInput in, int asVersion) throws IOException {
        WireVersion.Reader reader = WireVersion.read(in, "LanceNodeStats", asVersion);
        this.cacheEnabled = in.readBoolean();
        this.snapshotCount = in.readVInt();
        this.retiredSnapshotCount = in.readVInt();
        this.datasetOpenCount = in.readVLong();
        this.snapshotBuildCount = in.readVLong();
        this.snapshotHitCount = in.readVLong();
        this.columnStoreBytes = in.readVLong();
        this.columnStoreLimitBytes = in.readVLong();
        this.columnStoreEntries = in.readVInt();
        this.columnStoreHits = in.readVLong();
        this.columnStoreLoads = in.readVLong();
        this.columnStoreEvictions = in.readVLong();
        this.columnStoreBudgetMisses = in.readVLong();
        this.heapFallbackBytes = in.readVLong();
        this.heapFallbackRejections = in.readVLong();
        this.nativeEstimatedBytes = in.readLong();
        this.sessionBytes = in.readLong();
        this.indexCacheCapacityBytes = in.readVLong();
        this.indexCacheShards = in.readVInt();
        this.indexCacheShardShareBytes = in.readVLong();
        this.ftsSubsetProbeLimit = in.readVInt();
        this.admissionRejections = inKindOrder(in.readMap(StreamInput::readString, StreamInput::readVLong));
        this.admissionLastEstimateBytes = in.readVLong();
        this.admissionLastKind = in.readString();
        this.admissionAvailableBytes = in.readVLong();
        this.admissionRetainedBytes = in.readVLong();
        this.warmUpMode = in.readString();
        int warmUpCount = in.readVInt();
        List<LanceWarmUpStatus> read = new ArrayList<>(warmUpCount);
        for (int i = 0; i < warmUpCount; i++) {
            read.add(new LanceWarmUpStatus(in));
        }
        this.warmUps = List.copyOf(read);
        this.indices = in.readList(IndexReaderStats::new);
        this.localClones = in.readList(LocalCloneStats::new);
        this.planStatisticsTables = in.readVInt();
        this.planStatisticsCollectMillisTotal = in.readVLong();
        this.planRefinements = Collections.unmodifiableMap(in.readOrderedMap(StreamInput::readString, StreamInput::readVLong));
        this.planExecuted = Collections.unmodifiableMap(in.readOrderedMap(StreamInput::readString, StreamInput::readVLong));
        FreshnessStats counters = new FreshnessStats(in);
        this.planPrunedFragments = reader.block(2, StreamInput::readVLong, 0L);
        this.admissionLastSource = reader.block(3, StreamInput::readString, "none");
        this.freshness = counters.withMappingErrors(
            reader.block(4, block -> block.readOrderedMap(StreamInput::readString, StreamInput::readString), Map.of())
        );
        StatisticsProgress progress = reader.block(5, StatisticsProgress::new, StatisticsProgress.NONE);
        this.planStatisticsPending = progress.pending();
        this.planStatisticsPlannedWithout = progress.plannedWithout();
        this.admissionRetainedScope = reader.block(6, StreamInput::readString, "none");
        this.requestCache = reader.block(7, RequestCacheStats::new, RequestCacheStats.NONE);
        this.fetch = reader.block(8, FetchStats::new, FetchStats.NONE);
        this.fetchCache = reader.block(9, FetchCacheStats::new, FetchCacheStats.NONE);
        reader.finish();
    }

    /**
     * The version 5 block: the statistics collections queued or running
     * and the plans made without statistics. A record so the block is
     * read from its own stream in one step.
     */
    private record StatisticsProgress(int pending, long plannedWithout) implements Writeable {
        static final StatisticsProgress NONE = new StatisticsProgress(0, 0L);

        StatisticsProgress(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pending);
            out.writeVLong(plannedWithout);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeBoolean(cacheEnabled);
        out.writeVInt(snapshotCount);
        out.writeVInt(retiredSnapshotCount);
        out.writeVLong(datasetOpenCount);
        out.writeVLong(snapshotBuildCount);
        out.writeVLong(snapshotHitCount);
        out.writeVLong(columnStoreBytes);
        out.writeVLong(columnStoreLimitBytes);
        out.writeVInt(columnStoreEntries);
        out.writeVLong(columnStoreHits);
        out.writeVLong(columnStoreLoads);
        out.writeVLong(columnStoreEvictions);
        out.writeVLong(columnStoreBudgetMisses);
        out.writeVLong(heapFallbackBytes);
        out.writeVLong(heapFallbackRejections);
        out.writeLong(nativeEstimatedBytes);
        out.writeLong(sessionBytes);
        out.writeVLong(indexCacheCapacityBytes);
        out.writeVInt(indexCacheShards);
        out.writeVLong(indexCacheShardShareBytes);
        out.writeVInt(ftsSubsetProbeLimit);
        out.writeMap(admissionRejections, StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeVLong(admissionLastEstimateBytes);
        out.writeString(admissionLastKind);
        out.writeVLong(admissionAvailableBytes);
        out.writeVLong(admissionRetainedBytes);
        out.writeString(warmUpMode);
        out.writeVInt(warmUps.size());
        for (LanceWarmUpStatus warmUp : warmUps) {
            warmUp.writeTo(out);
        }
        out.writeList(indices);
        out.writeList(localClones);
        out.writeVInt(planStatisticsTables);
        out.writeVLong(planStatisticsCollectMillisTotal);
        out.writeMap(planRefinements, StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeMap(planExecuted, StreamOutput::writeString, StreamOutput::writeVLong);
        freshness.writeTo(out);
        // A coordinator that ignores the counter shows the stats
        // without it, so the block is never critical.
        WireVersion.writeBlock(out, false, o -> o.writeVLong(planPrunedFragments));
        // A coordinator that ignores the source shows the decision
        // without it, so the block is never critical.
        WireVersion.writeBlock(out, false, o -> o.writeString(admissionLastSource));
        // Likewise for the refused mapping updates: an older coordinator
        // shows the freshness counters without them.
        WireVersion.writeBlock(
            out,
            false,
            o -> o.writeMap(freshness.mappingErrors(), StreamOutput::writeString, StreamOutput::writeString)
        );
        // Likewise for the collection progress counters.
        WireVersion.writeBlock(out, false, new StatisticsProgress(planStatisticsPending, planStatisticsPlannedWithout));
        // Likewise for the retained pool's identity: an older coordinator
        // shows the retained bytes without the scans they belong to.
        WireVersion.writeBlock(out, false, o -> o.writeString(admissionRetainedScope));
        // Likewise for the result cache: an older coordinator shows the
        // node without it.
        WireVersion.writeBlock(out, false, requestCache);
        // Likewise for the fetch take counters: an older coordinator
        // shows the stats without them.
        WireVersion.writeBlock(out, false, fetch);
        // Likewise for the fetch cache: an older coordinator shows the
        // node without it.
        WireVersion.writeBlock(out, false, fetchCache);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("snapshots");
        builder.field("enabled", cacheEnabled);
        builder.field("count", snapshotCount);
        builder.field("retired", retiredSnapshotCount);
        builder.field("dataset_open_count", datasetOpenCount);
        builder.field("snapshot_build_count", snapshotBuildCount);
        builder.field("snapshot_hit_count", snapshotHitCount);
        builder.endObject();

        builder.startObject("column_store");
        builder.field("bytes", columnStoreBytes);
        builder.field("limit_bytes", columnStoreLimitBytes);
        builder.field("entries", columnStoreEntries);
        builder.field("hits", columnStoreHits);
        builder.field("loads", columnStoreLoads);
        builder.field("evictions", columnStoreEvictions);
        builder.field("budget_misses", columnStoreBudgetMisses);
        builder.field("heap_fallback_bytes", heapFallbackBytes);
        builder.field("heap_fallback_rejections", heapFallbackRejections);
        builder.endObject();

        builder.startObject("request_cache");
        builder.field("enabled", requestCache.enabled());
        builder.field("size_bytes", requestCache.sizeBytes());
        builder.field("limit_bytes", requestCache.limitBytes());
        builder.field("entries", requestCache.entries());
        builder.field("hits", requestCache.hits());
        builder.field("misses", requestCache.misses());
        builder.field("evictions", requestCache.evictions());
        builder.field("invalidations", requestCache.invalidations());
        builder.field("skipped", requestCache.skipped());
        builder.endObject();

        builder.startObject("native_memory");
        builder.field("estimated_bytes", nativeEstimatedBytes);
        builder.field("session_bytes", sessionBytes);
        builder.field("column_store_bytes", columnStoreBytes);
        builder.field("index_cache_capacity", indexCacheCapacityBytes);
        builder.field("index_cache_shards", indexCacheShards);
        builder.field("index_cache_shard_share", indexCacheShardShareBytes);
        builder.endObject();

        builder.startObject("fts");
        builder.field("subset_probe_limit", ftsSubsetProbeLimit);
        builder.endObject();

        builder.startObject("admission");
        builder.field("enabled", ScanAdmission.enabled());
        builder.field("headroom_bytes", ScanAdmission.headroomBytes());
        builder.field("available_bytes", admissionAvailableBytes);
        builder.field("retained_bytes", admissionRetainedBytes);
        builder.field("retained_scope", admissionRetainedScope);
        builder.field("last_estimate_bytes", admissionLastEstimateBytes);
        builder.field("last_kind", admissionLastKind);
        builder.field("last_source", admissionLastSource);
        builder.startObject("rejections");
        for (Map.Entry<String, Long> rejection : admissionRejections.entrySet()) {
            builder.field(rejection.getKey(), rejection.getValue());
        }
        builder.endObject();
        builder.endObject();

        builder.startObject("warm_up");
        builder.field("mode", warmUpMode);
        builder.startArray("tables");
        for (LanceWarmUpStatus warmUp : warmUps) {
            warmUp.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();

        builder.startObject("plan");
        builder.startObject("statistics");
        builder.field("tables", planStatisticsTables);
        builder.field("collect_millis_total", planStatisticsCollectMillisTotal);
        builder.field("pending", planStatisticsPending);
        builder.field("planned_without", planStatisticsPlannedWithout);
        builder.endObject();
        builder.startObject("refinements");
        for (Map.Entry<String, Long> refinement : planRefinements.entrySet()) {
            builder.field(refinement.getKey(), refinement.getValue());
        }
        builder.endObject();
        builder.startObject("executed");
        for (Map.Entry<String, Long> executed : planExecuted.entrySet()) {
            builder.field(executed.getKey(), executed.getValue());
        }
        builder.endObject();
        builder.startObject("pruned");
        builder.field("fragments", planPrunedFragments);
        builder.endObject();
        builder.endObject();

        builder.startObject("freshness");
        builder.field("tracked", freshness.tracked());
        builder.field("checks", freshness.checks());
        builder.field("moves", freshness.moves());
        builder.field("mapping_updates", freshness.mappingUpdates());
        builder.field("mapping_unchanged", freshness.mappingUnchanged());
        builder.field("rebuilds", freshness.rebuilds());
        builder.field("failures", freshness.failures());
        builder.field("last_check_millis", freshness.lastCheckMillis());
        builder.startObject("mapping_errors");
        for (Map.Entry<String, String> refused : freshness.mappingErrors().entrySet()) {
            builder.field(refused.getKey(), refused.getValue());
        }
        builder.endObject();
        builder.endObject();

        builder.startObject("fetch");
        builder.field("take_count", fetch.takeCount());
        builder.field("take_rows", fetch.takeRows());
        builder.field("take_columns", fetch.takeColumns());
        builder.field("take_millis_total", fetch.takeMillisTotal());
        builder.field("take_max_millis", fetch.takeMaxMillis());
        builder.field("stored_fields_takes", fetch.storedFieldsTakes());
        builder.field("column_takes", fetch.columnTakes());
        builder.endObject();

        builder.startObject("fetch_cache");
        builder.field("enabled", fetchCache.enabled());
        builder.field("size_bytes", fetchCache.sizeBytes());
        builder.field("limit_bytes", fetchCache.limitBytes());
        builder.field("entries", fetchCache.entries());
        builder.field("hits", fetchCache.hits());
        builder.field("misses", fetchCache.misses());
        builder.field("evictions", fetchCache.evictions());
        builder.field("invalidations", fetchCache.invalidations());
        builder.field("skipped", fetchCache.skipped());
        builder.field("rows_served", fetchCache.rowsServed());
        builder.endObject();

        builder.startObject("indices");
        for (IndexReaderStats index : indices) {
            builder.startObject(index.index());
            builder.field("rows", index.rows());
            builder.field("shard_reader_rows", index.shardReaderRows());
            builder.field("nested_docs", index.nestedDocs());
            builder.field("lucene_bound_exceeded", index.luceneBoundExceeded());
            builder.startObject("index_types");
            for (Map.Entry<String, List<String>> column : index.indexTypes().entrySet()) {
                builder.field(column.getKey(), column.getValue());
            }
            builder.endObject();
            if (!index.renamedFields().isEmpty()) {
                builder.startArray("renamed_fields");
                for (LanceMappingMeta.RenamedField renamed : index.renamedFields()) {
                    builder.startObject();
                    builder.field("from", renamed.from());
                    builder.field("to", renamed.to());
                    builder.field("lance_field_id", renamed.fieldId());
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("local_clones");
        for (LocalCloneStats clone : localClones) {
            builder.startObject(clone.index());
            builder.field("local_clone_bytes", clone.bytes());
            builder.field("source_version", clone.sourceVersion());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public List<LocalCloneStats> localClones() {
        return localClones;
    }

    /** Planner table statistics entries this node holds, one per (table URI, manifest version). */
    public int planStatisticsTables() {
        return planStatisticsTables;
    }

    /** Milliseconds this node has spent collecting planner table statistics, summed over every collection. */
    public long planStatisticsCollectMillisTotal() {
        return planStatisticsCollectMillisTotal;
    }

    /** Planner table statistics collections queued or running on this node. */
    public int planStatisticsPending() {
        return planStatisticsPending;
    }

    /** Plans this node made without table statistics because their version was not collected yet. */
    public long planStatisticsPlannedWithout() {
        return planStatisticsPlannedWithout;
    }

    /** Plan refinements this node's executor applied since it started, keyed by reason; every reason present. */
    public Map<String, Long> planRefinements() {
        return planRefinements;
    }

    /** Fragment requests this node's executor answered, keyed by {@code pushed_scan} and {@code lucene}. */
    public Map<String, Long> planExecuted() {
        return planExecuted;
    }

    /** Fragments this node's executor skipped under zone map pruning since the node started. */
    public long planPrunedFragments() {
        return planPrunedFragments;
    }

    /** This node's freshness checks of the Lance backed shards it holds. */
    public FreshnessStats freshness() {
        return freshness;
    }

    /** The coordinator result cache of this node; {@link RequestCacheStats#NONE} from a node that does not report it. */
    public RequestCacheStats requestCache() {
        return requestCache;
    }

    /** The take scans this node's fragment executors issued since it started. */
    public FetchStats fetch() {
        return fetch;
    }

    /** The fetch cache of this node; {@link FetchCacheStats#NONE} from a node that does not report it. */
    public FetchCacheStats fetchCache() {
        return fetchCache;
    }

    public boolean cacheEnabled() {
        return cacheEnabled;
    }

    public int snapshotCount() {
        return snapshotCount;
    }

    public int retiredSnapshotCount() {
        return retiredSnapshotCount;
    }

    public long datasetOpenCount() {
        return datasetOpenCount;
    }

    public long snapshotBuildCount() {
        return snapshotBuildCount;
    }

    public long snapshotHitCount() {
        return snapshotHitCount;
    }

    public long columnStoreBytes() {
        return columnStoreBytes;
    }

    public long columnStoreLimitBytes() {
        return columnStoreLimitBytes;
    }

    public int columnStoreEntries() {
        return columnStoreEntries;
    }

    public long columnStoreHits() {
        return columnStoreHits;
    }

    public long columnStoreLoads() {
        return columnStoreLoads;
    }

    public long columnStoreEvictions() {
        return columnStoreEvictions;
    }

    public long columnStoreBudgetMisses() {
        return columnStoreBudgetMisses;
    }

    /** Heap bytes open readers on the node currently have charged to the request breaker for columns the store could not hold. */
    public long heapFallbackBytes() {
        return heapFallbackBytes;
    }

    /** Heap column loads the request breaker refused (each one ended a request with HTTP 429). */
    public long heapFallbackRejections() {
        return heapFallbackRejections;
    }

    public long nativeEstimatedBytes() {
        return nativeEstimatedBytes;
    }

    public long sessionBytes() {
        return sessionBytes;
    }

    public long indexCacheCapacityBytes() {
        return indexCacheCapacityBytes;
    }

    public int indexCacheShards() {
        return indexCacheShards;
    }

    public long indexCacheShardShareBytes() {
        return indexCacheShardShareBytes;
    }

    public int ftsSubsetProbeLimit() {
        return ftsSubsetProbeLimit;
    }

    /** Scans this node's admission gate refused since it started, per kind; every kind present. */
    public Map<String, Long> admissionRejections() {
        return admissionRejections;
    }

    /** Refusals over every kind. */
    public long admissionRejectionsTotal() {
        long total = 0L;
        for (long count : admissionRejections.values()) {
            total += count;
        }
        return total;
    }

    /** Estimate of the node's last admission decision, admitted or not. */
    public long admissionLastEstimateBytes() {
        return admissionLastEstimateBytes;
    }

    /** Kind of the node's last admission decision, {@code none} before the first. */
    public String admissionLastKind() {
        return admissionLastKind;
    }

    /**
     * Source of the node's last admission decision: {@code request} for
     * a search request's gated path, {@code warm_up} for the metadata
     * warm up's full text probe, {@code none} before the first and from
     * a node whose plugin version does not report it.
     */
    public String admissionLastSource() {
        return admissionLastSource;
    }

    /**
     * The node's available physical memory when the stats were collected
     * ({@code MemAvailable} on Linux, the free physical memory elsewhere),
     * before the headroom is subtracted.
     */
    public long admissionAvailableBytes() {
        return admissionAvailableBytes;
    }

    /**
     * Memory earlier admitted scans left in the process that the node's
     * next admission decision on a scan of {@link #admissionRetainedScope()}
     * adds to the available memory (zero while a gated request is in
     * flight or a gated scan runs; a scan of any other identity is
     * credited nothing).
     */
    public long admissionRetainedBytes() {
        return admissionRetainedBytes;
    }

    /**
     * Identity of the scans that filled the retained pool, as
     * {@code kind:table:columns}: the only scans
     * {@link #admissionRetainedBytes()} is credited to. {@code none}
     * before the first admission and from a node whose plugin version
     * does not report it.
     */
    public String admissionRetainedScope() {
        return admissionRetainedScope;
    }

    /** Value of {@code lance.attach.warm_indexes} on the node. */
    public String warmUpMode() {
        return warmUpMode;
    }

    /** Warm-ups the node has seen since it started, one per Lance-backed index. */
    public List<LanceWarmUpStatus> warmUps() {
        return warmUps;
    }

    /** The shard readers of the Lance-backed indexes this node hosts, in index name order. */
    public List<IndexReaderStats> indices() {
        return indices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceNodeStats other)) {
            return false;
        }
        return cacheEnabled == other.cacheEnabled
            && snapshotCount == other.snapshotCount
            && retiredSnapshotCount == other.retiredSnapshotCount
            && datasetOpenCount == other.datasetOpenCount
            && snapshotBuildCount == other.snapshotBuildCount
            && snapshotHitCount == other.snapshotHitCount
            && columnStoreBytes == other.columnStoreBytes
            && columnStoreLimitBytes == other.columnStoreLimitBytes
            && columnStoreEntries == other.columnStoreEntries
            && columnStoreHits == other.columnStoreHits
            && columnStoreLoads == other.columnStoreLoads
            && columnStoreEvictions == other.columnStoreEvictions
            && columnStoreBudgetMisses == other.columnStoreBudgetMisses
            && heapFallbackBytes == other.heapFallbackBytes
            && heapFallbackRejections == other.heapFallbackRejections
            && nativeEstimatedBytes == other.nativeEstimatedBytes
            && sessionBytes == other.sessionBytes
            && indexCacheCapacityBytes == other.indexCacheCapacityBytes
            && indexCacheShards == other.indexCacheShards
            && indexCacheShardShareBytes == other.indexCacheShardShareBytes
            && ftsSubsetProbeLimit == other.ftsSubsetProbeLimit
            && admissionRejections.equals(other.admissionRejections)
            && admissionLastEstimateBytes == other.admissionLastEstimateBytes
            && admissionLastKind.equals(other.admissionLastKind)
            && admissionLastSource.equals(other.admissionLastSource)
            && admissionAvailableBytes == other.admissionAvailableBytes
            && admissionRetainedBytes == other.admissionRetainedBytes
            && admissionRetainedScope.equals(other.admissionRetainedScope)
            && warmUpMode.equals(other.warmUpMode)
            && warmUps.equals(other.warmUps)
            && indices.equals(other.indices)
            && localClones.equals(other.localClones)
            && planStatisticsTables == other.planStatisticsTables
            && planStatisticsCollectMillisTotal == other.planStatisticsCollectMillisTotal
            && planStatisticsPending == other.planStatisticsPending
            && planStatisticsPlannedWithout == other.planStatisticsPlannedWithout
            && planRefinements.equals(other.planRefinements)
            && planExecuted.equals(other.planExecuted)
            && planPrunedFragments == other.planPrunedFragments
            && freshness.equals(other.freshness)
            && requestCache.equals(other.requestCache)
            && fetch.equals(other.fetch)
            && fetchCache.equals(other.fetchCache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            cacheEnabled,
            snapshotCount,
            retiredSnapshotCount,
            datasetOpenCount,
            snapshotBuildCount,
            snapshotHitCount,
            columnStoreBytes,
            columnStoreLimitBytes,
            columnStoreEntries,
            columnStoreHits,
            columnStoreLoads,
            columnStoreEvictions,
            columnStoreBudgetMisses,
            heapFallbackBytes,
            heapFallbackRejections,
            nativeEstimatedBytes,
            sessionBytes,
            indexCacheCapacityBytes,
            indexCacheShards,
            indexCacheShardShareBytes,
            ftsSubsetProbeLimit,
            admissionRejections,
            admissionLastEstimateBytes,
            admissionLastKind,
            admissionLastSource,
            admissionAvailableBytes,
            admissionRetainedBytes,
            admissionRetainedScope,
            warmUpMode,
            warmUps,
            indices,
            localClones,
            planStatisticsTables,
            planStatisticsCollectMillisTotal,
            planStatisticsPending,
            planStatisticsPlannedWithout,
            planRefinements,
            planExecuted,
            planPrunedFragments,
            freshness,
            requestCache,
            fetch,
            fetchCache
        );
    }
}
