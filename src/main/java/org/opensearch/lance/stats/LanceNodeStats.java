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
 * {@code native_memory}, {@code fts}, {@code admission}, {@code warm_up},
 * {@code plan} and {@code indices} objects of one node in
 * {@code GET /_lance/stats}. {@code admission} carries the admission
 * gate's settings in force, its rejections per kind, the estimate and
 * kind of its last decision, the node's available memory and the memory
 * earlier admitted scans retained;
 * {@code warm_up} carries the mode in force and one
 * {@link LanceWarmUpStatus} per Lance-backed index the node has seen
 * since it started; {@code plan.statistics} the planner's table
 * statistics cache (entries held and milliseconds spent collecting);
 * {@code plan.refinements} how often the fragment executor moved a
 * pushed operation of a shipped plan to the Lucene side, per reason;
 * {@code plan.executed} how many fragment requests the Lance scan and
 * Lucene each answered;
 * {@code freshness} the node's checks of the Lance backed shards it
 * holds against their tables ({@link FreshnessStats});
 * {@code indices} the shard reader of every Lance-backed shard the node
 * hosts.
 */
public final class LanceNodeStats implements Writeable, ToXContentFragment {

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
    private final long admissionAvailableBytes;
    private final long admissionRetainedBytes;

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

    private final int planStatisticsTables;
    private final long planStatisticsCollectMillisTotal;
    private final FreshnessStats freshness;

    /**
     * This node's freshness checks of the Lance backed shards it holds:
     * how many indexes it tracks, how many checks ran, how many found
     * the table (or its tag) at another version than the shard serves,
     * how many mapping updates were sent and how many were skipped
     * because the derived mapping equalled the current one, how many
     * indexes were rebuilt for a keyword to lance_text flip, how many
     * checks failed, and when the last check ran (epoch millis, 0 when
     * none ran).
     */
    public record FreshnessStats(int tracked, long checks, long moves, long mappingUpdates, long mappingUnchanged, long rebuilds,
        long failures, long lastCheckMillis) implements Writeable {

        public static final FreshnessStats NONE = new FreshnessStats(0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

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
            0L,
            0L,
            "none",
            List.of(),
            List.of(),
            List.of(),
            0,
            0L,
            Map.of(),
            Map.of()
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
        long admissionAvailableBytes,
        long admissionRetainedBytes,
        String warmUpMode,
        List<LanceWarmUpStatus> warmUps,
        List<IndexReaderStats> indices,
        List<LocalCloneStats> localClones,
        int planStatisticsTables,
        long planStatisticsCollectMillisTotal,
        Map<String, Long> planRefinements,
        Map<String, Long> planExecuted
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
            ftsAdmissionRejections,
            ftsAdmissionLastEstimateBytes,
            ftsAdmissionAvailableBytes,
            ftsAdmissionRetainedBytes,
            warmUpMode,
            warmUps,
            indices,
            localClones,
            planStatisticsTables,
            planStatisticsCollectMillisTotal,
            planRefinements,
            planExecuted,
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
        long ftsAdmissionRejections,
        long ftsAdmissionLastEstimateBytes,
        long ftsAdmissionAvailableBytes,
        long ftsAdmissionRetainedBytes,
        String warmUpMode,
        List<LanceWarmUpStatus> warmUps,
        List<IndexReaderStats> indices,
        List<LocalCloneStats> localClones,
        int planStatisticsTables,
        long planStatisticsCollectMillisTotal,
        Map<String, Long> planRefinements,
        Map<String, Long> planExecuted,
        FreshnessStats freshness
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
        this.admissionAvailableBytes = admissionAvailableBytes;
        this.admissionRetainedBytes = admissionRetainedBytes;
        this.warmUpMode = warmUpMode;
        this.warmUps = List.copyOf(warmUps);
        this.indices = List.copyOf(indices);
        this.localClones = List.copyOf(localClones);
        this.planStatisticsTables = planStatisticsTables;
        this.planStatisticsCollectMillisTotal = planStatisticsCollectMillisTotal;
        this.planRefinements = Collections.unmodifiableMap(new LinkedHashMap<>(planRefinements));
        this.planExecuted = Collections.unmodifiableMap(new LinkedHashMap<>(planExecuted));
        this.freshness = freshness == null ? FreshnessStats.NONE : freshness;
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
        this.freshness = new FreshnessStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        builder.field("last_estimate_bytes", admissionLastEstimateBytes);
        builder.field("last_kind", admissionLastKind);
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

    /** Plan refinements this node's executor applied since it started, keyed by reason; every reason present. */
    public Map<String, Long> planRefinements() {
        return planRefinements;
    }

    /** Fragment requests this node's executor answered, keyed by {@code pushed_scan} and {@code lucene}. */
    public Map<String, Long> planExecuted() {
        return planExecuted;
    }

    /** This node's freshness checks of the Lance backed shards it holds. */
    public FreshnessStats freshness() {
        return freshness;
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
     * The node's available physical memory when the stats were collected
     * ({@code MemAvailable} on Linux, the free physical memory elsewhere),
     * before the headroom is subtracted.
     */
    public long admissionAvailableBytes() {
        return admissionAvailableBytes;
    }

    /**
     * Memory earlier admitted scans left in the process that the node's
     * next admission decision adds to the available memory (zero while a
     * gated request is in flight or a gated scan runs).
     */
    public long admissionRetainedBytes() {
        return admissionRetainedBytes;
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
            && admissionAvailableBytes == other.admissionAvailableBytes
            && admissionRetainedBytes == other.admissionRetainedBytes
            && warmUpMode.equals(other.warmUpMode)
            && warmUps.equals(other.warmUps)
            && indices.equals(other.indices)
            && localClones.equals(other.localClones)
            && planStatisticsTables == other.planStatisticsTables
            && planStatisticsCollectMillisTotal == other.planStatisticsCollectMillisTotal
            && planRefinements.equals(other.planRefinements)
            && planExecuted.equals(other.planExecuted)
            && freshness.equals(other.freshness);
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
            admissionAvailableBytes,
            admissionRetainedBytes,
            warmUpMode,
            warmUps,
            indices,
            localClones,
            planStatisticsTables,
            planStatisticsCollectMillisTotal,
            planRefinements,
            planExecuted,
            freshness
        );
    }
}
