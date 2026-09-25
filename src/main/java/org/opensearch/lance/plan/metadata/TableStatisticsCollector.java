/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentStatistics;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.lance.schema.LanceField;
import org.opensearch.lance.plan.metadata.ColumnStatistics.IndexSummary;
import org.opensearch.lance.plan.metadata.TableStatistics.FragmentStats;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Reads a {@link TableStatistics} from an open {@code Dataset} using
 * metadata calls only. One collection makes these Lance calls:
 * {@code getFragmentStatistics} once (row count per fragment),
 * {@code getFragments} once (physical rows, for the deleted row count),
 * {@code getLanceSchema} once (field id to column name),
 * {@code getIndexes} once, and {@code getIndexStatistics} once per
 * distinct bitmap or vector index name. No row is scanned and
 * {@code countRows} is not called.
 *
 * <p>{@code getIndexStatistics} answers a JSON object (as a
 * {@code Map}) whose top level, assembled by Lance for every scalar
 * and vector index, carries {@code index_type}, {@code name},
 * {@code num_indices}, {@code num_indexed_fragments},
 * {@code num_unindexed_fragments}, {@code num_indexed_rows},
 * {@code num_unindexed_rows}, {@code num_indexed_rows_per_delta},
 * {@code updated_at_timestamp_ms} and {@code indices}, a list with one
 * object per delta from the index implementation's own statistics: a
 * BTree reports {@code num_pages}, {@code min} and {@code max}; a
 * bitmap {@code num_bitmaps}; an inverted index {@code num_tokens},
 * {@code num_docs} and {@code params}; a zone map {@code num_zones} and
 * {@code rows_per_zone}; an IVF vector index {@code num_partitions},
 * its partition sizes, sub index and quantizer metadata. The figures
 * the planner and the admission gate read out of it are the bitmap's
 * {@code num_bitmaps} (the column's distinct value estimate) and the
 * IVF index's {@code num_partitions} (the share of the index a nearest
 * scan probes), so the call is made for bitmap and vector indexes only:
 * Lance assembles the answer from the index files, and on a table of
 * ten billion rows an inverted index takes minutes to answer. Every
 * other index is summarised from the manifest alone (type, fragment
 * coverage, size), with the row figures empty and
 * {@code statisticsAvailable} false. A bitmap or vector index whose
 * statistics Lance cannot produce is recorded the same way; the
 * collection as a whole still succeeds.
 */
public final class TableStatisticsCollector {

    private static final Logger LOGGER = LogManager.getLogger(TableStatisticsCollector.class);

    private TableStatisticsCollector() {}

    /** Collect the statistics of {@code dataset} at its current version. */
    public static TableStatistics collect(Dataset dataset) {
        long startNanos = System.nanoTime();
        long version = dataset.version();

        FragmentStatistics fragmentStatistics = dataset.getFragmentStatistics();
        int[] ids = fragmentStatistics.getIds();
        long[] rowCounts = fragmentStatistics.getRowCounts();
        int[] dataFileNums = fragmentStatistics.getDataFileNums();
        List<FragmentStats> fragments = new ArrayList<>(ids.length);
        long rowCount = 0L;
        for (int i = 0; i < ids.length; i++) {
            fragments.add(new FragmentStats(ids[i], rowCounts[i], dataFileNums[i]));
            rowCount += rowCounts[i];
        }
        long physicalRows = 0L;
        for (Fragment fragment : dataset.getFragments()) {
            physicalRows += fragment.metadata().getPhysicalRows();
        }
        long deletedRows = Math.max(0L, physicalRows - rowCount);

        Map<Integer, String> columnsByFieldId = new HashMap<>();
        for (LanceField field : dataset.getLanceSchema().fields()) {
            collectFieldNames(field, "", columnsByFieldId);
        }

        Map<String, ColumnStatistics> columns = new LinkedHashMap<>();
        int indexCount = 0;
        int statisticsMissing = 0;
        Map<String, List<Index>> deltasByName = new LinkedHashMap<>();
        for (Index index : dataset.getIndexes()) {
            deltasByName.computeIfAbsent(index.name(), k -> new ArrayList<>()).add(index);
        }
        Map<String, List<IndexSummary>> summariesByColumn = new LinkedHashMap<>();
        for (Map.Entry<String, List<Index>> entry : deltasByName.entrySet()) {
            String name = entry.getKey();
            List<Index> deltas = entry.getValue();
            Index first = deltas.get(0);
            String column = first.fields().isEmpty() ? null : columnsByFieldId.get(first.fields().get(0));
            if (column == null) {
                LOGGER.debug("index {} covers no schema field ({}), skipped", name, first.fields());
                continue;
            }
            indexCount++;
            IndexSummary summary = summarise(dataset, name, deltas, fragments.size());
            if (readsStatistics(summary.type()) && !summary.statisticsAvailable()) {
                statisticsMissing++;
            }
            summariesByColumn.computeIfAbsent(column, k -> new ArrayList<>()).add(summary);
        }
        for (Map.Entry<String, List<IndexSummary>> entry : summariesByColumn.entrySet()) {
            columns.put(entry.getKey(), new ColumnStatistics(entry.getKey(), entry.getValue()));
        }

        TableStatistics statistics = new TableStatistics(rowCount, deletedRows, fragments, columns, version, Instant.now());
        long millis = (System.nanoTime() - startNanos) / 1_000_000L;
        LOGGER.debug(
            "collected table statistics of {} at version {} in {} ms: {} rows, {} deleted, {} fragments, {} indexes ({} bitmap or vector indexes without statistics)",
            dataset.uri(),
            version,
            millis,
            rowCount,
            deletedRows,
            fragments.size(),
            indexCount,
            statisticsMissing
        );
        return statistics;
    }

    private static void collectFieldNames(LanceField field, String prefix, Map<Integer, String> out) {
        String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
        out.put(field.getId(), path);
        List<LanceField> children = field.getChildren();
        if (children != null) {
            for (LanceField child : children) {
                collectFieldNames(child, path, out);
            }
        }
    }

    private static IndexSummary summarise(Dataset dataset, String name, List<Index> deltas, int totalFragments) {
        Optional<IndexType> type = Optional.ofNullable(deltas.get(0).indexType());
        Set<Integer> covered = new HashSet<>();
        boolean coverageKnown = true;
        long sizeBytes = 0L;
        boolean sizeKnown = true;
        for (Index delta : deltas) {
            Optional<List<Integer>> fragments = delta.fragments();
            if (fragments.isPresent()) {
                covered.addAll(fragments.get());
            } else {
                coverageKnown = false;
            }
            Optional<Long> size = delta.getSizeBytes();
            if (size.isPresent()) {
                sizeBytes += size.get();
            } else {
                sizeKnown = false;
            }
        }
        // An index recorded before Lance kept fragment bitmaps covers
        // every fragment that existed when it was built; treat it as
        // covering the whole table rather than none of it.
        int coveredFragments = coverageKnown ? covered.size() : totalFragments;
        IndexSummary fromManifest = new IndexSummary(
            name,
            type,
            coveredFragments,
            totalFragments,
            sizeKnown ? OptionalLong.of(sizeBytes) : OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            false
        );
        if (!readsStatistics(type)) {
            // Nothing the planner reads is in the statistics of the
            // other index types, and Lance assembles them from the
            // index files, so the call is not made.
            return fromManifest;
        }
        Map<String, Object> raw;
        try {
            raw = dataset.getIndexStatistics(name);
        } catch (RuntimeException e) {
            LOGGER.debug("index statistics of {} ({}) unavailable: {}", name, type.map(Enum::name).orElse("unknown type"), e.toString());
            return fromManifest;
        }
        ParsedStatistics parsed = readStatistics(raw, type);
        return new IndexSummary(
            name,
            parsed.type(),
            coveredFragments,
            totalFragments,
            sizeKnown ? OptionalLong.of(sizeBytes) : OptionalLong.empty(),
            parsed.indexedRows(),
            parsed.unindexedRows(),
            parsed.distinctCount(),
            parsed.partitions(),
            true
        );
    }

    /** The figures read out of one {@code getIndexStatistics} answer, and the index type it names. */
    record ParsedStatistics(Optional<IndexType> type, OptionalLong indexedRows, OptionalLong unindexedRows, OptionalLong distinctCount,
        OptionalLong partitions) {
    }

    /**
     * Read the index type, the row figures, for a bitmap index the
     * distinct value estimate and for an IVF vector index the partition
     * count out of the map {@code getIndexStatistics} returned. The
     * map's {@code index_type} names the concrete type
     * ({@code IVF_PQ}, {@code BTree}, {@code ZoneMap}, ...) where the
     * manifest entry behind {@code Dataset.getIndexes()} only says
     * {@code VECTOR} for every vector index, so it replaces
     * {@code declared} when it maps to an {@link IndexType}. The
     * partition count is the smallest {@code num_partitions} of the
     * deltas under {@code indices}: a nearest scan probes the same
     * number of partitions in every delta, so the smallest count bounds
     * the share of the index the probes load. A missing or non numeric
     * entry leaves its figure empty; a {@code null} map leaves all of
     * them empty and the type as declared.
     */
    static ParsedStatistics readStatistics(Map<String, Object> raw, Optional<IndexType> declared) {
        if (raw == null) {
            return new ParsedStatistics(declared, OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty());
        }
        Optional<IndexType> type = indexType(raw.get("index_type")).or(() -> declared);
        OptionalLong indexedRows = longValue(raw.get("num_indexed_rows"));
        OptionalLong unindexedRows = longValue(raw.get("num_unindexed_rows"));
        OptionalLong distinct = OptionalLong.empty();
        OptionalLong partitions = OptionalLong.empty();
        if (type.isPresent() && type.get() == IndexType.BITMAP && raw.get("indices") instanceof List<?> deltas) {
            long bitmaps = 0L;
            boolean any = false;
            for (Object delta : deltas) {
                if (delta instanceof Map<?, ?> deltaMap) {
                    OptionalLong count = longValue(deltaMap.get("num_bitmaps"));
                    if (count.isPresent()) {
                        bitmaps += count.getAsLong();
                        any = true;
                    }
                }
            }
            if (any) {
                distinct = OptionalLong.of(bitmaps);
            }
        }
        if (type.isPresent() && isVectorIndex(type.get()) && raw.get("indices") instanceof List<?> deltas) {
            for (Object delta : deltas) {
                if (delta instanceof Map<?, ?> deltaMap) {
                    OptionalLong count = longValue(deltaMap.get("num_partitions"));
                    if (count.isPresent()
                        && count.getAsLong() > 0L
                        && (partitions.isEmpty() || count.getAsLong() < partitions.getAsLong())) {
                        partitions = count;
                    }
                }
            }
        }
        return new ParsedStatistics(type, indexedRows, unindexedRows, distinct, partitions);
    }

    /** Whether {@code type} is a vector index ({@code VECTOR} or any {@code IVF_*}), whose statistics carry a partition count. */
    private static boolean isVectorIndex(IndexType type) {
        return type == IndexType.VECTOR || type.name().startsWith("IVF");
    }

    /**
     * Whether the collector asks Lance for the statistics of an index of
     * {@code type}: a bitmap (its {@code num_bitmaps}) or a vector index
     * (its {@code num_partitions}); no other type's answer carries a
     * figure the planner or the admission gate reads.
     */
    static boolean readsStatistics(Optional<IndexType> type) {
        return type.isPresent() && (type.get() == IndexType.BITMAP || isVectorIndex(type.get()));
    }

    /**
     * The {@link IndexType} whose name, ignoring case and underscores,
     * matches Lance's {@code index_type} string ({@code BTree},
     * {@code IVF_PQ}, {@code BloomFilter}); empty for {@code N/A} and
     * anything else that names no type.
     */
    private static Optional<IndexType> indexType(Object value) {
        if (!(value instanceof String name)) {
            return Optional.empty();
        }
        String normalised = name.replace("_", "").toUpperCase(Locale.ROOT);
        for (IndexType candidate : IndexType.values()) {
            if (candidate.name().replace("_", "").equals(normalised)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private static OptionalLong longValue(Object value) {
        if (value instanceof Number number) {
            return OptionalLong.of(number.longValue());
        }
        return OptionalLong.empty();
    }
}
