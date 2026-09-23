/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import org.lance.Dataset;
import org.lance.index.IndexType;
import org.lance.index.scalar.ZoneStats;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * What the planner knows about one column of a Lance table: the indexes
 * that cover it, each with its type, fragment coverage, size and the
 * figures Lance's index statistics report for it, plus the column's zone
 * map when a zone map index exists.
 *
 * <p>The zone map is not read when the table statistics are collected:
 * it has one entry per zone per fragment, so a wide table with many
 * fragments would pay for zones the planner never consults. It is read
 * on the first {@link #zoneMap(Dataset)} call and kept for the life of
 * the owning {@link TableStatistics}, which is one manifest version, so
 * the memoised zones never go stale.
 */
public final class ColumnStatistics {

    /**
     * One index over the column, as {@code Dataset.getIndexes()} lists
     * it, with every delta of the same index name folded into one
     * summary.
     *
     * @param name index name
     * @param type Lance index type: the concrete type the index
     *     statistics name ({@code IVF_PQ}, {@code BTree}, ...) when
     *     they were read, since the manifest entry says {@code VECTOR}
     *     for every vector index; otherwise the manifest entry's type;
     *     empty when neither names a type this Lance build maps
     * @param coveredFragments fragments the index covers (the union of
     *     the deltas' fragment bitmaps); equals {@code totalFragments}
     *     when no fragment is unindexed
     * @param totalFragments fragments in the table at this version
     * @param sizeBytes total size of the index files, empty when the
     *     manifest does not record them
     * @param indexedRows {@code num_indexed_rows} of
     *     {@code Dataset.getIndexStatistics}, empty when the statistics
     *     could not be read
     * @param unindexedRows {@code num_unindexed_rows}, likewise
     * @param distinctCount an estimate of the column's distinct values
     *     when the index type exposes one: for a bitmap index the sum of
     *     {@code num_bitmaps} over its deltas (a null bitmap counts as
     *     one value); empty for every other type, including BTree whose
     *     statistics report pages and bounds but no cardinality
     * @param statisticsAvailable whether {@code getIndexStatistics}
     *     answered for this index; false leaves the row and distinct
     *     figures empty
     */
    public record IndexSummary(String name, Optional<IndexType> type, int coveredFragments, int totalFragments, OptionalLong sizeBytes,
        OptionalLong indexedRows, OptionalLong unindexedRows, OptionalLong distinctCount, boolean statisticsAvailable) {
        /** Whether every fragment of the table is covered by this index. */
        public boolean coversAllFragments() {
            return coveredFragments >= totalFragments;
        }

        /** Whether the index is of {@code type}. */
        public boolean is(IndexType candidate) {
            return type.isPresent() && type.get() == candidate;
        }
    }

    private final String column;
    private final List<IndexSummary> indexes;
    private volatile List<ZoneStats> zoneMap;

    /**
     * @param column column name (dotted path for a nested field)
     * @param indexes the indexes covering the column
     */
    public ColumnStatistics(String column, List<IndexSummary> indexes) {
        this.column = column;
        this.indexes = List.copyOf(indexes);
    }

    /** Column name. */
    public String column() {
        return column;
    }

    /** The indexes covering the column. */
    public List<IndexSummary> indexes() {
        return indexes;
    }

    /** Whether some index of {@code type} covers the column. */
    public boolean hasIndex(IndexType type) {
        for (IndexSummary index : indexes) {
            if (index.is(type)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The smallest distinct value estimate any index over the column
     * reports, empty when none does. Several indexes over one column
     * each bound the cardinality from above, so the smallest bound is
     * the tightest.
     */
    public OptionalLong distinctCount() {
        OptionalLong best = OptionalLong.empty();
        for (IndexSummary index : indexes) {
            if (index.distinctCount().isPresent() && (best.isEmpty() || index.distinctCount().getAsLong() < best.getAsLong())) {
                best = index.distinctCount();
            }
        }
        return best;
    }

    /**
     * The column's zone map, read from {@code dataset} on the first call
     * and memoised. An empty list when the column has no zone map index.
     * {@code dataset} must be open at the version the owning
     * {@link TableStatistics} describes.
     */
    public List<ZoneStats> zoneMap(Dataset dataset) {
        List<ZoneStats> zones = zoneMap;
        if (zones == null) {
            synchronized (this) {
                zones = zoneMap;
                if (zones == null) {
                    zones = hasIndex(IndexType.ZONEMAP) ? List.copyOf(dataset.getZonemapStats(column)) : List.of();
                    zoneMap = zones;
                }
            }
        }
        return zones;
    }

    /** The zone map if a previous {@link #zoneMap(Dataset)} call read it, empty otherwise. */
    public Optional<List<ZoneStats>> zoneMapIfRead() {
        return Optional.ofNullable(zoneMap);
    }

    @Override
    public String toString() {
        return "ColumnStatistics{" + column + ", indexes=" + indexes + "}";
    }
}
