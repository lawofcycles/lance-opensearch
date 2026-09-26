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
     * @param type Lance index type as the manifest entry records it
     *     ({@code BTREE}, {@code BITMAP}, {@code INVERTED},
     *     {@code ZONEMAP}, and {@code VECTOR} for every vector index);
     *     the concrete type the index statistics name replaces it when
     *     they were read; empty when the manifest names no type this
     *     Lance build maps
     * @param coveredFragments fragments the index covers (the union of
     *     the deltas' fragment bitmaps); equals {@code totalFragments}
     *     when no fragment is unindexed
     * @param totalFragments fragments in the table at this version
     * @param sizeBytes total size of the index files, empty when the
     *     manifest does not record them
     * @param indexedRows {@code num_indexed_rows} of
     *     {@code Dataset.getIndexStatistics}, empty when the statistics
     *     were not read or could not be read
     * @param unindexedRows {@code num_unindexed_rows}, likewise
     * @param distinctCount an estimate of the column's distinct values
     *     when the index type exposes one: for a bitmap index the sum of
     *     {@code num_bitmaps} over its deltas (a null bitmap counts as
     *     one value); empty for every other type, including BTree whose
     *     statistics report pages and bounds but no cardinality
     * @param partitions the IVF partition count of a vector index, the
     *     smallest {@code num_partitions} over its deltas (a nearest
     *     scan probes {@code nprobes} partitions of every delta, so the
     *     share of the index it loads is at most {@code nprobes} over
     *     the smallest count); empty for a scalar index and when the
     *     statistics report no count
     * @param integerRange for a BTree over an integer column, the number
     *     of integers from the smallest to the largest indexed value
     *     ({@code max - min + 1} over the deltas' {@code min} and
     *     {@code max}), an upper bound on the column's distinct values
     *     that a terms key over the column cannot exceed; empty for every
     *     other index, for a BTree over a column of any other type (a
     *     string or a float between two bounds has no finite count), for
     *     a column that holds nulls (Lance sorts nulls first when it
     *     trains the index, so the statistics report no smallest value)
     *     and when the range overflows a long. Not an estimate: a sparse
     *     column (five status codes between 200 and 599) has far fewer
     *     values than its range, so the figure bounds a group count from
     *     above and is not read where an undercount is unsafe (the
     *     admission gate's selectivity)
     * @param statisticsAvailable whether {@code getIndexStatistics} was
     *     read and answered for this index; the collector reads it for
     *     bitmap, BTree and vector indexes only, so this is false for
     *     every other type and leaves the row, distinct, range and
     *     partition figures empty
     */
    public record IndexSummary(String name, Optional<IndexType> type, int coveredFragments, int totalFragments, OptionalLong sizeBytes,
        OptionalLong indexedRows, OptionalLong unindexedRows, OptionalLong distinctCount, OptionalLong partitions,
        OptionalLong integerRange, boolean statisticsAvailable) {

        /** A summary whose statistics report no integer range (every index but a BTree over an integer column). */
        public IndexSummary(
            String name,
            Optional<IndexType> type,
            int coveredFragments,
            int totalFragments,
            OptionalLong sizeBytes,
            OptionalLong indexedRows,
            OptionalLong unindexedRows,
            OptionalLong distinctCount,
            OptionalLong partitions,
            boolean statisticsAvailable
        ) {
            this(
                name,
                type,
                coveredFragments,
                totalFragments,
                sizeBytes,
                indexedRows,
                unindexedRows,
                distinctCount,
                partitions,
                OptionalLong.empty(),
                statisticsAvailable
            );
        }

        /** A summary whose statistics report neither a partition count nor an integer range (a bitmap, an inverted index, a zone map). */
        public IndexSummary(
            String name,
            Optional<IndexType> type,
            int coveredFragments,
            int totalFragments,
            OptionalLong sizeBytes,
            OptionalLong indexedRows,
            OptionalLong unindexedRows,
            OptionalLong distinctCount,
            boolean statisticsAvailable
        ) {
            this(
                name,
                type,
                coveredFragments,
                totalFragments,
                sizeBytes,
                indexedRows,
                unindexedRows,
                distinctCount,
                OptionalLong.empty(),
                statisticsAvailable
            );
        }

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

    /**
     * Statistics whose zone map is already known, so no dataset read is
     * needed: {@link #zoneMap(Dataset)} and {@link #zoneMapIfRead()}
     * answer {@code zoneMap} from the start. For callers that hold the
     * zones themselves (tests, fixtures).
     */
    public ColumnStatistics(String column, List<IndexSummary> indexes, List<ZoneStats> zoneMap) {
        this(column, indexes);
        this.zoneMap = List.copyOf(zoneMap);
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
     * the tightest. Only the counts an index measured (a bitmap's
     * {@code num_bitmaps}) enter here, not the integer range of a
     * BTree: the admission gate divides by this figure to size the rows
     * an equality selects, and a range wider than the values it holds
     * would make that share too small. {@link #distinctUpperBound}
     * adds the range for the callers a bound serves.
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
     * The tightest upper bound on the column's distinct values any index
     * gives: the smallest of {@link #distinctCount} and the integer
     * ranges of the BTree indexes over an integer column
     * ({@link IndexSummary#integerRange}), empty when neither exists.
     * The planner's group estimate of a terms key reads this: the groups
     * a key produces cannot exceed its values, and a bound that is too
     * wide only makes the estimate cautious, where the guess it replaces
     * (a share of the table's rows) grows with the table and puts every
     * large table's nested tree above the hash table threshold whatever
     * the column holds.
     */
    public OptionalLong distinctUpperBound() {
        OptionalLong best = distinctCount();
        for (IndexSummary index : indexes) {
            if (index.integerRange().isPresent() && (best.isEmpty() || index.integerRange().getAsLong() < best.getAsLong())) {
                best = index.integerRange();
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
