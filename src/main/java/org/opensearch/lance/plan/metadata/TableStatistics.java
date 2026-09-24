/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import org.lance.Dataset;
import org.lance.index.IndexType;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * What the planner knows about one Lance table at one manifest version,
 * read from the table's metadata alone: the live row count, how many
 * physical rows a deletion file hides, the fragment list with each
 * fragment's row count and data file count, and per column the indexes
 * that cover it (see {@link ColumnStatistics}). Immutable apart from the
 * lazily read zone map inside a column's statistics, which is memoised
 * on first read for the life of this instance.
 *
 * <p>Row counts here are logical: {@code Dataset.getFragmentStatistics()}
 * reports physical rows minus deleted rows per fragment, so
 * {@link #rowCount()} is what a full scan returns and
 * {@link #deletedRows()} is the difference to the physical total the
 * fragment metadata records.
 */
public final class TableStatistics {

    /** One fragment of the table: its id, live row count and number of data files. */
    public record FragmentStats(int id, long rows, int dataFiles) {
    }

    private final long rowCount;
    private final long deletedRows;
    private final List<FragmentStats> fragments;
    private final Map<String, ColumnStatistics> columns;
    private final long datasetVersion;
    private final Instant collectedAt;

    /**
     * @param rowCount live rows across every fragment
     * @param deletedRows physical rows hidden by deletion files
     * @param fragments the fragments in manifest order
     * @param columns statistics by column name (dotted path for nested
     *     fields), for the columns that carry at least one index
     * @param datasetVersion manifest version the statistics describe
     * @param collectedAt when they were read
     */
    public TableStatistics(
        long rowCount,
        long deletedRows,
        List<FragmentStats> fragments,
        Map<String, ColumnStatistics> columns,
        long datasetVersion,
        Instant collectedAt
    ) {
        this.rowCount = rowCount;
        this.deletedRows = deletedRows;
        this.fragments = List.copyOf(fragments);
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
        this.datasetVersion = datasetVersion;
        this.collectedAt = collectedAt;
    }

    /** Live rows across every fragment. */
    public long rowCount() {
        return rowCount;
    }

    /** Physical rows hidden by deletion files: the physical total minus {@link #rowCount()}. */
    public long deletedRows() {
        return deletedRows;
    }

    /** The fragments in manifest order. */
    public List<FragmentStats> fragments() {
        return fragments;
    }

    /** Statistics of every column that carries at least one index, by column name. */
    public Map<String, ColumnStatistics> columns() {
        return columns;
    }

    /** Statistics of {@code column}, empty when no index covers it. */
    public Optional<ColumnStatistics> column(String column) {
        return Optional.ofNullable(columns.get(column));
    }

    /**
     * Reads the zone maps of every column that carries a zone map index
     * and that {@code fields} names, directly or through a dotted child
     * or sub field ({@code body.raw} reads the zone map of {@code body}),
     * so a planner that runs after {@code dataset} is closed can prune
     * fragments with them ({@link ColumnStatistics#zoneMapIfRead()}).
     * Each zone map is read once per instance and memoised; a column
     * without a zone map index or outside {@code fields} is left
     * unread. A read that fails propagates its {@link RuntimeException};
     * callers treat the zone maps as an input to plan quality and catch
     * it rather than fail the request.
     *
     * @param dataset the open dataset at {@link #datasetVersion()}
     * @param fields the request fields the query's leaves name
     */
    public void readZoneMaps(Dataset dataset, Set<String> fields) {
        if (fields.isEmpty()) {
            return;
        }
        for (ColumnStatistics column : columns.values()) {
            if (column.hasIndex(IndexType.ZONEMAP) && namesColumn(fields, column.column())) {
                column.zoneMap(dataset);
            }
        }
    }

    /** Whether some field is {@code column} or a dotted path below it. */
    static boolean namesColumn(Set<String> fields, String column) {
        if (fields.contains(column)) {
            return true;
        }
        String prefix = column + ".";
        for (String field : fields) {
            if (field.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /** Manifest version the statistics describe. */
    public long datasetVersion() {
        return datasetVersion;
    }

    /** When the statistics were read. */
    public Instant collectedAt() {
        return collectedAt;
    }

    @Override
    public String toString() {
        return "TableStatistics{version="
            + datasetVersion
            + ", rows="
            + rowCount
            + ", deleted="
            + deletedRows
            + ", fragments="
            + fragments.size()
            + ", indexedColumns="
            + columns.keySet()
            + "}";
    }
}
