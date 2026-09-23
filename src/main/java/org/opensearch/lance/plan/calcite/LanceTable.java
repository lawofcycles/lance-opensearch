/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Calcite table over one Lance backed index. The row type derives from the
 * Arrow schema of the attached table through {@link LanceTypeSystem}, and
 * the statistic carries the current row count only (no keys, no collations
 * yet).
 *
 * <p>The table holds no {@code Dataset}: the row count is read through a
 * supplier so the caller controls the dataset lifecycle and the table stays
 * a plain metadata object. A caller that has the table's
 * {@link TableStatistics} (collected once per manifest version) passes
 * them instead; the row count then comes from them, and the planner's
 * metadata handlers read the column statistics through
 * {@link #tableStatistics()}. Without them the caller passes the row
 * count alone, typically the sum of the fragment row counts of the
 * currently open dataset version.
 */
public final class LanceTable extends AbstractTable implements TranslatableTable {

    private final String indexName;
    private final Schema arrowSchema;
    private final LongSupplier rowCount;
    private final Supplier<TableStatistics> statistics;

    /**
     * @param indexName name of the OpenSearch index backed by the Lance table
     * @param arrowSchema Arrow schema of the attached Lance table
     * @param rowCount supplies the current row count on every read
     */
    public LanceTable(String indexName, Schema arrowSchema, LongSupplier rowCount) {
        this(indexName, arrowSchema, rowCount, null);
    }

    /**
     * @param indexName name of the OpenSearch index backed by the Lance table
     * @param arrowSchema Arrow schema of the attached Lance table
     * @param rowCount supplies the current row count on every read; only
     *     read when {@code statistics} is null
     * @param statistics supplies the table statistics on every read, or
     *     null when the caller has none
     */
    public LanceTable(String indexName, Schema arrowSchema, LongSupplier rowCount, Supplier<TableStatistics> statistics) {
        this.indexName = indexName;
        this.arrowSchema = arrowSchema;
        this.rowCount = rowCount;
        this.statistics = statistics;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return LanceTypeSystem.rowType(arrowSchema, typeFactory);
    }

    /**
     * The Arrow schema of the attached Lance table, for rules that
     * need the physical column types behind the row type (the sort
     * pushdown reads it to decide whether Lance can order by a
     * column).
     */
    public Schema arrowSchema() {
        return arrowSchema;
    }

    /**
     * The table statistics the caller supplied, empty when the table
     * was built with a row count alone.
     */
    public Optional<TableStatistics> tableStatistics() {
        return statistics == null ? Optional.empty() : Optional.ofNullable(statistics.get());
    }

    @Override
    public Statistic getStatistic() {
        Optional<TableStatistics> tableStatistics = tableStatistics();
        double rows = tableStatistics.isPresent() ? tableStatistics.get().rowCount() : rowCount.getAsLong();
        return Statistics.of(rows, ImmutableList.of());
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TABLE;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new LanceTableScan(context.getCluster(), relOptTable);
    }

    @Override
    public String toString() {
        return "LanceTable{" + indexName + "}";
    }
}
