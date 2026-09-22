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
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.function.LongSupplier;

/**
 * Calcite table over one Lance backed index. The row type derives from the
 * Arrow schema of the attached table through {@link LanceTypeSystem}, and
 * the statistic carries the current row count only (no keys, no collations
 * yet).
 *
 * <p>The table holds no {@code Dataset}: the row count is read through a
 * supplier so the caller controls the dataset lifecycle and the table stays
 * a plain metadata object. The caller typically passes the sum of the
 * fragment row counts of the currently open dataset version.
 */
public final class LanceTable extends AbstractTable implements TranslatableTable {

    private final String indexName;
    private final Schema arrowSchema;
    private final LongSupplier rowCount;

    /**
     * @param indexName name of the OpenSearch index backed by the Lance table
     * @param arrowSchema Arrow schema of the attached Lance table
     * @param rowCount supplies the current row count on every read
     */
    public LanceTable(String indexName, Schema arrowSchema, LongSupplier rowCount) {
        this.indexName = indexName;
        this.arrowSchema = arrowSchema;
        this.rowCount = rowCount;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return LanceTypeSystem.rowType(arrowSchema, typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(rowCount.getAsLong(), ImmutableList.of());
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
