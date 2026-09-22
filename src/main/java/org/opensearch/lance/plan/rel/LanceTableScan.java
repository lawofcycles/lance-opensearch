/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LanceRel;

import java.util.List;

/**
 * Scan of a Lance backed index, the leaf every plan over a Lance table
 * starts from. A plain scan for now: pushdown state (filters, projections,
 * aggregations pushed into the dataset scan) is not modelled yet.
 */
public class LanceTableScan extends TableScan implements LanceRel {

    /** Creates the scan with the {@link LanceConvention} trait. */
    public LanceTableScan(RelOptCluster cluster, RelOptTable table) {
        this(cluster, cluster.traitSetOf(LanceConvention.INSTANCE), table);
    }

    private LanceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new LanceTableScan(getCluster(), traitSet, table);
    }

    /** Row count from the table's statistic (the Lance fragment row counts). */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return table.getRowCount();
    }

    /**
     * Rows read stand in for predicted milliseconds until the cost model
     * gets real coefficients; native and heap bytes are not modelled yet,
     * so both byte slots stay zero and the budget check in the cost
     * ordering cannot fire on a bare scan.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = estimateRowCount(mq);
        return planner.getCostFactory().makeCost(rows, 0, 0);
    }
}
