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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LanceRel;
import org.apache.calcite.rex.RexNode;
import org.opensearch.lance.plan.rel.PushedOperation.PushedAggregate;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/**
 * Scan of a Lance backed index, the leaf every plan over a Lance table
 * starts from. The scan carries an immutable list of
 * {@link PushedOperation}s the Lance dataset scan computes; a scan with
 * a pushed aggregate stands in for the whole aggregate it replaced, so
 * its row type is the aggregate's row type and its row estimate is the
 * aggregate's group estimate.
 */
public class LanceTableScan extends TableScan implements LanceRel {

    private final ImmutableList<PushedOperation> pushedOperations;

    /** Creates the bare scan with the {@link LanceConvention} trait. */
    public LanceTableScan(RelOptCluster cluster, RelOptTable table) {
        this(cluster, cluster.traitSetOf(LanceConvention.INSTANCE), table, ImmutableList.of());
    }

    private LanceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, ImmutableList<PushedOperation> pushed) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.pushedOperations = pushed;
    }

    /** The pushed operations, in push order; empty for a bare scan. */
    public List<PushedOperation> pushedOperations() {
        return pushedOperations;
    }

    /** The pushed aggregate, when the scan carries one. */
    public Optional<PushedAggregate> pushedAggregate() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedAggregate aggregate) {
                return Optional.of(aggregate);
            }
        }
        return Optional.empty();
    }

    /** The pushed filter, when the scan carries one. */
    public Optional<PushedFilter> pushedFilter() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedFilter filter) {
                return Optional.of(filter);
            }
        }
        return Optional.empty();
    }

    /**
     * The same scan with {@code aggregate} pushed into it: the scan's
     * row type becomes the aggregate's and the plan above no longer
     * contains the aggregate. {@code aggregate} is the node the rule
     * matched with its input rebuilt to the concrete tree; {@code bytes}
     * is what the Substrait producer encoded for it.
     */
    public LanceTableScan withPushedAggregate(LanceAggregate aggregate, ByteBuffer bytes) {
        if (pushedAggregate().isPresent()) {
            throw new IllegalStateException("the scan already carries a pushed aggregate");
        }
        ImmutableList<PushedOperation> pushed = ImmutableList.<PushedOperation>builder()
            .addAll(pushedOperations)
            .add(new PushedAggregate(aggregate, bytes))
            .build();
        return new LanceTableScan(getCluster(), getTraitSet(), table, pushed);
    }

    /**
     * The same scan with {@code condition} pushed as its filter,
     * spelled as {@code sql} for the executor's
     * {@code ScanOptions.filter}. The row type does not change.
     */
    public LanceTableScan withPushedFilter(RexNode condition, String sql) {
        if (pushedFilter().isPresent()) {
            throw new IllegalStateException("the scan already carries a pushed filter");
        }
        ImmutableList<PushedOperation> pushed = ImmutableList.<PushedOperation>builder()
            .addAll(pushedOperations)
            .add(new PushedFilter(condition, sql))
            .build();
        return new LanceTableScan(getCluster(), getTraitSet(), table, pushed);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new LanceTableScan(getCluster(), traitSet, table, pushedOperations);
    }

    /** The aggregate's row type when one is pushed, the table row type otherwise. */
    @Override
    public RelDataType deriveRowType() {
        Optional<PushedAggregate> pushed = pushedAggregate();
        if (pushed.isPresent()) {
            return pushed.get().aggregate().getRowType();
        }
        return super.deriveRowType();
    }

    /**
     * Row count from the table's statistic (the Lance fragment row
     * counts) for a bare scan, scaled by the pushed filter's guessed
     * selectivity when one is pushed; a pushed aggregate returns one
     * row per group, so its own estimate stands.
     */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        Optional<PushedAggregate> pushed = pushedAggregate();
        if (pushed.isPresent()) {
            return pushed.get().aggregate().estimateRowCount(mq);
        }
        double rows = table.getRowCount();
        Optional<PushedFilter> filter = pushedFilter();
        if (filter.isPresent()) {
            rows *= RelMdUtil.guessSelectivity(filter.get().condition());
        }
        return rows;
    }

    /**
     * Rows read (a bare scan) or groups returned (a pushed aggregate)
     * stand in for predicted milliseconds until the cost model gets
     * real coefficients, with a constant per pushed operation in the
     * second slot so two scans over the same table order by how much
     * work was pushed. Native and heap bytes are not modelled yet, so
     * the byte slots stay at the constant model and the budget check in
     * the cost ordering cannot fire on a scan.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = estimateRowCount(mq);
        return planner.getCostFactory().makeCost(rows, pushedOperations.size(), 0);
    }

    /** Prints the pushed operations, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("pushed", pushedOperations, !pushedOperations.isEmpty());
    }
}
