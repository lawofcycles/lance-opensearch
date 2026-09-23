/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;

import java.util.List;
import java.util.Objects;

/**
 * Aggregation executed through Lucene's stock aggregator machinery
 * over the per-fragment leaf readers instead of inside the Lance
 * dataset scan. The node wraps the {@link LanceAggregate} it stands in
 * for (the bucket and metric specs, with the aggregate's input rebuilt
 * to the concrete projection / filter / scan tree, exactly like a
 * pushed aggregate carries it) and takes the bare
 * {@link LanceTableScan} as its input, because the aggregators consume
 * the fragment readers the scan exposes. The runtime path is the
 * existing Lucene aggregator fallback in
 * {@code TransportLanceFragmentQueryAction}; no executor traverses
 * this node yet, it makes the fallback visible to the Volcano
 * planner's cost comparison.
 */
public final class LuceneAggregateExec extends SingleRel implements LuceneRel {

    private final LanceAggregate aggregate;

    /**
     * @param traitSet must carry {@link LuceneConvention#INSTANCE}
     * @param input the bare scan whose fragment readers the
     *     aggregators run over
     * @param aggregate the aggregate the node stands in for, its input
     *     rebuilt to the concrete tree
     */
    public LuceneAggregateExec(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, LanceAggregate aggregate) {
        super(cluster, traitSet, input);
        this.aggregate = Objects.requireNonNull(aggregate, "aggregate");
    }

    /** The aggregate the node stands in for, with its concrete input tree. */
    public LanceAggregate aggregate() {
        return aggregate;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LuceneAggregateExec(getCluster(), traitSet, sole(inputs), aggregate);
    }

    /** The wrapped aggregate's row type: the node stands in for the whole aggregate. */
    @Override
    protected RelDataType deriveRowType() {
        return aggregate.getRowType();
    }

    /** One row per group, the wrapped aggregate's own estimate. */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return aggregate.estimateRowCount(mq);
    }

    /**
     * A constant until the cost model gets real coefficients: the tiny
     * cost plus one unit in every slot. The offset keeps this
     * alternative strictly above the zero cost of
     * {@link LuceneHandoffExec}, so whenever a pushdown rule folds the
     * same tree into the Lance scan, the Lance plan costs less and the
     * Volcano planner never has to break a tie between the two
     * conventions.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCostFactory factory = planner.getCostFactory();
        return factory.makeTinyCost().plus(factory.makeCost(1, 1, 1));
    }

    /**
     * Prints the grouping, the calls, the specs and the wrapped tree's
     * projection expressions and filter predicate, so two nodes
     * wrapping different aggregates never share a digest and the
     * explain output shows the OpenSearch shape.
     */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("group", aggregate.getGroupSet());
        pw.itemIf("calls", aggregate.getAggCallList(), !aggregate.getAggCallList().isEmpty());
        pw.itemIf("buckets", aggregate.bucketSpecs(), !aggregate.bucketSpecs().isEmpty());
        pw.itemIf("metrics", aggregate.metricSpecs(), !aggregate.metricSpecs().isEmpty());
        RelNode node = aggregate.getInput();
        while (true) {
            if (node instanceof Project project) {
                pw.item("keys", project.getProjects());
            } else if (node instanceof Filter filter) {
                pw.item("filter", filter.getCondition());
            } else {
                break;
            }
            node = node.getInput(0);
        }
        return pw;
    }
}
