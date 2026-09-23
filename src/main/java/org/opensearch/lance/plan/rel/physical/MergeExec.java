/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;

import java.util.List;
import java.util.Objects;

/**
 * The coordinator's reduce over the per-node answers a
 * {@link FanOutExec} below it gathered: per-node aggregation trees
 * reduce through the stock {@code InternalAggregations} reduce,
 * per-node hit pages merge by the request's sort into one page,
 * per-node match counts sum. The output row type is the input row
 * type: the reduce combines rows of the shape the per-node subtree
 * produced, it does not reshape them. The runtime counterpart is the
 * coordinator's merge the {@code PlanExecutor} drives once every
 * per-node response is in.
 */
public final class MergeExec extends SingleRel implements LuceneRel {

    /** Which reduce combines the per-node answers. */
    public enum ReduceKind {
        /** Per-node {@code InternalAggregations} trees, reduced through the stock top level reduce. */
        AGGREGATE_INTERNAL,
        /** Per-node hit pages, merged by the request's sort (or by score) and cut to the requested page. */
        HITS_TOP_K,
        /** Per-node match counts, summed under the request's {@code track_total_hits} contract. */
        COUNT_SUM,
        /** Per-node hits passed through unmerged, for a shard mode fallback; no runtime path builds this yet. */
        RAW_HITS
    }

    private final ReduceKind reduceKind;

    /**
     * @param traits {@link LuceneConvention#INSTANCE} for the physical
     *     form, {@code Convention.NONE} for the logical form the
     *     translator wrapper builds
     * @param input the {@link FanOutExec} whose gathered answers reduce here
     * @param reduceKind which reduce combines them
     */
    public MergeExec(RelOptCluster cluster, RelTraitSet traits, RelNode input, ReduceKind reduceKind) {
        super(cluster, traits, input);
        this.reduceKind = Objects.requireNonNull(reduceKind, "reduceKind");
    }

    /** Which reduce combines the per-node answers. */
    public ReduceKind reduceKind() {
        return reduceKind;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MergeExec(getCluster(), traitSet, sole(inputs), reduceKind);
    }

    /** The input's row type: the reduce combines rows, it does not reshape them. */
    @Override
    protected RelDataType deriveRowType() {
        return input.getRowType();
    }

    /** A constant until the cost model gets real coefficients: the reduce runs once, whatever the fan-out width. */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    /** Prints the reduce kind so two merges never share a digest. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("reduce", reduceKind);
    }
}
