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
 * The coordinator's fan-out: the per-node subtree below it runs once
 * per fragment group on the data node that owns the group, and the
 * rows of every run flow upward unchanged, so the output row type is
 * the input row type. The node carries how many per-node requests
 * leave the coordinator ({@link #fanOut()}) and how the table's
 * fragments are cut into those requests ({@link #partitioning()}).
 * The runtime counterpart is the fragment fan-out the
 * {@code PlanExecutor} drives: one transport request per group, the
 * responses gathered for the {@link MergeExec} above.
 */
public final class FanOutExec extends SingleRel implements LuceneRel {

    /**
     * How the table's fragments are cut into per-node requests.
     * {@code EQUAL_FRAGMENT_GROUPS} is the only strategy today:
     * fragments are dealt round-robin across the sorted data-node
     * list, and a node's share is split further only when its rows
     * exceed what one Lucene reader may hold.
     */
    public enum Partitioning {
        EQUAL_FRAGMENT_GROUPS
    }

    private final int fanOut;
    private final Partitioning partitioning;

    /**
     * @param traits {@link LuceneConvention#INSTANCE} for the physical
     *     form, {@code Convention.NONE} for the logical form the
     *     translator wrapper builds
     * @param input the per-node subtree every fragment group runs
     * @param fanOut how many per-node requests leave the coordinator
     * @param partitioning how the fragments are cut into the requests
     */
    public FanOutExec(RelOptCluster cluster, RelTraitSet traits, RelNode input, int fanOut, Partitioning partitioning) {
        super(cluster, traits, input);
        this.fanOut = fanOut;
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
    }

    /** How many per-node requests leave the coordinator. */
    public int fanOut() {
        return fanOut;
    }

    /** How the table's fragments are cut into the per-node requests. */
    public Partitioning partitioning() {
        return partitioning;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FanOutExec(getCluster(), traitSet, sole(inputs), fanOut, partitioning);
    }

    /** The input's row type: the fan-out moves rows, it does not reshape them. */
    @Override
    protected RelDataType deriveRowType() {
        return input.getRowType();
    }

    /**
     * The tiny cost scaled by the fan-out width: each per-node request
     * costs one transport round trip, and nothing else is predicted
     * until the cost model gets real coefficients.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost().multiplyBy(Math.max(1, fanOut));
    }

    /** Prints the width and the partitioning so two fan-outs never share a digest. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fanOut", fanOut).item("partitioning", partitioning);
    }
}
