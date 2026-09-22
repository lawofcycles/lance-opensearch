/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Aggregate over a Lance backed scan that carries the OpenSearch
 * request semantics Calcite's {@link Aggregate} does not model: one
 * {@link BucketSpec} per group key (in group set order) and one
 * {@link MetricSpec} per aggregate call (in call order).
 *
 * <p>A nested bucket tree ({@code terms > terms > avg}) is one
 * aggregate whose group set has one key per level, with the specs in
 * level order; Calcite has no notion of nested aggregations and the
 * executor rebuilds the tree from the flat groups, as the hand written
 * pushdown does today. The group key expressions themselves (field
 * references, floored divisions, {@code CASE WHEN} bit masks) live in
 * the projection under this node; for {@code filter} / {@code filters}
 * buckets the per filter predicates are additionally carried here in
 * {@link #filterPredicates()}, aligned with the bucket specs, because
 * the Substrait producer reads them back per filter while the projected
 * mask has already summed them.
 *
 * <p>The node is logical (convention {@link Convention#NONE}); the
 * pushdown rule and the physical conversions come later.
 */
public class LanceAggregate extends Aggregate {

    private final ImmutableList<BucketSpec> bucketSpecs;
    private final ImmutableList<MetricSpec> metricSpecs;
    private final ImmutableList<ImmutableList<RexNode>> filterPredicates;

    private LanceAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        ImmutableList<BucketSpec> bucketSpecs,
        ImmutableList<MetricSpec> metricSpecs,
        ImmutableList<ImmutableList<RexNode>> filterPredicates
    ) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
        if (bucketSpecs.size() != groupSet.cardinality()) {
            throw new IllegalArgumentException(bucketSpecs.size() + " bucket specs for " + groupSet.cardinality() + " group keys");
        }
        if (metricSpecs.size() != aggCalls.size()) {
            throw new IllegalArgumentException(metricSpecs.size() + " metric specs for " + aggCalls.size() + " aggregate calls");
        }
        if (filterPredicates.size() != bucketSpecs.size()) {
            throw new IllegalArgumentException(
                filterPredicates.size() + " filter predicate lists for " + bucketSpecs.size() + " bucket specs"
            );
        }
        this.bucketSpecs = bucketSpecs;
        this.metricSpecs = metricSpecs;
        this.filterPredicates = filterPredicates;
    }

    /**
     * Creates the aggregate with a simple group set (no rollup) at the
     * logical convention.
     *
     * @param input the scan or the projection of the group key
     *     expressions and metric argument columns
     * @param groupSet the group key positions in the input row
     * @param aggCalls one call per metric, in request traversal order
     * @param bucketSpecs one spec per group key, in group set order
     * @param metricSpecs one spec per call, in call order
     * @param filterPredicates per group key, the filter / filters
     *     predicates over the input row type (empty for the other
     *     bucket kinds)
     */
    public static LanceAggregate create(
        RelNode input,
        ImmutableBitSet groupSet,
        List<AggregateCall> aggCalls,
        List<BucketSpec> bucketSpecs,
        List<MetricSpec> metricSpecs,
        List<? extends List<RexNode>> filterPredicates
    ) {
        RelOptCluster cluster = input.getCluster();
        ImmutableList.Builder<ImmutableList<RexNode>> predicates = ImmutableList.builder();
        for (List<RexNode> perKey : filterPredicates) {
            predicates.add(ImmutableList.copyOf(perKey));
        }
        return new LanceAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            ImmutableList.copyOf(aggCalls),
            ImmutableList.copyOf(bucketSpecs),
            ImmutableList.copyOf(metricSpecs),
            predicates.build()
        );
    }

    /** One spec per group key, in group set order. */
    public List<BucketSpec> bucketSpecs() {
        return bucketSpecs;
    }

    /** One spec per aggregate call, in call order. */
    public List<MetricSpec> metricSpecs() {
        return metricSpecs;
    }

    /**
     * Per group key, the filter / filters predicates, parallel to the
     * spec's {@code filterKeys}; empty for the bucket kinds that are
     * not filter shaped. The predicates are expressions over
     * {@code getInput().getRowType()}: the projection under this node
     * carries a pass through of every scan column after the group key
     * expressions, and the predicates reference those pass through
     * positions.
     */
    public List<ImmutableList<RexNode>> filterPredicates() {
        return filterPredicates;
    }

    /**
     * Copies the node, keeping the specs. The specs are aligned with
     * the group keys and the calls, so a copy may substitute the input
     * and the traits (what planner rules do) but not the grouping or
     * the calls; changing those without the specs would silently
     * misalign them.
     *
     * @throws IllegalArgumentException when {@code groupSet},
     *     {@code groupSets} or {@code aggCalls} differ from this node's
     *     own
     */
    @Override
    public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        if (!getGroupSet().equals(groupSet) || !getGroupSets().equals(groupSets) || !getAggCallList().equals(aggCalls)) {
            throw new IllegalArgumentException(
                "a LanceAggregate copy cannot change the grouping or the calls: the bucket and metric specs are aligned with them"
            );
        }
        return new LanceAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, bucketSpecs, metricSpecs, filterPredicates);
    }

    /** The same aggregate over a substituted input, for rules that rewrite the tree below this node. */
    public LanceAggregate withInput(RelNode input) {
        return input == getInput() ? this : (LanceAggregate) copy(getTraitSet(), input, getGroupSet(), getGroupSets(), getAggCallList());
    }

    /**
     * Prints the specs after the stock aggregate terms, so the explain
     * output shows the OpenSearch shape next to the relational one. The
     * filter predicates print only when a filter shaped bucket carries
     * them.
     */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("buckets", bucketSpecs, !bucketSpecs.isEmpty());
        pw.itemIf("metrics", metricSpecs, !metricSpecs.isEmpty());
        boolean anyPredicates = filterPredicates.stream().anyMatch(perKey -> !perKey.isEmpty());
        pw.itemIf("filters", filterPredicates, anyPredicates);
        return pw;
    }
}
