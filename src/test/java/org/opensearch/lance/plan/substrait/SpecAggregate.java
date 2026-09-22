/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Test-only {@link Aggregate} subclass implementing
 * {@link LanceAggregateSpecs}: the stand-in for the planner's aggregate
 * node, which lands in a parallel change. The producer keys every
 * expansion decision on the {@link LanceAggregateSpecs.MetricSpec}
 * kinds, not on the Calcite aggregate functions, so tests build the
 * calls with whichever standard function types correctly and attach
 * the OpenSearch kind through the spec.
 */
public final class SpecAggregate extends Aggregate implements LanceAggregateSpecs {

    private final List<BucketSpec> buckets;
    private final List<MetricSpec> metrics;

    /**
     * @param input    the aggregate's input (a project, filter or scan)
     * @param groupSet group keys as positions in the input row type
     * @param calls    the aggregate calls, one per metric
     * @param buckets  one spec per group key, in group set order
     * @param metrics  one spec per call, in call order
     */
    public SpecAggregate(
        RelNode input,
        ImmutableBitSet groupSet,
        List<AggregateCall> calls,
        List<BucketSpec> buckets,
        List<MetricSpec> metrics
    ) {
        super(input.getCluster(), input.getTraitSet(), ImmutableList.of(), input, groupSet, ImmutableList.of(groupSet), calls);
        this.buckets = List.copyOf(buckets);
        this.metrics = List.copyOf(metrics);
    }

    @Override
    public BucketSpec bucket(int groupKeyIndex) {
        return buckets.get(groupKeyIndex);
    }

    @Override
    public MetricSpec metric(int callIndex) {
        return metrics.get(callIndex);
    }

    @Override
    public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new SpecAggregate(input, groupSet, aggCalls, buckets, metrics);
    }
}
