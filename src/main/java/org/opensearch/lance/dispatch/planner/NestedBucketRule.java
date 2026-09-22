/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.lance.dispatch.LanceAggregatePushdown;
import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * The nested bucket shape, every pushdown shape that is neither metric
 * only nor composite: a single top level bucket aggregation
 * ({@code terms}, {@code histogram}, {@code date_histogram},
 * {@code range}, {@code date_range}, {@code filter}, {@code filters}
 * or {@code missing}), each level carrying any number of metric
 * children and at most one nested bucket. The match and the plan are
 * one call, {@link LanceAggregatePushdown#resolveNestedBucketShape}:
 * it refuses a tree that is not this shape (an empty top, a metric top
 * or a composite top, which the other two rules own), a level or a
 * metric that does not resolve against the schema and the mapping, a
 * group estimate above the context's bound, and a {@code terms} order
 * the top-k selection does not support, and every refusal answers
 * empty here so the request falls through to the aggregators. Package
 * private: the registry in this package is the only production caller.
 */
final class NestedBucketRule implements AggregationRewriteRule {

    static final String NAME = "nested_bucket";

    @Override
    public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
        List<AggregationBuilder> top = new ArrayList<>(ctx.aggregations().getAggregatorFactories());
        LanceAggregatePushdown.Plan plan = LanceAggregatePushdown.resolveNestedBucketShape(
            top,
            ctx.schema(),
            ctx.multiFields(),
            ctx.queryShardContext(),
            ctx.maxGroups(),
            ctx.percentilesBins(),
            ctx.topKSlack()
        );
        return plan == null ? Optional.empty() : Optional.of(PushdownPlan.of(plan));
    }

    @Override
    public String name() {
        return NAME;
    }
}
