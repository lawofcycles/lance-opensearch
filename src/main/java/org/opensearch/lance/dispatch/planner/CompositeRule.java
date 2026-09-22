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
 * The composite shape: the single top level aggregation is a
 * {@code composite} over {@code terms} and fixed interval
 * {@code date_histogram} sources whose children are all metrics, and
 * the executor applies {@code size} and {@code after} paging to the
 * sorted group rows. The match and the plan are one call,
 * {@link LanceAggregatePushdown#resolveCompositeShape}: it refuses a
 * tree that is not this shape (a {@code missing_bucket} source among
 * the refusals) and a source or metric that does not resolve against
 * the schema and the mapping, and both refusals answer empty here so
 * the request falls through. Package private: the registry in this
 * package is the only production caller.
 */
final class CompositeRule implements AggregationRewriteRule {

    static final String NAME = "composite";

    @Override
    public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
        List<AggregationBuilder> top = new ArrayList<>(ctx.aggregations().getAggregatorFactories());
        LanceAggregatePushdown.Plan plan = LanceAggregatePushdown.resolveCompositeShape(
            top,
            ctx.schema(),
            ctx.multiFields(),
            ctx.queryShardContext(),
            ctx.percentilesBins()
        );
        return plan == null ? Optional.empty() : Optional.of(PushdownPlan.of(plan));
    }

    @Override
    public String name() {
        return NAME;
    }
}
