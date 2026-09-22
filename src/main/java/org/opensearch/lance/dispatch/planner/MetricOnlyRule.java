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
 * The metric only shape: every top level aggregation is a metric the
 * scan computes and there is no bucket level. The match and the plan
 * are one call, {@link LanceAggregatePushdown#resolveMetricOnly}: it
 * refuses a tree whose first top level aggregation is not such a
 * metric and a metric that does not resolve against the schema and
 * the mapping, and both refusals answer empty here so the request
 * falls through. Package private: the registry in this package is the
 * only production caller.
 */
final class MetricOnlyRule implements AggregationRewriteRule {

    static final String NAME = "metric_only";

    @Override
    public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
        List<AggregationBuilder> top = new ArrayList<>(ctx.aggregations().getAggregatorFactories());
        LanceAggregatePushdown.Plan plan = LanceAggregatePushdown.resolveMetricOnly(
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
