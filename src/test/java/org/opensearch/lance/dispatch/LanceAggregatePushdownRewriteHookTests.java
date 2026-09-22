/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.planner.AggregationRewriteContext;
import org.opensearch.lance.dispatch.planner.AggregationRewriteRegistry;
import org.opensearch.lance.dispatch.planner.AggregationRewriteRule;
import org.opensearch.lance.dispatch.planner.PlannerTestRegistries;
import org.opensearch.lance.dispatch.planner.PushdownPlan;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceAggregatePushdown#planViaRegistry} returns the first
 * matching rule's unwrapped plan and falls through to the legacy shape
 * dispatcher when the registry is empty or no rule matches. These
 * tests drive the hook with curated registries; the empty aggregation
 * tree they pass is refused by the legacy dispatcher's shape check, so
 * a fall through answers null and a rule match answers the wrapped
 * plan, which tells the two paths apart.
 */
public class LanceAggregatePushdownRewriteHookTests extends OpenSearchTestCase {

    /** Answers its fixed plan regardless of the context. */
    private record FixedRule(String name, Optional<PushdownPlan> answer) implements AggregationRewriteRule {
        @Override
        public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
            return answer;
        }
    }

    private static LanceAggregatePushdown.Plan planVia(AggregationRewriteRegistry registry) {
        return LanceAggregatePushdown.planViaRegistry(
            new AggregatorFactories.Builder(),
            new Schema(List.of()),
            Map.of(),
            mock(QueryShardContext.class),
            registry,
            0,
            0,
            0
        );
    }

    public void testMatchingRuleAnswersItsUnwrappedPlan() {
        PushdownPlan wrapped = PushdownPlan.of(PlannerTestPlans.emptyPlan());
        LanceAggregatePushdown.Plan out = planVia(PlannerTestRegistries.registryOf(new FixedRule("match", Optional.of(wrapped))));

        assertSame(wrapped.asLegacyPlan(), out);
    }

    public void testEmptyRegistryFallsThroughToTheLegacyPath() {
        LanceAggregatePushdown.Plan out = planVia(PlannerTestRegistries.registryOf());

        // The legacy dispatcher refuses the empty tree, so the fall
        // through answer is null; a rule match would have answered the
        // wrapped plan instead.
        assertNull(out);
    }

    public void testNonMatchingRuleFallsThroughToTheLegacyPath() {
        LanceAggregatePushdown.Plan out = planVia(PlannerTestRegistries.registryOf(new FixedRule("miss", Optional.empty())));

        assertNull(out);
    }
}
