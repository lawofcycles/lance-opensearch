/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.PlannerTestPlans;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link AggregationRewriteRegistry#rewrite} asks each rule in
 * registration order and returns the first non empty answer, so an
 * empty registry always answers empty (the dispatcher's fall through
 * to the legacy shape dispatcher) and the first matching rule wins
 * when several would match. The registry itself never reads the
 * context, so these tests hand every rule the same minimal one.
 */
public class AggregationRewriteRegistryTests extends OpenSearchTestCase {

    /** Answers its fixed plan regardless of the context. */
    private record TestRule(String name, Optional<PushdownPlan> answer) implements AggregationRewriteRule {
        @Override
        public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
            return answer;
        }
    }

    private static PushdownPlan plan() {
        return PushdownPlan.of(PlannerTestPlans.identityMarkerPlan());
    }

    private static AggregationRewriteContext context() {
        return new AggregationRewriteContext(
            new AggregatorFactories.Builder(),
            new Schema(List.of()),
            Map.of(),
            mock(QueryShardContext.class),
            0,
            0,
            0
        );
    }

    public void testEmptyRegistryReturnsEmpty() {
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(List.of());

        assertTrue(registry.rewrite(context()).isEmpty());
        assertTrue(registry.rules().isEmpty());
    }

    public void testSingleMatchingRuleReturnsItsPlan() {
        PushdownPlan plan = plan();
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(List.of(new TestRule("match", Optional.of(plan))));

        Optional<PushdownPlan> out = registry.rewrite(context());

        assertTrue(out.isPresent());
        assertSame(plan, out.get());
    }

    public void testSingleNonMatchingRuleReturnsEmpty() {
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(List.of(new TestRule("miss", Optional.empty())));

        assertTrue(registry.rewrite(context()).isEmpty());
    }

    public void testSecondRuleMatchesAfterFirstReturnsEmpty() {
        PushdownPlan plan = plan();
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(
            List.of(new TestRule("miss", Optional.empty()), new TestRule("match", Optional.of(plan)))
        );

        Optional<PushdownPlan> out = registry.rewrite(context());

        assertTrue(out.isPresent());
        assertSame(plan, out.get());
    }

    public void testFirstMatchWins() {
        PushdownPlan first = plan();
        PushdownPlan second = plan();
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(
            List.of(new TestRule("first", Optional.of(first)), new TestRule("second", Optional.of(second)))
        );

        Optional<PushdownPlan> out = registry.rewrite(context());

        assertTrue(out.isPresent());
        assertSame(first, out.get());
    }

    public void testRulesListIsImmutable() {
        List<AggregationRewriteRule> input = new ArrayList<>();
        input.add(new TestRule("miss", Optional.empty()));
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(input);

        // A rule appended to the handed-in list after construction is
        // not seen by the registry: the dispatch answer stays empty and
        // the rule list keeps its size. Would fail if the constructor
        // stopped copying.
        input.add(new TestRule("appended, matches everything", Optional.of(plan())));
        assertTrue(registry.rewrite(context()).isEmpty());
        assertEquals(1, registry.rules().size());
        assertEquals("miss", registry.rules().get(0).name());

        // And the accessor's view refuses mutation.
        expectThrows(UnsupportedOperationException.class, () -> registry.rules().add(new TestRule("late", Optional.empty())));
    }

    public void testProductionInstanceIsEmpty() {
        // No rule is registered yet; a rule accidentally registered on
        // the production singleton in a later change must trip this.
        assertTrue(AggregationRewriteRegistry.instance().rules().isEmpty());
    }

    public void testRejectsEmptyAndDuplicateRuleNames() {
        expectThrows(IllegalArgumentException.class, () -> new AggregationRewriteRegistry(List.of(new TestRule("", Optional.empty()))));
        expectThrows(
            IllegalArgumentException.class,
            () -> new AggregationRewriteRegistry(List.of(new TestRule("twin", Optional.empty()), new TestRule("twin", Optional.empty())))
        );
    }

    public void testNullRejection() {
        // No match is Optional.empty(), never a null plan.
        expectThrows(NullPointerException.class, () -> PushdownPlan.of(null));
        // Every reference field of the context is required.
        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        Schema schema = new Schema(List.of());
        Map<String, LinkedHashMap<String, String>> multiFields = Map.of();
        QueryShardContext qsc = mock(QueryShardContext.class);
        expectThrows(NullPointerException.class, () -> new AggregationRewriteContext(null, schema, multiFields, qsc, 0, 0, 0));
        expectThrows(NullPointerException.class, () -> new AggregationRewriteContext(aggregations, null, multiFields, qsc, 0, 0, 0));
        expectThrows(NullPointerException.class, () -> new AggregationRewriteContext(aggregations, schema, null, qsc, 0, 0, 0));
        expectThrows(NullPointerException.class, () -> new AggregationRewriteContext(aggregations, schema, multiFields, null, 0, 0, 0));
    }
}
