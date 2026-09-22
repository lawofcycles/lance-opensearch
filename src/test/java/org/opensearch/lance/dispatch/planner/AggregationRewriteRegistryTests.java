/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link AggregationRewriteRegistry#rewrite} asks each rule in
 * registration order and returns the first non empty answer, so an
 * empty registry always answers empty (the dispatcher's fall through
 * to the legacy shape dispatcher) and the first matching rule wins
 * when several would match. The registry itself never reads the
 * context, so these tests hand every rule the same null-heavy one.
 */
public class AggregationRewriteRegistryTests extends OpenSearchTestCase {

    /** Answers its fixed plan regardless of the context. */
    private record TestRule(String name, Optional<PushdownPlan> answer) implements AggregationRewriteRule {
        @Override
        public Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx) {
            return answer;
        }
    }

    private static AggregationRewriteContext context() {
        return new AggregationRewriteContext(null, null, null, null, 0, 0, 0);
    }

    public void testEmptyRegistryReturnsEmpty() {
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(List.of());

        assertTrue(registry.rewrite(context()).isEmpty());
        assertTrue(registry.rules().isEmpty());
    }

    public void testSingleMatchingRuleReturnsItsPlan() {
        PushdownPlan plan = PushdownPlan.of(null);
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
        PushdownPlan plan = PushdownPlan.of(null);
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(
            List.of(new TestRule("miss", Optional.empty()), new TestRule("match", Optional.of(plan)))
        );

        Optional<PushdownPlan> out = registry.rewrite(context());

        assertTrue(out.isPresent());
        assertSame(plan, out.get());
    }

    public void testFirstMatchWins() {
        PushdownPlan first = PushdownPlan.of(null);
        PushdownPlan second = PushdownPlan.of(null);
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(
            List.of(new TestRule("first", Optional.of(first)), new TestRule("second", Optional.of(second)))
        );

        Optional<PushdownPlan> out = registry.rewrite(context());

        assertTrue(out.isPresent());
        assertSame(first, out.get());
    }

    public void testRulesListIsImmutable() {
        PushdownPlan plan = PushdownPlan.of(null);
        TestRule original = new TestRule("original", Optional.of(plan));
        List<AggregationRewriteRule> input = new ArrayList<>();
        input.add(original);
        AggregationRewriteRegistry registry = new AggregationRewriteRegistry(input);

        // Mutating the handed-in list after construction does not
        // reach the registry's own copy.
        input.clear();
        assertEquals(1, registry.rules().size());
        assertSame(original, registry.rules().get(0));
        assertSame(plan, registry.rewrite(context()).orElseThrow());

        // And the accessor's view refuses mutation.
        expectThrows(UnsupportedOperationException.class, () -> registry.rules().add(new TestRule("late", Optional.empty())));
    }
}
