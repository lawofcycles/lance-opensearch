/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.dispatch.planner.AggregationRewriteContext;
import org.opensearch.lance.dispatch.planner.AggregationRewriteRegistry;
import org.opensearch.lance.dispatch.planner.AggregationRewriteRule;
import org.opensearch.lance.dispatch.planner.PlannerTestRegistries;
import org.opensearch.lance.dispatch.planner.PushdownPlan;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceAggregatePushdown#planViaRegistry} consults the rules
 * only for a tree the structural gate accepts, returns the first
 * matching rule's unwrapped plan, and falls through to the legacy
 * shape dispatcher when the registry is empty or no rule matches.
 * These tests drive the hook with curated registries over a one metric
 * tree the gate accepts; its field is unmapped in the mock context, so
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

    /** A single top level sum: the structural gate accepts it, and its unmapped field resolves to no legacy plan. */
    private static AggregatorFactories.Builder metricTree() {
        return AggregatorFactories.builder().addAggregator(AggregationBuilders.sum("s").field("f"));
    }

    private static LanceAggregatePushdown.Plan planVia(AggregationRewriteRegistry registry) {
        return planVia(registry, metricTree());
    }

    private static LanceAggregatePushdown.Plan planVia(AggregationRewriteRegistry registry, AggregatorFactories.Builder tree) {
        return LanceAggregatePushdown.planViaRegistry(
            tree,
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
        PushdownPlan wrapped = PushdownPlan.of(PlannerTestPlans.identityMarkerPlan());
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

    public void testNonCandidateTreeSkipsTheRules() {
        // The empty tree fails the structural gate, so a rule that
        // would match everything is never asked and the legacy path's
        // refusal is the answer.
        LanceAggregatePushdown.Plan out = planVia(
            PlannerTestRegistries.registryOf(new FixedRule("match", Optional.of(PushdownPlan.of(PlannerTestPlans.identityMarkerPlan())))),
            new AggregatorFactories.Builder()
        );

        assertNull(out);
    }

    public void testPublicOverloadDelegatesThroughTheHook() {
        // The 4 argument overload reads maxGroups from the node
        // settings and hands the production registry to the hook; the
        // empty tree fails the structural gate, so no rule is
        // consulted and the answer is the legacy path's null. A broken
        // delegation would throw here instead (the settings read or
        // the hook call would fail).
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            new Index("lance", "uuid"),
            Settings.EMPTY,
            Settings.builder().put(LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.getKey(), 123).build()
        );
        QueryShardContext qsc = mock(QueryShardContext.class);
        when(qsc.getIndexSettings()).thenReturn(indexSettings);

        LanceAggregatePushdown.Plan out = LanceAggregatePushdown.plan(
            new AggregatorFactories.Builder(),
            new Schema(List.of()),
            Map.of(),
            qsc
        );

        assertNull(out);
        // The null alone would also come from a hollowed overload; the
        // settings read proves the delegation ran.
        verify(qsc, atLeastOnce()).getIndexSettings();
    }
}
