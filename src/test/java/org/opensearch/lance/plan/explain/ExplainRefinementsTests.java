/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner.Reason;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * The explain endpoint's prediction of the data node's refinements
 * follows the refiner: a reader wrapper drops a pushed aggregate, a
 * pushed page and a full text clause but not a knn or the filter SQL,
 * and a page ordered by an {@code ip} column moves to the collector.
 */
public class ExplainRefinementsTests extends OpenSearchTestCase {

    private static final Set<String> NO_IP = Set.of();
    private static final Set<String> IP_ADDR = Set.of("addr");

    private static FragmentPlan pushedPage(String column) {
        return new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            "rating >= 5",
            null,
            new FragmentPlan.TopK(List.of(new FragmentPlan.ScanOrdering(column, true, false)), 10, null),
            null
        );
    }

    private static FragmentPlan pushedAggregate() {
        return new FragmentPlan(
            FragmentPlan.Kind.PUSHED_SCAN,
            null,
            null,
            null,
            new FragmentPlan.Aggregate(new byte[] { 1 }, 0, List.of(new FragmentPlan.MetricSlot("s", MetricSpec.Kind.SUM)))
        );
    }

    public void testNothingAppliesWithoutAWrapperOnAPlainPage() {
        assertEquals(List.of(), ExplainRefinements.predict(pushedPage("rating"), false, NO_IP));
        assertEquals(List.of(), ExplainRefinements.predict(pushedAggregate(), false, NO_IP));
        assertEquals(List.of(), ExplainRefinements.predict(FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, "rating = 1"), true, NO_IP));
    }

    public void testAWrapperDropsThePushedAggregateAndPage() {
        assertEquals(List.of(Reason.SECURITY_WRAPPER), ExplainRefinements.predict(pushedAggregate(), true, NO_IP));
        assertEquals(List.of(Reason.SECURITY_WRAPPER), ExplainRefinements.predict(pushedPage("rating"), true, NO_IP));
    }

    public void testAWrapperDropsAFullTextClauseButKeepsAKnn() {
        FragmentPlan fts = new FragmentPlan(
            FragmentPlan.Kind.LUCENE_COUNT,
            "rating >= 5",
            new LanceMatchQueryBuilder("body", "hello"),
            null,
            null
        );
        assertEquals(List.of(Reason.SECURITY_WRAPPER), ExplainRefinements.predict(fts, true, NO_IP));

        float[] vector = new float[8];
        FragmentPlan knn = new FragmentPlan(
            FragmentPlan.Kind.LUCENE_TOPK,
            "rating >= 5",
            new LanceKnnQueryBuilder("embedding", vector, 3),
            null,
            null
        );
        assertEquals(List.of(), ExplainRefinements.predict(knn, true, NO_IP));
    }

    public void testAPageOrderedByAnIpColumnMovesToTheCollector() {
        assertEquals(List.of(Reason.SORT_FIELD_TYPE), ExplainRefinements.predict(pushedPage("addr"), false, IP_ADDR));
        assertEquals(List.of(), ExplainRefinements.predict(pushedPage("rating"), false, IP_ADDR));
        // The wrapper drops the page first, so the sort guard has
        // nothing left to judge.
        assertEquals(List.of(Reason.SECURITY_WRAPPER), ExplainRefinements.predict(pushedPage("addr"), true, IP_ADDR));
    }
}
