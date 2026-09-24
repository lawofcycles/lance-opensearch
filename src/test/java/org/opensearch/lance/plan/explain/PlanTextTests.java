/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The explain plan text: every physical operator line carries the two
 * trait values and its cost, the root the total, the logical operators
 * and the logical tree nothing of the kind, and the numbers read at two
 * significant digits.
 */
public class PlanTextTests extends OpenSearchTestCase {

    private static ExecutionShape shape(String json) throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse(json);
        int size = source.size() < 0 ? 10 : source.size();
        return new ExecutionShape(
            source.query(),
            source.postFilter(),
            source.sorts() == null ? List.of() : source.sorts(),
            source.searchAfter(),
            0,
            size,
            source.aggregations(),
            false
        );
    }

    /** The coordinator plan of {@code json} over three nodes, as the explain endpoint renders it. */
    private static String coordinatorText(String json) throws IOException {
        ExecutionShape shape = shape(json);
        RequestPlanner.Planned planned = RequestPlanner.plan(shape, PlanTestFixtures.model(), Set.of(), PlanTestFixtures.factory());
        return PlanText.render(planned.coordinatorPlan(shape, 3));
    }

    private static List<String> lines(String text) {
        return List.of(text.split("\n"));
    }

    public void testEveryPhysicalOperatorCarriesTraitsAndCostAndTheRootTheTotal() throws IOException {
        String text = coordinatorText("{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}");
        List<String> lines = lines(text);
        assertEquals(text, 3, lines.size());
        assertTrue(
            text,
            lines.get(0).startsWith("MergeExec(reduce=[AGGREGATE_INTERNAL], accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms=")
        );
        assertTrue(text, lines.get(0).contains("total_cost=[{ms="));
        assertTrue(
            text,
            lines.get(1)
                .startsWith(
                    "  FanOutExec(fanOut=[3], partitioning=[EQUAL_FRAGMENT_GROUPS], accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms="
                )
        );
        assertFalse("only the root carries the total: " + text, lines.get(1).contains("total_cost"));
        assertTrue(text, lines.get(2).startsWith("    LanceTableScan(table=[[lance, idx]], pushed=[[aggregate{"));
        assertTrue(text, lines.get(2).contains("accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms="));
        assertFalse(text, lines.get(2).contains("total_cost"));
        // The pushed aggregate below the fitted model's range is charged
        // its rows, a non zero figure; the coordinator layer's constants
        // are one millisecond each.
        assertTrue(text, lines.get(1).contains("cost=[{ms=3, native_bytes=3, heap_bytes=0}]"));
        assertTrue(text, lines.get(0).contains("cost=[{ms=1, native_bytes=1, heap_bytes=0}]"));
        assertFalse("the scan's cost is not zero: " + text, lines.get(2).contains("cost=[{ms=0,"));
    }

    public void testHitsPageDeclaresTheKeyOrderAndTheHeapPageTheSame() throws IOException {
        String pushed = coordinatorText("{\"size\":5,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":\"asc\"}]}");
        assertTrue(pushed, lines(pushed).get(0).contains("reduce=[HITS_TOP_K], accuracy=[EXACT], tie_stability=[STABLE_KEY]"));
        assertTrue(pushed, lines(pushed).get(2).contains("topk{"));
        assertTrue(pushed, lines(pushed).get(2).contains("tie_stability=[STABLE_KEY]"));

        String scored = coordinatorText("{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}");
        assertTrue(scored, lines(scored).get(0).contains("accuracy=[EXACT], tie_stability=[UNSTABLE]"));

        String heap = coordinatorText(
            "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"id\":\"asc\"}]}"
        );
        // The last collation is a stored column, so the heap page is
        // STABLE_KEY whatever the score order in front of it.
        assertTrue(heap, lines(heap).get(2).startsWith("    HeapTopKExec("));
        assertTrue(heap, lines(heap).get(2).contains("accuracy=[EXACT], tie_stability=[STABLE_KEY], cost=[{ms="));
        assertTrue(heap, lines(heap).get(0).contains("accuracy=[EXACT], tie_stability=[STABLE_KEY]"));
        assertEquals("the operator's input is the bare scan: " + heap, 4, lines(heap).size());
        assertTrue(
            heap,
            lines(heap).get(3).contains("LanceTableScan(table=[[lance, idx]], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[")
        );
    }

    public void testSketchAggregateDeclaresApproximateOnTheLuceneOperator() throws IOException {
        String text = coordinatorText("{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"category\"}}}}");
        assertTrue(text, lines(text).get(0).contains("accuracy=[APPROXIMATE], tie_stability=[UNSTABLE]"));
        assertTrue(text, lines(text).get(2).startsWith("    LuceneAggregateExec("));
        assertTrue(
            text,
            lines(text).get(2).contains("accuracy=[APPROXIMATE], tie_stability=[UNSTABLE], cost=[{ms=2, native_bytes=2, heap_bytes=1}]")
        );
    }

    public void testLogicalOperatorsCarryNoTraitOrCost() throws IOException {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
        String text = PlanText.render(logical);
        List<String> lines = lines(text);
        assertTrue(text, lines.get(0).startsWith("LanceAggregate("));
        assertFalse("a logical operator declares nothing: " + text, lines.get(0).contains("accuracy="));
        assertFalse(text, lines.get(0).contains("cost="));
        assertTrue(
            "the scan is physical even under a logical root: " + text,
            text.contains("LanceTableScan(table=[[lance, idx]], accuracy=[EXACT]")
        );
        assertFalse("the total sits on the root alone, and the root is logical: " + text, text.contains("total_cost"));
    }

    public void testShardPathRootCarriesTheFallbackTraits() throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse("{\"size\":5,\"highlight\":{\"fields\":{\"body\":{}}}}");
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelNode logical = SearchRequestToRel.translateDispatch(source, PlanTestFixtures.model(), factory);
        String text = PlanText.render(factory.plan(logical));
        assertTrue(
            text,
            lines(text).get(0)
                .startsWith("ShardPathFallbackExec(reasons=[[HIGHLIGHT]], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=100")
        );
        assertTrue("the total adds the scan below: " + text, lines(text).get(0).contains("total_cost=[{ms="));
        assertFalse(text, lines(text).get(0).contains("total_cost=[{ms=100,"));
    }

    public void testRenderWithoutTermsMatchesCalciteForLogicalTrees() throws IOException {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
        String calcite = RelOptUtil.toString(logical);
        String ours = PlanText.render(logical);
        // Same lines apart from the scan, which is physical and gets the terms.
        assertEquals(lines(calcite).get(0), lines(ours).get(0));
        assertTrue(ours, lines(ours).get(1).startsWith(lines(calcite).get(1).replace(")", ", accuracy=[EXACT]")));
    }

    public void testSignificantDigits() {
        assertEquals("0", PlanText.significant(0.0));
        assertEquals("1", PlanText.significant(1.0));
        assertEquals("1.2", PlanText.significant(1.234));
        assertEquals("12", PlanText.significant(12.34));
        assertEquals("1200", PlanText.significant(1234.5));
        assertEquals("0.0012", PlanText.significant(0.0012345));
        assertEquals("100", PlanText.significant(99.9));
        assertEquals("inf", PlanText.significant(Double.POSITIVE_INFINITY));
        assertEquals("nan", PlanText.significant(Double.NaN));
        assertEquals("1.8e+308", PlanText.significant(Double.MAX_VALUE));
        assertEquals(
            "{ms=1200, native_bytes=0, heap_bytes=0.5}",
            PlanText.format(PlanTestFixtures.factory().newCluster().getPlanner().getCostFactory().makeCost(1234, 0, 0.5))
        );
    }
}
