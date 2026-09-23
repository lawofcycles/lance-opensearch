/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The converter rule fires on every logical chain the translator
 * builds: with the pushdown and fuse rules removed from the planner,
 * the Lucene operator is the only physical form, so the Volcano run
 * pins that each chain config matches and produces the operator
 * wrapping the right logical nodes over the bare scan.
 */
public class LanceToLuceneConverterRuleTests extends OpenSearchTestCase {

    /**
     * Runs the Volcano planner over {@code logical} demanding the
     * Lucene convention, with every rule that folds into the Lance
     * scan removed, so only the converter rule can answer.
     */
    private static RelNode luceneOnly(RelNode logical) {
        RelOptPlanner planner = logical.getCluster().getPlanner();
        for (RelOptRule rule : List.copyOf(planner.getRules())) {
            if (rule instanceof PushAggregateIntoLanceScan
                || rule instanceof PushFilterIntoLanceScan
                || rule instanceof PushSortLimitIntoLanceScan
                || rule instanceof FuseFtsWithFilter
                || rule instanceof FuseKnnWithFilter) {
                planner.removeRule(rule);
            }
        }
        return PlanTestFixtures.factory().plan(logical);
    }

    private static RelNode translate(String body) throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse(body));
    }

    private static LuceneAggregateExec aggregateExec(String body) throws IOException {
        RelNode logical = translate(body);
        RelNode physical = luceneOnly(logical);
        assertTrue("the converter produced the aggregate operator: " + physical, physical instanceof LuceneAggregateExec);
        LuceneAggregateExec exec = (LuceneAggregateExec) physical;
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue("the input is the bare scan: " + exec.getInput(), exec.getInput() instanceof LanceTableScan);
        assertTrue(((LanceTableScan) exec.getInput()).pushedOperations().isEmpty());
        assertEquals("the row type survives the conversion", logical.getRowType(), exec.getRowType());
        return exec;
    }

    private static HeapTopKExec topKExec(RelNode logical) {
        RelNode physical = luceneOnly(logical);
        assertTrue("the converter produced the top-k operator: " + physical, physical instanceof HeapTopKExec);
        HeapTopKExec exec = (HeapTopKExec) physical;
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue("the input is the bare scan: " + exec.getInput(), exec.getInput() instanceof LanceTableScan);
        assertEquals("the row type survives the conversion", logical.getRowType(), exec.getRowType());
        return exec;
    }

    public void testAggregateOverScan() throws IOException {
        LuceneAggregateExec exec = aggregateExec("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        assertTrue(exec.aggregate().getInput() instanceof LanceTableScan);
    }

    public void testAggregateOverProjection() throws IOException {
        LuceneAggregateExec exec = aggregateExec("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}");
        assertTrue(
            "the group key projection is kept inside the wrapped aggregate: " + exec.aggregate().getInput(),
            exec.aggregate().getInput() instanceof org.apache.calcite.rel.core.Project
        );
    }

    public void testAggregateOverFilter() throws IOException {
        LuceneAggregateExec exec = aggregateExec(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"
        );
        assertTrue(exec.aggregate().getInput() instanceof Filter);
    }

    public void testAggregateOverProjectionOverFilter() throws IOException {
        LuceneAggregateExec exec = aggregateExec(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}"
        );
        RelNode input = exec.aggregate().getInput();
        assertTrue(input instanceof org.apache.calcite.rel.core.Project);
        assertTrue(input.getInput(0) instanceof Filter);
    }

    public void testHitShapeOverTopKOverScan() throws IOException {
        RelNode logical = translate("{\"size\":10}");
        assertTrue(logical instanceof LanceHitShape);
        HeapTopKExec exec = topKExec(logical);
        assertNotNull("the hit envelope rides on the operator", exec.hitShape());
        assertEquals(10, exec.topK().fetch());
        assertTrue(exec.topK().getInput() instanceof LanceTableScan);
    }

    public void testHitShapeOverTopKOverFilter() throws IOException {
        HeapTopKExec exec = topKExec(translate("{\"size\":5,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"asc\"}]}"));
        assertNotNull(exec.hitShape());
        assertTrue(exec.topK().getInput() instanceof Filter);
    }

    public void testHitShapeOverTopKOverFts() throws IOException {
        HeapTopKExec exec = topKExec(translate("{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"));
        assertNotNull(exec.hitShape());
        assertTrue(exec.topK().getInput() instanceof LanceFtsMatch);
    }

    public void testHitShapeOverTopKOverFtsOverFilter() throws IOException {
        HeapTopKExec exec = topKExec(
            translate(
                "{\"size\":3,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
                    + "\"filter\":[{\"term\":{\"category\":\"c0\"}}]}}}"
            )
        );
        assertNotNull(exec.hitShape());
        RelNode fts = exec.topK().getInput();
        assertTrue(fts instanceof LanceFtsMatch);
        assertTrue(fts.getInput(0) instanceof Filter);
    }

    public void testHitShapeOverTopKOverKnn() throws IOException {
        HeapTopKExec exec = topKExec(
            translate("{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5}}}")
        );
        assertNotNull(exec.hitShape());
        assertTrue(exec.topK().getInput() instanceof LanceKnnSearch);
    }

    public void testHitShapeOverTopKOverKnnOverFilter() throws IOException {
        HeapTopKExec exec = topKExec(
            translate(
                "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
                    + "\"filter\":{\"range\":{\"rating\":{\"gte\":10}}}}}}"
            )
        );
        assertNotNull(exec.hitShape());
        RelNode knn = exec.topK().getInput();
        assertTrue(knn instanceof LanceKnnSearch);
        assertTrue(knn.getInput(0) instanceof Filter);
    }

    /**
     * A {@link LanceTopK} without a hit envelope, the shape the
     * fragment routing plans for a sorted page, converts through the
     * bare top-k configs.
     */
    public void testBareTopKOverScan() throws IOException {
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelNode root = SearchRequestToRel.translateQuery(null, PlanTestFixtures.model(), factory);
        LanceTopK topK = new LanceTopK(root.getCluster(), root.getCluster().traitSetOf(Convention.NONE), root, List.of(), 7, 0, null);
        HeapTopKExec exec = topKExec(topK);
        assertNull("no hit envelope rides on the bare shape", exec.hitShape());
        assertEquals(7, exec.topK().fetch());
    }

    public void testBareTopKOverFilter() throws IOException {
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelNode root = SearchRequestToRel.translateQuery(
            PlanTestFixtures.parse("{\"query\":{\"term\":{\"category\":\"c0\"}}}").query(),
            PlanTestFixtures.model(),
            factory
        );
        assertTrue(root instanceof Filter);
        LanceTopK topK = new LanceTopK(root.getCluster(), root.getCluster().traitSetOf(Convention.NONE), root, List.of(), 7, 0, null);
        HeapTopKExec exec = topKExec(topK);
        assertNull(exec.hitShape());
        assertTrue(exec.topK().getInput() instanceof Filter);
    }

    /**
     * A {@link LanceAggregate} chain the aggregate pushdown rule refuses
     * (the projection over the query filter, with a cardinality metric
     * the rule's operand rejects) still gets a physical form: with every
     * rule registered, the full planner answers the shape with the
     * Lucene operator instead of a {@code CannotPlanException}, and the
     * filter stays inside the wrapped tree for the Lucene side.
     */
    public void testProjectionOverFilterChainConvertsUnderTheFullRuleSet() throws IOException {
        RelNode logical = translate(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},"
                + "\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"rating\"}}}}}}"
        );
        assertTrue(logical instanceof LanceAggregate);
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("the full rule set answers with the Lucene operator: " + physical, physical instanceof LuceneAggregateExec);
        RelNode input = ((LuceneAggregateExec) physical).aggregate().getInput();
        assertTrue(input instanceof org.apache.calcite.rel.core.Project);
        assertTrue(input.getInput(0) instanceof Filter);
    }
}
