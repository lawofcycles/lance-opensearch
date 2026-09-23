/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * The Lucene aggregate operator alone: its convention, row type, row
 * estimate, constant cost, copy and the explain terms that keep two
 * different aggregates apart.
 */
public class LuceneAggregateExecTests extends OpenSearchTestCase {

    private static LanceAggregate aggregate(String body) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        assertTrue("the fixture translates to an aggregate: " + logical, logical instanceof LanceAggregate);
        return (LanceAggregate) logical;
    }

    private static LanceTableScan scanBelow(RelNode node) {
        RelNode current = node;
        while (!(current instanceof LanceTableScan)) {
            current = current.getInput(0);
        }
        return (LanceTableScan) current;
    }

    private static LuceneAggregateExec exec(LanceAggregate aggregate) {
        LanceTableScan scan = scanBelow(aggregate);
        return new LuceneAggregateExec(
            aggregate.getCluster(),
            aggregate.getCluster().traitSetOf(LuceneConvention.INSTANCE),
            scan,
            aggregate
        );
    }

    public void testConventionAndInterface() throws IOException {
        LuceneAggregateExec exec = exec(aggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue(exec instanceof LuceneRel);
    }

    public void testRowTypeIsTheAggregates() throws IOException {
        LanceAggregate aggregate = aggregate(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
        );
        LuceneAggregateExec exec = exec(aggregate);
        assertEquals(aggregate.getRowType(), exec.getRowType());
        assertSame(aggregate, exec.aggregate());
    }

    public void testRowEstimateIsTheAggregates() throws IOException {
        LanceAggregate aggregate = aggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        LuceneAggregateExec exec = exec(aggregate);
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        assertEquals(aggregate.estimateRowCount(mq), exec.estimateRowCount(mq), 0.0);
    }

    public void testCostIsTheConstant() throws IOException {
        LuceneAggregateExec exec = exec(aggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
        RelOptPlanner planner = exec.getCluster().getPlanner();
        RelOptCost cost = exec.computeSelfCost(planner, exec.getCluster().getMetadataQuery());
        assertTrue(cost.equals(planner.getCostFactory().makeTinyCost().plus(planner.getCostFactory().makeCost(1, 1, 1))));
    }

    public void testCostStaysAboveTheHandoff() throws IOException {
        // The deterministic preference for the Lance form rests on this
        // inequality: the handoff over a pushed scan must always cost
        // less than the exec's constant when the trees below are equal.
        LuceneAggregateExec exec = exec(aggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
        RelOptPlanner planner = exec.getCluster().getPlanner();
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        LuceneHandoffExec handoff = new LuceneHandoffExec(
            exec.getCluster(),
            exec.getInput().getTraitSet().replace(LuceneConvention.INSTANCE),
            exec.getInput()
        );
        RelOptCost handoffCost = handoff.computeSelfCost(planner, mq);
        RelOptCost execCost = exec.computeSelfCost(planner, mq);
        assertTrue(handoffCost.isLt(execCost));
    }

    public void testCopyBindsANewInputAndKeepsTheAggregate() throws IOException {
        LanceAggregate aggregate = aggregate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        LuceneAggregateExec exec = exec(aggregate);
        LuceneAggregateExec copy = (LuceneAggregateExec) exec.copy(exec.getTraitSet(), java.util.List.of(exec.getInput()));
        assertSame(aggregate, copy.aggregate());
        assertEquals(exec.getRowType(), copy.getRowType());
        assertSame(exec.getInput(), copy.getInput());
    }

    public void testExplainCarriesTheShape() throws IOException {
        LuceneAggregateExec exec = exec(
            aggregate(
                "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
            )
        );
        String plan = RelOptUtil.toString(exec);
        assertTrue("names the operator: " + plan, plan.contains("LuceneAggregateExec"));
        assertTrue("carries the bucket spec: " + plan, plan.contains("TERMS{name=by"));
        assertTrue("carries the metric spec: " + plan, plan.contains("AVG{name=a}"));
        assertTrue("the scan is the input: " + plan, plan.contains("LanceTableScan"));
    }

    public void testExplainKeepsDifferentAggregatesApart() throws IOException {
        // Same aggregation name over different columns: the calls in
        // the explain terms are what keeps the digests distinct.
        LuceneAggregateExec onRating = exec(aggregate("{\"size\":0,\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}"));
        LuceneAggregateExec onPrice = exec(aggregate("{\"size\":0,\"aggs\":{\"a\":{\"avg\":{\"field\":\"price\"}}}}"));
        assertFalse(RelOptUtil.toString(onRating).equals(RelOptUtil.toString(onPrice)));
    }

    public void testExplainCarriesTheFilterOfTheWrappedTree() throws IOException {
        LuceneAggregateExec exec = exec(
            aggregate("{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")
        );
        String plan = RelOptUtil.toString(exec);
        assertTrue("carries the filter predicate: " + plan, plan.contains("filter="));
    }
}
