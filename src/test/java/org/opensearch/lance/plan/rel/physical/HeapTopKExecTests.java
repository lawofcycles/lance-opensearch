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
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The heap top-k operator alone: its convention, the row type with and
 * without the hit envelope, the page bound row estimate, constant
 * cost, copy and the explain terms that keep two different pages
 * apart.
 */
public class HeapTopKExecTests extends OpenSearchTestCase {

    private static LanceHitShape hitsPlan(String body) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        assertTrue("the fixture translates to a hits plan: " + logical, logical instanceof LanceHitShape);
        return (LanceHitShape) logical;
    }

    private static LanceTableScan scanBelow(RelNode node) {
        RelNode current = node;
        while (!(current instanceof LanceTableScan)) {
            current = current.getInput(0);
        }
        return (LanceTableScan) current;
    }

    private static HeapTopKExec exec(LanceHitShape hitShape) {
        LanceTopK topK = (LanceTopK) hitShape.getInput();
        LanceTableScan scan = scanBelow(topK);
        return new HeapTopKExec(topK.getCluster(), topK.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, topK, hitShape);
    }

    private static HeapTopKExec bareExec(LanceHitShape hitShape) {
        LanceTopK topK = (LanceTopK) hitShape.getInput();
        LanceTableScan scan = scanBelow(topK);
        return new HeapTopKExec(topK.getCluster(), topK.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, topK, null);
    }

    public void testConventionAndInterface() throws IOException {
        HeapTopKExec exec = exec(hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}"));
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue(exec instanceof LuceneRel);
    }

    public void testRowTypeIsTheEnvelopesWithAHitShape() throws IOException {
        LanceHitShape hitShape = hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        HeapTopKExec exec = exec(hitShape);
        assertEquals(hitShape.getRowType(), exec.getRowType());
        assertSame(hitShape, exec.hitShape());
    }

    public void testRowTypeIsTheTopKsWithoutAHitShape() throws IOException {
        LanceHitShape hitShape = hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        HeapTopKExec exec = bareExec(hitShape);
        assertEquals(hitShape.getInput().getRowType(), exec.getRowType());
        assertNull(exec.hitShape());
    }

    public void testRowEstimateIsThePageBound() throws IOException {
        // The fixture table reports 512 rows; a page of 5 keeps 5.
        HeapTopKExec exec = exec(hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}"));
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        assertEquals(5.0, exec.estimateRowCount(mq), 0.0);
    }

    public void testRowEstimateIsTheInputWhenSmaller() throws IOException {
        HeapTopKExec exec = exec(hitsPlan("{\"size\":10000,\"sort\":[{\"rating\":\"asc\"}]}"));
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        assertEquals(512.0, exec.estimateRowCount(mq), 0.0);
    }

    public void testCostIsTheConstant() throws IOException {
        HeapTopKExec exec = exec(hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}"));
        RelOptPlanner planner = exec.getCluster().getPlanner();
        RelOptCost cost = exec.computeSelfCost(planner, exec.getCluster().getMetadataQuery());
        assertTrue(cost.equals(planner.getCostFactory().makeTinyCost().plus(planner.getCostFactory().makeCost(1, 1, 1))));
    }

    public void testCostStaysAboveTheHandoff() throws IOException {
        // The deterministic preference for the Lance form rests on this
        // inequality: the handoff over a pushed scan must always cost
        // less than the exec's constant when the trees below are equal.
        HeapTopKExec exec = exec(hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}"));
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

    public void testCopyBindsANewInputAndKeepsThePage() throws IOException {
        LanceHitShape hitShape = hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}]}");
        HeapTopKExec exec = exec(hitShape);
        HeapTopKExec copy = (HeapTopKExec) exec.copy(exec.getTraitSet(), List.of(exec.getInput()));
        assertSame(exec.topK(), copy.topK());
        assertSame(hitShape, copy.hitShape());
        assertEquals(exec.getRowType(), copy.getRowType());
    }

    public void testExplainCarriesThePageAndTheEnvelope() throws IOException {
        HeapTopKExec exec = exec(hitsPlan("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}],\"search_after\":[100]}"));
        String plan = RelOptUtil.toString(exec);
        assertTrue("names the operator: " + plan, plan.contains("HeapTopKExec"));
        assertTrue("carries the page size: " + plan, plan.contains("fetch=[5]"));
        assertTrue("carries the cursor: " + plan, plan.contains("searchAfter="));
        assertTrue("carries the envelope: " + plan, plan.contains("source=[true]"));
        assertTrue("the scan is the input: " + plan, plan.contains("LanceTableScan"));
    }

    public void testExplainKeepsDifferentQueriesApart() throws IOException {
        // Same page over different filters: the wrapped tree's
        // predicate in the explain terms is what keeps the digests
        // distinct.
        HeapTopKExec onC0 = exec(hitsPlan("{\"size\":5,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"asc\"}]}"));
        HeapTopKExec onC1 = exec(hitsPlan("{\"size\":5,\"query\":{\"term\":{\"category\":\"c1\"}},\"sort\":[{\"rating\":\"asc\"}]}"));
        String first = RelOptUtil.toString(onC0);
        assertTrue("carries the filter predicate: " + first, first.contains("filter="));
        assertFalse(first.equals(RelOptUtil.toString(onC1)));
    }

    public void testExplainCarriesTheFtsOfTheWrappedTree() throws IOException {
        HeapTopKExec exec = exec(hitsPlan("{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"));
        String plan = RelOptUtil.toString(exec);
        assertTrue("carries the FTS clause: " + plan, plan.contains("fts="));
    }
}
