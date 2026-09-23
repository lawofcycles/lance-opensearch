/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The fan-out operator alone: its convention, row type, the width and
 * partitioning it carries, the cost that scales with the width, copy
 * and the explain terms that keep two different fan-outs apart.
 */
public class FanOutExecTests extends OpenSearchTestCase {

    private static RelNode tree() throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
    }

    private static FanOutExec exec(RelNode input, int fanOut) {
        return new FanOutExec(
            input.getCluster(),
            input.getCluster().traitSetOf(LuceneConvention.INSTANCE),
            input,
            fanOut,
            FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS
        );
    }

    public void testConventionAndInterface() throws IOException {
        FanOutExec exec = exec(tree(), 3);
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue(exec instanceof LuceneRel);
    }

    public void testLogicalFormCarriesNoConvention() throws IOException {
        RelNode input = tree();
        FanOutExec logical = new FanOutExec(
            input.getCluster(),
            input.getCluster().traitSetOf(Convention.NONE),
            input,
            3,
            FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS
        );
        assertSame(Convention.NONE, logical.getConvention());
    }

    public void testRowTypeIsTheInputs() throws IOException {
        RelNode input = tree();
        FanOutExec exec = exec(input, 3);
        assertEquals(input.getRowType(), exec.getRowType());
    }

    public void testWidthAndPartitioning() throws IOException {
        FanOutExec exec = exec(tree(), 5);
        assertEquals(5, exec.fanOut());
        assertSame(FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS, exec.partitioning());
    }

    public void testCostScalesWithTheWidth() throws IOException {
        RelNode input = tree();
        FanOutExec narrow = exec(input, 1);
        FanOutExec wide = exec(input, 4);
        RelOptPlanner planner = input.getCluster().getPlanner();
        RelOptCost narrowCost = narrow.computeSelfCost(planner, input.getCluster().getMetadataQuery());
        RelOptCost wideCost = wide.computeSelfCost(planner, input.getCluster().getMetadataQuery());
        assertTrue(narrowCost.equals(planner.getCostFactory().makeTinyCost()));
        assertTrue(wideCost.equals(planner.getCostFactory().makeTinyCost().multiplyBy(4)));
        assertTrue(narrowCost.isLt(wideCost));
    }

    public void testZeroWidthCostsLikeOne() throws IOException {
        // An empty table fans out to no node; the cost floor keeps the
        // plan comparable instead of free.
        RelNode input = tree();
        FanOutExec exec = exec(input, 0);
        RelOptPlanner planner = input.getCluster().getPlanner();
        assertTrue(exec.computeSelfCost(planner, input.getCluster().getMetadataQuery()).equals(planner.getCostFactory().makeTinyCost()));
    }

    public void testCopyKeepsTheWidthAndPartitioning() throws IOException {
        FanOutExec exec = exec(tree(), 3);
        FanOutExec copy = (FanOutExec) exec.copy(exec.getTraitSet(), List.of(exec.getInput()));
        assertEquals(3, copy.fanOut());
        assertSame(FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS, copy.partitioning());
        assertSame(exec.getInput(), copy.getInput());
        assertEquals(exec.getRowType(), copy.getRowType());
    }

    public void testExplainCarriesTheWidthAndPartitioning() throws IOException {
        String plan = RelOptUtil.toString(exec(tree(), 3));
        assertTrue("names the operator: " + plan, plan.contains("FanOutExec"));
        assertTrue("carries the width: " + plan, plan.contains("fanOut=[3]"));
        assertTrue("carries the partitioning: " + plan, plan.contains("partitioning=[EQUAL_FRAGMENT_GROUPS]"));
    }

    public void testExplainKeepsDifferentWidthsApart() throws IOException {
        RelNode input = tree();
        assertFalse(RelOptUtil.toString(exec(input, 2)).equals(RelOptUtil.toString(exec(input, 3))));
    }
}
