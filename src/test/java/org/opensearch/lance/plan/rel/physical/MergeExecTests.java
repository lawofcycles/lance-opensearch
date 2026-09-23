/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.Convention;
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
 * The merge operator alone: its convention, row type, the reduce kind
 * it carries, the constant cost, copy and the explain terms that keep
 * two different reduces apart.
 */
public class MergeExecTests extends OpenSearchTestCase {

    private static RelNode tree() throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
    }

    private static FanOutExec fanOut(RelNode input) {
        return new FanOutExec(
            input.getCluster(),
            input.getCluster().traitSetOf(LuceneConvention.INSTANCE),
            input,
            3,
            FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS
        );
    }

    private static MergeExec exec(RelNode input, MergeExec.ReduceKind kind) {
        return new MergeExec(input.getCluster(), input.getCluster().traitSetOf(LuceneConvention.INSTANCE), input, kind);
    }

    public void testConventionAndInterface() throws IOException {
        MergeExec exec = exec(fanOut(tree()), MergeExec.ReduceKind.AGGREGATE_INTERNAL);
        assertSame(LuceneConvention.INSTANCE, exec.getConvention());
        assertTrue(exec instanceof LuceneRel);
    }

    public void testLogicalFormCarriesNoConvention() throws IOException {
        RelNode input = tree();
        MergeExec logical = new MergeExec(
            input.getCluster(),
            input.getCluster().traitSetOf(Convention.NONE),
            input,
            MergeExec.ReduceKind.COUNT_SUM
        );
        assertSame(Convention.NONE, logical.getConvention());
    }

    public void testRowTypeIsTheInputs() throws IOException {
        RelNode input = tree();
        FanOutExec fanOut = fanOut(input);
        MergeExec exec = exec(fanOut, MergeExec.ReduceKind.AGGREGATE_INTERNAL);
        assertEquals(fanOut.getRowType(), exec.getRowType());
        assertEquals(input.getRowType(), exec.getRowType());
    }

    public void testReduceKind() throws IOException {
        FanOutExec input = fanOut(tree());
        for (MergeExec.ReduceKind kind : MergeExec.ReduceKind.values()) {
            assertSame(kind, exec(input, kind).reduceKind());
        }
    }

    public void testCostIsTheConstant() throws IOException {
        RelNode input = tree();
        MergeExec exec = exec(fanOut(input), MergeExec.ReduceKind.HITS_TOP_K);
        RelOptPlanner planner = input.getCluster().getPlanner();
        assertTrue(exec.computeSelfCost(planner, input.getCluster().getMetadataQuery()).equals(planner.getCostFactory().makeTinyCost()));
    }

    public void testCopyKeepsTheReduceKind() throws IOException {
        MergeExec exec = exec(fanOut(tree()), MergeExec.ReduceKind.COUNT_SUM);
        MergeExec copy = (MergeExec) exec.copy(exec.getTraitSet(), List.of(exec.getInput()));
        assertSame(MergeExec.ReduceKind.COUNT_SUM, copy.reduceKind());
        assertSame(exec.getInput(), copy.getInput());
        assertEquals(exec.getRowType(), copy.getRowType());
    }

    public void testExplainCarriesTheReduceKind() throws IOException {
        String plan = RelOptUtil.toString(exec(fanOut(tree()), MergeExec.ReduceKind.AGGREGATE_INTERNAL));
        assertTrue("names the operator: " + plan, plan.contains("MergeExec"));
        assertTrue("carries the reduce kind: " + plan, plan.contains("reduce=[AGGREGATE_INTERNAL]"));
    }

    public void testExplainKeepsDifferentReducesApart() throws IOException {
        FanOutExec input = fanOut(tree());
        assertFalse(
            RelOptUtil.toString(exec(input, MergeExec.ReduceKind.HITS_TOP_K))
                .equals(RelOptUtil.toString(exec(input, MergeExec.ReduceKind.COUNT_SUM)))
        );
    }
}
