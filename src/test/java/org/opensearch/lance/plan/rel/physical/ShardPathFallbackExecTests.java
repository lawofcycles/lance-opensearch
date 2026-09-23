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
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.ShardPathConvention;
import org.opensearch.lance.plan.calcite.ShardPathRel;
import org.opensearch.lance.plan.rel.LanceShardPathShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

/**
 * The shard path fallback operator alone: its convention, the response
 * envelope row type shared with the logical shape, the constant cost
 * pinned above the Lucene operators', copy, the explain terms that
 * keep two different reason sets apart, and the reason kinds the
 * translator can attach.
 */
public class ShardPathFallbackExecTests extends OpenSearchTestCase {

    private static LanceTableScan scan() {
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelBuilder builder = factory.relBuilder(PlanTestFixtures.model().schema());
        return (LanceTableScan) builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx").build();
    }

    private static ShardPathFallbackExec exec(ShardPathReason... reasons) {
        LanceTableScan scan = scan();
        return new ShardPathFallbackExec(
            scan.getCluster(),
            scan.getCluster().traitSetOf(ShardPathConvention.INSTANCE),
            scan,
            Arrays.asList(reasons)
        );
    }

    public void testConventionAndInterface() {
        ShardPathFallbackExec exec = exec(ShardPathReason.SUGGEST);
        assertSame(ShardPathConvention.INSTANCE, exec.getConvention());
        assertTrue(exec instanceof ShardPathRel);
    }

    public void testRowTypeIsTheResponseEnvelope() {
        ShardPathFallbackExec exec = exec(ShardPathReason.COLLAPSE);
        assertEquals(List.of("_hits", "_aggregations"), exec.getRowType().getFieldNames());
    }

    public void testRowTypeMatchesTheLogicalShape() {
        // The Volcano planner verifies type equivalence when the rule
        // replaces the logical shape with this operator, so the two
        // derivations must stay identical.
        LanceTableScan scan = scan();
        LanceShardPathShape shape = new LanceShardPathShape(scan.getCluster(), scan.getTraitSet(), scan, List.of(ShardPathReason.RESCORE));
        ShardPathFallbackExec exec = new ShardPathFallbackExec(
            scan.getCluster(),
            scan.getCluster().traitSetOf(ShardPathConvention.INSTANCE),
            scan,
            List.of(ShardPathReason.RESCORE)
        );
        assertEquals(shape.getRowType(), exec.getRowType());
    }

    public void testCostIsTheConstant() {
        ShardPathFallbackExec exec = exec(ShardPathReason.RESCORE);
        RelOptPlanner planner = exec.getCluster().getPlanner();
        RelOptCost cost = exec.computeSelfCost(planner, exec.getCluster().getMetadataQuery());
        assertTrue(cost.equals(planner.getCostFactory().makeTinyCost().plus(planner.getCostFactory().makeCost(100, 100, 100))));
    }

    public void testCostStaysAboveTheLuceneOperators() {
        // The preference order rests on these inequalities: the Lucene
        // fallback operators' constant and the zero cost handoff of a
        // pushed Lance scan must always cost less than the shard path,
        // so whenever another form of the same tree exists the planner
        // keeps the fragment path.
        ShardPathFallbackExec exec = exec(ShardPathReason.PIPELINE_AGG);
        RelOptPlanner planner = exec.getCluster().getPlanner();
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        RelOptCost luceneOperatorCost = planner.getCostFactory().makeTinyCost().plus(planner.getCostFactory().makeCost(1, 1, 1));
        assertTrue(luceneOperatorCost.isLt(exec.computeSelfCost(planner, mq)));
        LuceneHandoffExec handoff = new LuceneHandoffExec(
            exec.getCluster(),
            exec.getInput().getTraitSet().replace(LuceneConvention.INSTANCE),
            exec.getInput()
        );
        assertTrue(handoff.computeSelfCost(planner, mq).isLt(exec.computeSelfCost(planner, mq)));
    }

    public void testCopyBindsANewInputAndKeepsTheReasons() {
        ShardPathFallbackExec exec = exec(ShardPathReason.SUGGEST, ShardPathReason.HIGHLIGHT);
        ShardPathFallbackExec copy = (ShardPathFallbackExec) exec.copy(exec.getTraitSet(), List.of(exec.getInput()));
        assertEquals(exec.reasons(), copy.reasons());
        assertEquals(exec.getRowType(), copy.getRowType());
    }

    public void testExplainCarriesTheReasons() {
        ShardPathFallbackExec exec = exec(ShardPathReason.COLLAPSE, ShardPathReason.RESCORE);
        String plan = RelOptUtil.toString(exec);
        assertTrue("names the operator: " + plan, plan.contains("ShardPathFallbackExec"));
        assertTrue("carries the reasons: " + plan, plan.contains("COLLAPSE") && plan.contains("RESCORE"));
        assertTrue("the scan is the input: " + plan, plan.contains("LanceTableScan"));
    }

    public void testExplainKeepsDifferentReasonsApart() {
        assertFalse(RelOptUtil.toString(exec(ShardPathReason.SUGGEST)).equals(RelOptUtil.toString(exec(ShardPathReason.RESCORE))));
    }

    public void testEmptyReasonsAreRefused() {
        LanceTableScan scan = scan();
        expectThrows(
            IllegalArgumentException.class,
            () -> new ShardPathFallbackExec(scan.getCluster(), scan.getCluster().traitSetOf(ShardPathConvention.INSTANCE), scan, List.of())
        );
        expectThrows(IllegalArgumentException.class, () -> new LanceShardPathShape(scan.getCluster(), scan.getTraitSet(), scan, List.of()));
    }

    public void testReasonKinds() {
        // The kinds the translator can attach; the executor and the
        // explain output spell them by name, so renames are breaking.
        assertEquals(
            List.of("SUGGEST", "HIGHLIGHT", "COLLAPSE", "RESCORE", "PIPELINE_AGG"),
            Arrays.stream(ShardPathReason.values()).map(Enum::name).toList()
        );
    }

    public void testRowEstimatePassesTheInputThrough() {
        // The fixture table reports 512 rows and the operator does not
        // override the estimate.
        ShardPathFallbackExec exec = exec(ShardPathReason.SUGGEST);
        RelMetadataQuery mq = exec.getCluster().getMetadataQuery();
        assertEquals(512.0, exec.estimateRowCount(mq), 0.0);
    }

    public void testInputIsTheBareScan() {
        ShardPathFallbackExec exec = exec(ShardPathReason.SUGGEST);
        RelNode input = exec.getInput();
        assertTrue("the input is the scan: " + input, input instanceof LanceTableScan);
        assertTrue(((LanceTableScan) input).pushedOperations().isEmpty());
    }
}
