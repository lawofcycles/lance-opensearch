/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;

import java.util.List;

/**
 * Hands the rows a {@link LanceConvention} subtree produced to the
 * Lucene side unchanged. The planner demands one convention at the
 * root; without this converter a plan the pushdown rules folded
 * entirely into the Lance scan could never satisfy a
 * {@link LuceneConvention} root and the planner would refuse exactly
 * the plans it should prefer. The node moves no rows and does no work,
 * so its cost is zero: a fully pushed scan plus this handoff always
 * costs less than the same tree behind {@link LuceneAggregateExec} or
 * {@link HeapTopKExec}, whose constant cost carries a deliberate
 * offset. {@code LancePlannerFactory.plan} unwraps the node from the
 * root before returning, so callers see the pushed scan itself.
 */
public final class LuceneHandoffExec extends ConverterImpl implements LuceneRel {

    /**
     * @param traits must carry {@link LuceneConvention#INSTANCE}
     * @param input the Lance convention subtree whose rows pass through
     */
    public LuceneHandoffExec(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LuceneHandoffExec(getCluster(), traitSet, sole(inputs));
    }

    /** Zero: the handoff moves no rows, so only the subtree below it counts. */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }
}
