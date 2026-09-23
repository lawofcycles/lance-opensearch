/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;

import java.util.List;

/**
 * Lowers the coordinator layer nodes the translator wrapper builds in
 * {@code Convention.NONE} to their {@link LuceneConvention} form: a
 * trait change on the node itself with the input demanded in the same
 * convention, nothing else. The per-node subtree below the
 * {@link FanOutExec} then reaches the convention through the existing
 * rules: the pushdown rules plus the zero cost handoff for a tree that
 * folds into the Lance scan, or the {@code LanceToLuceneConverterRule}
 * chains for one that does not. The conversions are self terminating:
 * the output carries {@link LuceneConvention} and the rules only match
 * {@code Convention.NONE}.
 */
public final class CoordinatorLayerRules {

    private CoordinatorLayerRules() {}

    /** Both converters, for {@code LancePlannerFactory.newCluster} to register. */
    public static List<RelOptRule> rules() {
        return List.of(FanOut.INSTANCE, Merge.INSTANCE);
    }

    /** {@link FanOutExec} in {@code Convention.NONE} to the same node in {@link LuceneConvention}. */
    public static final class FanOut extends ConverterRule {

        private static final ConverterRule.Config CONFIG = ConverterRule.Config.INSTANCE.withConversion(
            FanOutExec.class,
            Convention.NONE,
            LuceneConvention.INSTANCE,
            "CoordinatorLayer(FanOut)"
        ).withRuleFactory(FanOut::new);

        static final FanOut INSTANCE = CONFIG.toRule(FanOut.class);

        private FanOut(ConverterRule.Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            FanOutExec fanOut = (FanOutExec) rel;
            RelNode input = convert(fanOut.getInput(), fanOut.getInput().getTraitSet().replace(LuceneConvention.INSTANCE));
            return fanOut.copy(fanOut.getTraitSet().replace(LuceneConvention.INSTANCE), List.of(input));
        }
    }

    /** {@link MergeExec} in {@code Convention.NONE} to the same node in {@link LuceneConvention}. */
    public static final class Merge extends ConverterRule {

        private static final ConverterRule.Config CONFIG = ConverterRule.Config.INSTANCE.withConversion(
            MergeExec.class,
            Convention.NONE,
            LuceneConvention.INSTANCE,
            "CoordinatorLayer(Merge)"
        ).withRuleFactory(Merge::new);

        static final Merge INSTANCE = CONFIG.toRule(Merge.class);

        private Merge(ConverterRule.Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            MergeExec merge = (MergeExec) rel;
            RelNode input = convert(merge.getInput(), merge.getInput().getTraitSet().replace(LuceneConvention.INSTANCE));
            return merge.copy(merge.getTraitSet().replace(LuceneConvention.INSTANCE), List.of(input));
        }
    }
}
