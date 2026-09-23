/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.rel.physical.LuceneHandoffExec;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers the logical aggregation and hits trees to the
 * {@link LuceneConvention} physical operators, so the Lucene
 * aggregator / collector fallback is a plan alternative the Volcano
 * planner costs against the Lance pushdown instead of a hand written
 * escape hatch outside its view: a {@link LanceAggregate} becomes a
 * {@link LuceneAggregateExec} and a {@link LanceTopK} (with the
 * {@link LanceHitShape} over it, when the plan carries one) becomes a
 * {@link HeapTopKExec}, each over the bare scan whose fragment readers
 * the Lucene machinery consumes.
 *
 * <p>Like {@link PushAggregateIntoLanceScan} and
 * {@link PushSortLimitIntoLanceScan}, every rule matches the concrete
 * logical chain between its root and the bare scan and folds the whole
 * chain in one call, because the chain nodes have no physical form of
 * their own in this convention (the wrapped logical nodes carry them
 * for the executor, exactly as the pushed operations do). The
 * aggregate chains mirror what the translator builds: nothing, the
 * group key projection, the query filter, and the projection over the
 * filter. The top-k chains mirror the top-k pushdown rule's: nothing,
 * a {@code Filter}, a {@link LanceFtsMatch} / {@link LanceKnnSearch},
 * or one of those over a {@code Filter}, each with and without the hit
 * envelope. A {@link LanceHitShape} never stands alone (the translator
 * only builds it over a {@link LanceTopK}), so no rule matches it
 * without the top-k below.
 *
 * <p>The rules terminate: every operand requires the bare scan, and
 * the output is a {@link LuceneConvention} node no operand matches.
 *
 * <p>{@link Handoff} is the third piece: the dedicated converter that
 * lets a plan the pushdown rules folded entirely into the Lance scan
 * satisfy the {@link LuceneConvention} the planner demands at the
 * root, at zero cost.
 */
public final class LanceToLuceneConverterRule extends RelRule<LanceToLuceneConverterRule.Config> {

    private LanceToLuceneConverterRule(Config config) {
        super(config);
    }

    /**
     * Every rule to register: the aggregate chains, the top-k chains
     * (with and without the hit envelope) and the {@link Handoff}
     * converter.
     */
    public static List<RelOptRule> rules() {
        List<List<Class<? extends RelNode>>> aggregateChains = List.of(
            List.of(),
            List.of(Project.class),
            List.of(Filter.class),
            List.of(Project.class, Filter.class)
        );
        List<List<Class<? extends RelNode>>> topKChains = List.of(
            List.of(),
            List.of(Filter.class),
            List.of(LanceFtsMatch.class),
            List.of(LanceFtsMatch.class, Filter.class),
            List.of(LanceKnnSearch.class),
            List.of(LanceKnnSearch.class, Filter.class)
        );
        List<RelOptRule> rules = new ArrayList<>(aggregateChains.size() + topKChains.size() * 2 + 1);
        for (List<Class<? extends RelNode>> chain : aggregateChains) {
            rules.add(new Config(description("Aggregate", chain), b -> rootOperand(b, LanceAggregate.class, chain)).toRule());
        }
        for (List<Class<? extends RelNode>> chain : topKChains) {
            rules.add(
                new Config(
                    description("HitShape", chain),
                    b -> b.operand(LanceHitShape.class).oneInput(b1 -> rootOperand(b1, LanceTopK.class, chain))
                ).toRule()
            );
            rules.add(new Config(description("TopK", chain), b -> rootOperand(b, LanceTopK.class, chain)).toRule());
        }
        rules.add(Handoff.INSTANCE);
        return rules;
    }

    private static String description(String root, List<Class<? extends RelNode>> chain) {
        StringBuilder sb = new StringBuilder("LanceToLucene(").append(root);
        for (Class<? extends RelNode> node : chain) {
            sb.append(',').append(node.getSimpleName());
        }
        return sb.append(')').toString();
    }

    private static RelRule.Done rootOperand(
        RelRule.OperandBuilder builder,
        Class<? extends RelNode> root,
        List<Class<? extends RelNode>> chain
    ) {
        return builder.operand(root).oneInput(b -> chainOperand(b, chain));
    }

    private static RelRule.Done chainOperand(RelRule.OperandBuilder builder, List<Class<? extends RelNode>> chain) {
        if (chain.isEmpty()) {
            return builder.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs();
        }
        return builder.operand(chain.get(0)).oneInput(b -> chainOperand(b, chain.subList(1, chain.size())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode root = call.rel(0);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        if (root instanceof LanceAggregate aggregate) {
            LanceAggregate rebuilt = aggregate.withInput(rebuildChain(call, 1));
            call.transformTo(
                new LuceneAggregateExec(scan.getCluster(), scan.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, rebuilt)
            );
            return;
        }
        LanceHitShape hitShape = root instanceof LanceHitShape shape ? shape : null;
        int next = hitShape == null ? 0 : 1;
        LanceTopK topK = call.rel(next);
        LanceTopK rebuilt = (LanceTopK) topK.copy(topK.getTraitSet(), List.of(rebuildChain(call, next + 1)));
        call.transformTo(
            new HeapTopKExec(scan.getCluster(), scan.getCluster().traitSetOf(LuceneConvention.INSTANCE), scan, rebuilt, hitShape)
        );
    }

    /**
     * The matched chain between {@code firstMiddle} and the scan,
     * rebuilt with concrete inputs: under the Volcano planner the
     * matched rels hold {@code RelSubset} children, and the wrapped
     * logical tree is read structurally by the digest and, later, by
     * the executor.
     */
    private static RelNode rebuildChain(RelOptRuleCall call, int firstMiddle) {
        RelNode input = call.rel(call.rels.length - 1);
        for (int i = call.rels.length - 2; i >= firstMiddle; i--) {
            RelNode middle = call.rel(i);
            input = middle.copy(middle.getTraitSet(), List.of(input));
        }
        return input;
    }

    /**
     * The dedicated converter from {@link LanceConvention} to
     * {@link LuceneConvention}: wraps any Lance convention rel in a
     * zero cost {@link LuceneHandoffExec}. The conventions answer
     * {@code useAbstractConvertersForConversion} with false, so this
     * rule is the only bridge between them.
     */
    public static final class Handoff extends ConverterRule {

        private static final ConverterRule.Config CONFIG = ConverterRule.Config.INSTANCE.withConversion(
            RelNode.class,
            LanceConvention.INSTANCE,
            LuceneConvention.INSTANCE,
            "LanceToLucene(Handoff)"
        ).withRuleFactory(Handoff::new);

        /** The single instance {@link #rules()} registers. */
        public static final Handoff INSTANCE = CONFIG.toRule(Handoff.class);

        private Handoff(ConverterRule.Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            return new LuceneHandoffExec(rel.getCluster(), rel.getTraitSet().replace(LuceneConvention.INSTANCE), rel);
        }
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link PushSortLimitIntoLanceScan.Config} because this project
     * carries no annotation processor.
     */
    public static final class Config implements RelRule.Config {

        private final String description;
        private final OperandTransform operandSupplier;

        private Config(String description, OperandTransform operandSupplier) {
            this.description = description;
            this.operandSupplier = operandSupplier;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public OperandTransform operandSupplier() {
            return operandSupplier;
        }

        @Override
        public Config withDescription(String newDescription) {
            return new Config(newDescription, operandSupplier);
        }

        @Override
        public Config withOperandSupplier(OperandTransform newOperandSupplier) {
            return new Config(description, newOperandSupplier);
        }

        @Override
        public Config withRelBuilderFactory(RelBuilderFactory factory) {
            throw new UnsupportedOperationException("the converter rule builds no rels through a RelBuilder");
        }

        @Override
        public LanceToLuceneConverterRule toRule() {
            return new LanceToLuceneConverterRule(this);
        }
    }
}
