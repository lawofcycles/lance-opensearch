/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.calcite.ShardPathConvention;
import org.opensearch.lance.plan.rel.LanceShardPathShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec;

import java.util.List;

/**
 * Lowers the logical {@link LanceShardPathShape} to the
 * {@link ShardPathConvention} physical operator, so the shard path
 * fallback (a request carrying {@code suggest}, {@code highlight} or a
 * pipeline aggregation, the
 * elements only the standard shard search path serves) is a
 * plan the Volcano planner produces instead of a hand written allow
 * list inside the dispatch filter.
 *
 * <p>No rule lowers the shape to {@code LanceConvention} or
 * {@code LuceneConvention}, so a tree that carries it has exactly one
 * physical form and the planner's answer is the routing decision. The
 * {@link ShardPathFallbackExec} cost stays pinned above the Lucene
 * operators' regardless, so a future plan where both forms exist keeps
 * preferring the fragment path.
 *
 * <p>The rule terminates: the operand requires the bare scan below the
 * shape, and the output is a {@link ShardPathConvention} node no
 * operand matches.
 */
public final class PlanToShardPathRule extends RelRule<PlanToShardPathRule.Config> {

    private PlanToShardPathRule(Config config) {
        super(config);
    }

    /** Every rule to register: the shape over the bare scan. */
    public static List<RelOptRule> rules() {
        return List.of(
            new Config(
                "PlanToShardPath(Shape)",
                b -> b.operand(LanceShardPathShape.class)
                    .oneInput(b1 -> b1.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs())
            ).toRule()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceShardPathShape shape = call.rel(0);
        LanceTableScan scan = call.rel(1);
        call.transformTo(
            new ShardPathFallbackExec(scan.getCluster(), scan.getCluster().traitSetOf(ShardPathConvention.INSTANCE), scan, shape.reasons())
        );
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link LanceToLuceneConverterRule.Config} because this project
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
            throw new UnsupportedOperationException("the shard path rule builds no rels through a RelBuilder");
        }

        @Override
        public PlanToShardPathRule toRule() {
            return new PlanToShardPathRule(this);
        }
    }
}
