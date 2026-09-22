/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;

import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@code Filter} directly above a {@link LanceTableScan} into
 * the scan, so the predicate runs inside the Lance dataset scan as a
 * SQL filter instead of as a relational operator. Fires only when
 * {@link RexToLanceSql} can spell the whole predicate; otherwise the
 * {@code Filter} stays in place for the Lucene side.
 *
 * <p>Two operand shapes are registered. The plain {@code Filter(scan)}
 * shape rewrites the filter's own equivalence set; the
 * {@code Project(Filter(scan))} shape additionally copies the project
 * over the pushed scan, because the Volcano planner matches a rule's
 * child operands against the trait subset the parent registered with
 * and the pushed scan carries the Lance convention.
 *
 * <p>{@code LanceAggregate(Filter(scan))} is not a shape here.
 * {@link PushAggregateIntoLanceScan} already matches that tree,
 * rebuilds the concrete input (including the filter) into the pushed
 * aggregate, and produces the LANCE convention scan at the root, so
 * the aggregate path continues to absorb the filter as it did on main.
 * A filter without an aggregate above it becomes a
 * {@link org.opensearch.lance.plan.rel.PushedOperation.PushedFilter}
 * on the scan and shows up in the explain output.
 *
 * <p>The rules terminate: every shape matches only a scan with nothing
 * pushed, and every rewrite has the filter inside the scan.
 */
public final class PushFilterIntoLanceScan extends RelRule<PushFilterIntoLanceScan.Config> {

    private PushFilterIntoLanceScan(Config config) {
        super(config);
    }

    /** The two rules to register, one per operand shape. */
    public static List<PushFilterIntoLanceScan> rules() {
        return List.of(Config.DIRECT.toRule(), Config.PROJECT.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(call.rels.length - 2);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
        if (sql.isEmpty()) {
            return;
        }
        LanceTableScan pushed = scan.withPushedFilter(filter.getCondition(), sql.get());
        if (call.rels.length == 2) {
            call.transformTo(pushed);
            return;
        }
        RelNode parent = call.rel(0);
        call.transformTo(parent.copy(parent.getTraitSet(), List.of(pushed)));
    }

    private static RelRule.Done bareScan(RelRule.OperandBuilder builder) {
        return builder.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs();
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link PushAggregateIntoLanceScan.Config} because this project
     * carries no annotation processor.
     */
    public static final class Config implements RelRule.Config {

        /** The filter directly over a scan with nothing pushed. */
        public static final Config DIRECT = new Config(
            "PushFilterIntoLanceScan",
            b0 -> b0.operand(Filter.class).oneInput(PushFilterIntoLanceScan::bareScan)
        );

        /** A projection over the filter over the scan, rewritten together so the project's set gains the pushed form. */
        public static final Config PROJECT = new Config(
            "PushFilterIntoLanceScan(Project)",
            b0 -> b0.operand(Project.class).oneInput(b1 -> b1.operand(Filter.class).oneInput(PushFilterIntoLanceScan::bareScan))
        );

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
            throw new UnsupportedOperationException("the pushdown rule builds no rels through a RelBuilder");
        }

        @Override
        public PushFilterIntoLanceScan toRule() {
            return new PushFilterIntoLanceScan(this);
        }
    }
}
