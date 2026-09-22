/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.substrait.RexToLanceSql;

import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@code Filter} directly above a {@link LanceTableScan} into
 * the scan, so the predicate runs inside the Lance dataset scan as a
 * SQL filter instead of as a relational operator. Fires only when
 * {@link RexToLanceSql} can spell the whole predicate; otherwise the
 * {@code Filter} stays in place for the Lucene side.
 *
 * <p>The aggregate pushdown composes with this rule through the
 * planner's memo: the filtered scan joins the {@code Filter}'s
 * equivalence set, so {@link PushAggregateIntoLanceScan}'s scan operand
 * also binds it and the aggregate lands on the scan that already
 * carries the filter.
 *
 * <p>The rule terminates: it only matches a scan with no pushed filter,
 * and its rewrite has the filter inside the scan.
 */
public final class PushFilterIntoLanceScan extends RelRule<PushFilterIntoLanceScan.Config> {

    private PushFilterIntoLanceScan(Config config) {
        super(config);
    }

    /** The rules to register; one shape today, the filter directly over the scan. */
    public static List<PushFilterIntoLanceScan> rules() {
        return List.of(Config.DIRECT.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LanceTableScan scan = call.rel(1);
        Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
        if (sql.isEmpty()) {
            return;
        }
        call.transformTo(scan.withPushedFilter(filter.getCondition(), sql.get()));
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
            b0 -> b0.operand(Filter.class)
                .oneInput(b1 -> b1.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs())
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
