/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.substrait.RexToLanceSql;

import java.util.List;
import java.util.Optional;

/**
 * Fuses a {@link LanceFtsMatch} with the scan below it, so the Lance
 * dataset scan runs the inverted-index lookup itself: the scan gains a
 * {@link org.opensearch.lance.plan.rel.PushedOperation.PushedFts}
 * holding the FTS node and, for the
 * {@code LanceFtsMatch(Filter(scan))} shape, the filter's predicate
 * spelled as Lance SQL, which the executor hands to the scan as a
 * prefilter evaluated before the posting-list lookup.
 *
 * <p>The filtered shape fires only when {@link RexToLanceSql} can
 * spell the whole predicate; otherwise the {@code Filter} stays in
 * place and the plan keeps the logical {@code LanceFtsMatch}, which
 * the executor answers through the Lucene composition (a later phase
 * turns the remaining filter into a Lucene side query).
 *
 * <p>The rules terminate: both shapes match only a scan with nothing
 * pushed, the output is a scan carrying the pushed FTS, and no operand
 * of either rule matches a scan.
 */
public final class FuseFtsWithFilter extends RelRule<FuseFtsWithFilter.Config> {

    private FuseFtsWithFilter(Config config) {
        super(config);
    }

    /** The two rules to register, one per operand shape. */
    public static List<FuseFtsWithFilter> rules() {
        return List.of(Config.FILTER.toRule(), Config.DIRECT.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceFtsMatch fts = call.rel(0);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        if (call.rels.length == 2) {
            call.transformTo(scan.withPushedFts(fts, null));
            return;
        }
        Filter filter = call.rel(1);
        Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
        if (sql.isEmpty()) {
            return;
        }
        call.transformTo(scan.withPushedFts(fts, sql.get()));
    }

    private static RelRule.Done bareScan(RelRule.OperandBuilder builder) {
        return builder.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs();
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link PushFilterIntoLanceScan.Config} because this project
     * carries no annotation processor.
     */
    public static final class Config implements RelRule.Config {

        /** The FTS node directly over a scan with nothing pushed. */
        public static final Config DIRECT = new Config(
            "FuseFtsWithFilter",
            b0 -> b0.operand(LanceFtsMatch.class).oneInput(FuseFtsWithFilter::bareScan)
        );

        /** The FTS node over a filter over the scan; the filter becomes the scan's SQL prefilter. */
        public static final Config FILTER = new Config(
            "FuseFtsWithFilter(Filter)",
            b0 -> b0.operand(LanceFtsMatch.class).oneInput(b1 -> b1.operand(Filter.class).oneInput(FuseFtsWithFilter::bareScan))
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
            throw new UnsupportedOperationException("the fuse rule builds no rels through a RelBuilder");
        }

        @Override
        public FuseFtsWithFilter toRule() {
            return new FuseFtsWithFilter(this);
        }
    }
}
