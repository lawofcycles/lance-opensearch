/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;

import java.util.List;
import java.util.Optional;

/**
 * Fuses a {@link LanceKnnSearch} with the scan below it, so the Lance
 * dataset scan runs the nearest lookup itself: the scan gains a
 * {@link org.opensearch.lance.plan.rel.PushedOperation.PushedKnn}
 * holding the knn node and, for the
 * {@code LanceKnnSearch(Filter(scan))} shape, the inner filter's
 * predicate spelled as Lance SQL, which the executor hands to the scan
 * as a prefilter evaluated before the top-k cutoff (a post-filter
 * would drop rows from the k nearest and leave fewer than k results).
 *
 * <p>The filtered shape fires only when {@link RexToLanceSql} can
 * spell the whole predicate; otherwise the {@code Filter} stays over
 * the logical {@code LanceKnnSearch} and the executor refuses the
 * clause, keeping the contract that a {@code lance_knn} filter without
 * a Lance SQL form answers 400.
 *
 * <p>The rules terminate: both shapes match only a scan with nothing
 * pushed, the output is a scan carrying the pushed knn, and no operand
 * of either rule matches a scan.
 */
public final class FuseKnnWithFilter extends RelRule<FuseKnnWithFilter.Config> {

    private FuseKnnWithFilter(Config config) {
        super(config);
    }

    /** The two rules to register, one per operand shape. */
    public static List<FuseKnnWithFilter> rules() {
        return List.of(Config.FILTER.toRule(), Config.DIRECT.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceKnnSearch knn = call.rel(0);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        if (call.rels.length == 2) {
            call.transformTo(scan.withPushedKnn(knn, null));
            return;
        }
        Filter filter = call.rel(1);
        Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
        if (sql.isEmpty()) {
            return;
        }
        call.transformTo(scan.withPushedKnn(knn, sql.get()));
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

        /** The knn node directly over a scan with nothing pushed. */
        public static final Config DIRECT = new Config(
            "FuseKnnWithFilter",
            b0 -> b0.operand(LanceKnnSearch.class).oneInput(FuseKnnWithFilter::bareScan)
        );

        /** The knn node over a filter over the scan; the filter becomes the scan's SQL prefilter. */
        public static final Config FILTER = new Config(
            "FuseKnnWithFilter(Filter)",
            b0 -> b0.operand(LanceKnnSearch.class).oneInput(b1 -> b1.operand(Filter.class).oneInput(FuseKnnWithFilter::bareScan))
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
        public FuseKnnWithFilter toRule() {
            return new FuseKnnWithFilter(this);
        }
    }
}
