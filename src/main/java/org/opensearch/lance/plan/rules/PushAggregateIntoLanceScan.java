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
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@link LanceAggregate} into the {@link LanceTableScan}
 * below it: when the Substrait producer can encode the aggregate for
 * Lance's dataset scan, the whole aggregate is replaced by the scan
 * carrying the encoded bytes as a pushed operation. When the producer
 * returns empty (a function outside the Lance consumer's vocabulary),
 * the rule does not transform and the aggregate stays the plan's root.
 *
 * <p>Three operand shapes are registered: the aggregate directly over
 * the scan (metric only trees), over the projection that computes the
 * group key expressions (every bucket tree, because Calcite requires
 * group keys to be input fields), and over a {@code Filter} over the
 * scan. A filter's condition never travels inside the aggregate bytes;
 * the Lance scan filter is passed separately through
 * {@code ScanOptions.filter}, as the producer documents.
 *
 * <p>The scan the rule produces carries a pushed aggregate and is not
 * a bare {@code LanceTableScan} under a {@code LanceAggregate}, so the
 * rule cannot match its own output; re-firing on the original operands
 * reproduces the same scan digest and registers nothing new, so the
 * planner terminates.
 */
public final class PushAggregateIntoLanceScan extends RelRule<PushAggregateIntoLanceScan.Config> {

    private PushAggregateIntoLanceScan(Config config) {
        super(config);
    }

    /** The three rules to register, one per operand shape. */
    public static List<PushAggregateIntoLanceScan> rules() {
        return List.of(Config.DIRECT.toRule(), Config.PROJECT.toRule(), Config.FILTER.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceAggregate aggregate = call.rel(0);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        if (!scan.pushedOperations().isEmpty()) {
            return;
        }
        // Rebuild the matched chain with concrete inputs: under the
        // Volcano planner the matched rels hold RelSubset children, and
        // both the producer and the executor's percentiles bin scans
        // read the tree structurally.
        RelNode input = scan;
        for (int i = call.rels.length - 2; i >= 1; i--) {
            RelNode middle = call.rel(i);
            input = middle.copy(middle.getTraitSet(), List.of(input));
        }
        LanceAggregate rebuilt = aggregate.withInput(input);
        Optional<ByteBuffer> bytes = LanceSubstraitProducer.toLanceAggregate(rebuilt);
        if (bytes.isEmpty()) {
            return;
        }
        call.transformTo(scan.withPushedAggregate(rebuilt, bytes.get()));
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair. Calcite generates its own configs with Immutables;
     * this project carries no annotation processor, so the three
     * instances are spelled directly.
     */
    public static final class Config implements RelRule.Config {

        /** The aggregate directly over the scan. */
        public static final Config DIRECT = new Config(
            "PushAggregateIntoLanceScan",
            b0 -> b0.operand(LanceAggregate.class).oneInput(b1 -> b1.operand(LanceTableScan.class).noInputs())
        );

        /** The aggregate over the projection of the group key expressions. */
        public static final Config PROJECT = new Config(
            "PushAggregateIntoLanceScan(Project)",
            b0 -> b0.operand(LanceAggregate.class)
                .oneInput(b1 -> b1.operand(Project.class).oneInput(b2 -> b2.operand(LanceTableScan.class).noInputs()))
        );

        /** The aggregate over a filter over the scan. */
        public static final Config FILTER = new Config(
            "PushAggregateIntoLanceScan(Filter)",
            b0 -> b0.operand(LanceAggregate.class)
                .oneInput(b1 -> b1.operand(Filter.class).oneInput(b2 -> b2.operand(LanceTableScan.class).noInputs()))
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
        public PushAggregateIntoLanceScan toRule() {
            return new PushAggregateIntoLanceScan(this);
        }
    }
}
