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
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
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
 * <p>Four operand shapes are registered: the aggregate directly over
 * the scan (metric only trees), over the projection that computes the
 * group key expressions (every bucket tree, because Calcite requires
 * group keys to be input fields), over a {@code Filter} over the scan
 * (a metric only tree under a query filter), and over the projection
 * over the {@code Filter} (a bucket tree under a query filter, the
 * chain the request translator builds for {@code size: 0 + query +
 * buckets}). A filter's condition never travels inside the aggregate
 * bytes; {@link RexToLanceSql} prints it as Lance SQL and the pushed
 * aggregate carries that SQL for the executor's
 * {@code ScanOptions.filter}, the same split the fused FTS and knn
 * operations use for their prefilter. A filter the printer cannot
 * spell keeps the rule from transforming, so the tree's only physical
 * form is the {@code LuceneAggregateExec} alternative from
 * {@link LanceToLuceneConverterRule}, which evaluates the filter on the
 * Lucene side; dropping the filter or pushing the aggregate without it
 * would answer over the wrong rows.
 *
 * <p>The operands carry no shape predicate beyond what the translator
 * accepts: every {@link LanceAggregate} the translator builds is offered
 * to the producer, and whether the pushed form runs is the cost model's
 * decision. A tree with a {@code cardinality} metric is pushed like any
 * other and loses the cost comparison to the {@code LuceneAggregateExec}
 * alternative (see {@link LanceTableScan#computeSelfCost}), as do the
 * trees the routing settings forbid.
 *
 * <p>The scan the rule produces carries a pushed aggregate and is not
 * a bare {@code LanceTableScan} under a {@code LanceAggregate}, so the
 * rule cannot match its own output; re-firing on the original operands
 * reproduces the same scan digest and registers nothing new, so the
 * planner terminates. A scan another rule already pushed a filter into
 * is left alone as well: the filter belongs inside the aggregate.
 */
public final class PushAggregateIntoLanceScan extends RelRule<PushAggregateIntoLanceScan.Config> {

    private PushAggregateIntoLanceScan(Config config) {
        super(config);
    }

    /** The four rules to register, one per operand shape. */
    public static List<PushAggregateIntoLanceScan> rules() {
        return List.of(Config.DIRECT.toRule(), Config.PROJECT.toRule(), Config.FILTER.toRule(), Config.PROJECT_FILTER.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceAggregate aggregate = call.rel(0);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        if (!scan.pushedOperations().isEmpty()) {
            return;
        }
        // The query filter, when the chain carries one, sits directly
        // over the scan, so its condition is spelled over the scan's row
        // type. No SQL spelling means no push: the filter must run
        // somewhere, and the bytes cannot carry it.
        String filterSql = null;
        for (int i = 1; i < call.rels.length - 1; i++) {
            if (call.rel(i) instanceof Filter filter) {
                Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
                if (sql.isEmpty()) {
                    return;
                }
                filterSql = sql.get();
            }
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
        call.transformTo(scan.withPushedAggregate(rebuilt, bytes.get(), filterSql));
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair. Calcite generates its own configs with Immutables;
     * this project carries no annotation processor, so the four
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

        /** The aggregate over the projection of the group key expressions over a filter over the scan. */
        public static final Config PROJECT_FILTER = new Config(
            "PushAggregateIntoLanceScan(Project,Filter)",
            b0 -> b0.operand(LanceAggregate.class)
                .oneInput(
                    b1 -> b1.operand(Project.class)
                        .oneInput(b2 -> b2.operand(Filter.class).oneInput(b3 -> b3.operand(LanceTableScan.class).noInputs()))
                )
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
