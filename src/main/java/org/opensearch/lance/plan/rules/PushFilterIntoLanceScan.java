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
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.substrait.LanceSubstraitFilterProducer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@code Filter} directly above a {@link LanceTableScan} into
 * the scan, so the predicate runs inside the Lance dataset scan instead
 * of as a relational operator. Two encodings of the pushed predicate
 * exist, each with its own rule ({@link Encoding}): the Lance SQL the
 * {@link RexToLanceSql} printer spells, handed to the scan's
 * {@code filter(sql)}, and the Substrait bytes
 * {@link LanceSubstraitFilterProducer} encodes, handed to
 * {@code substraitFilter(bytes)}. Both rules register the pushed scan
 * in the filter's equivalence set, so the Volcano planner keeps both
 * forms and the scan's cost ({@code CostModel.filterEncodingMillis})
 * picks one; a rule fires only when its encoder can spell the whole
 * predicate, and when neither can the {@code Filter} stays in place for
 * the Lucene side. The Substrait form carries the SQL as well when the
 * printer can spell it, because the executor's column loads take SQL
 * only.
 *
 * <p>Two operand shapes are registered per encoding. The plain
 * {@code Filter(scan)} shape rewrites the filter's own equivalence set;
 * the {@code Project(Filter(scan))} shape additionally copies the
 * project over the pushed scan, because the Volcano planner matches a
 * rule's child operands against the trait subset the parent registered
 * with and the pushed scan carries the Lance convention.
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

    /** The Lance encoding a rule pushes the predicate in. */
    public enum Encoding {
        /** The Lance SQL string of {@link RexToLanceSql}, for {@code ScanOptions.filter}. */
        SQL,
        /** The Substrait bytes of {@link LanceSubstraitFilterProducer}, for {@code ScanOptions.substraitFilter}. */
        SUBSTRAIT
    }

    private PushFilterIntoLanceScan(Config config) {
        super(config);
    }

    /** The four rules to register: one per encoding and operand shape. */
    public static List<PushFilterIntoLanceScan> rules() {
        List<PushFilterIntoLanceScan> rules = new ArrayList<>(4);
        for (Encoding encoding : Encoding.values()) {
            rules.addAll(rules(encoding));
        }
        return rules;
    }

    /** The two rules of one encoding, one per operand shape. */
    public static List<PushFilterIntoLanceScan> rules(Encoding encoding) {
        return List.of(Config.direct(encoding).toRule(), Config.project(encoding).toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(call.rels.length - 2);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
        LanceTableScan pushed;
        if (config.encoding() == Encoding.SQL) {
            if (sql.isEmpty()) {
                return;
            }
            pushed = scan.withPushedFilter(filter.getCondition(), sql.get());
        } else {
            Optional<ByteBuffer> bytes = LanceSubstraitFilterProducer.toLanceFilter(
                filter.getCondition(),
                scan.getRowType(),
                scan.getCluster().getTypeFactory()
            );
            if (bytes.isEmpty()) {
                return;
            }
            pushed = scan.withPushedFilter(filter.getCondition(), sql.orElse(null), bytes.get());
        }
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
     * The rule's configuration: an immutable description, encoding and
     * operand shape triple, spelled directly like
     * {@link PushAggregateIntoLanceScan.Config} because this project
     * carries no annotation processor.
     */
    public static final class Config implements RelRule.Config {

        /** The filter directly over a scan with nothing pushed. */
        static Config direct(Encoding encoding) {
            return new Config(
                "PushFilterIntoLanceScan(" + encoding + ")",
                encoding,
                b0 -> b0.operand(Filter.class).oneInput(PushFilterIntoLanceScan::bareScan)
            );
        }

        /** A projection over the filter over the scan, rewritten together so the project's set gains the pushed form. */
        static Config project(Encoding encoding) {
            return new Config(
                "PushFilterIntoLanceScan(" + encoding + ",Project)",
                encoding,
                b0 -> b0.operand(Project.class).oneInput(b1 -> b1.operand(Filter.class).oneInput(PushFilterIntoLanceScan::bareScan))
            );
        }

        private final String description;
        private final Encoding encoding;
        private final OperandTransform operandSupplier;

        private Config(String description, Encoding encoding, OperandTransform operandSupplier) {
            this.description = description;
            this.encoding = encoding;
            this.operandSupplier = operandSupplier;
        }

        /** The encoding this rule pushes the predicate in. */
        public Encoding encoding() {
            return encoding;
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
            return new Config(newDescription, encoding, operandSupplier);
        }

        @Override
        public Config withOperandSupplier(OperandTransform newOperandSupplier) {
            return new Config(description, encoding, newOperandSupplier);
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
