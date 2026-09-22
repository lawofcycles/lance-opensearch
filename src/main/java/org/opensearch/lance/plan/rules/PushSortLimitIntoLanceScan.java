/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.PushedOperation;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFts;
import org.opensearch.lance.plan.rel.PushedOperation.PushedKnn;

import org.lance.ipc.ColumnOrdering;

import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@link LanceTopK} (and the {@link LanceHitShape} over it,
 * when the plan carries one) into the scan below, so the Lance dataset
 * scan returns the hits page already ordered and cut instead of every
 * matching row.
 *
 * <p>A collation folds in one of two ways. A score collation
 * ({@code _score} descending, {@code _distance} ascending) requires
 * the scan to carry the pushed FTS or knn whose scan produces that
 * order by itself, must be the only collation (a page mixing score
 * and column order needs Lucene's {@code TopFieldCollector}) and
 * carries no cursor. Column collations require a scan that is bare or
 * carries a single pushed filter (the query the page cuts), resolve
 * through {@link SortResolution#toOrderings} to the Lance
 * {@code ColumnOrdering}s, and accept a single-collation
 * {@code search_after} cursor as the strict bound
 * {@link SortResolution#cursorPredicate} spells, printed to Lance SQL.
 * An empty collation list (a page without a {@code sort} clause) folds
 * over any compatible scan: the page is then the scan's own order
 * (score for FTS / knn, row address otherwise), which is what the
 * executor returns for that shape today.
 *
 * <p>Whenever a collation, the cursor or the scan does not fit, the
 * rule does not transform: the {@link LanceTopK} stays in the plan and
 * the executor answers the shape through the Lucene collector. The
 * rules terminate because the output scan carries a pushed top-k,
 * which no operand accepts.
 */
public final class PushSortLimitIntoLanceScan extends RelRule<PushSortLimitIntoLanceScan.Config> {

    private PushSortLimitIntoLanceScan(Config config) {
        super(config);
    }

    /** The two rules to register, one per operand shape. */
    public static List<PushSortLimitIntoLanceScan> rules() {
        return List.of(Config.DIRECT.toRule(), Config.HIT_SHAPE.toRule());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LanceHitShape hitShape = call.rels.length == 3 ? call.rel(0) : null;
        LanceTopK topK = call.rel(call.rels.length - 2);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        List<RelFieldCollation> collations = topK.collations();
        if (collations.isEmpty()) {
            // A page without a sort clause: the scan's own order (score
            // for FTS / knn, row address otherwise) is the page order.
            if (topK.searchAfter() != null) {
                return;
            }
            call.transformTo(scan.withPushedTopK(topK, hitShape, List.of(), null));
            return;
        }
        boolean score = false;
        for (RelFieldCollation collation : collations) {
            score |= SortResolution.isScoreCollation(scan.getRowType(), collation);
        }
        if (score) {
            // The FTS / knn scan produces the score order by itself; a
            // second collation or a cursor would need a comparator.
            if (collations.size() != 1 || topK.searchAfter() != null) {
                return;
            }
            String name = scan.getRowType().getFieldNames().get(collations.get(0).getFieldIndex());
            boolean served = LanceFtsMatch.SCORE_FIELD.equals(name) ? scan.pushedFts().isPresent() : scan.pushedKnn().isPresent();
            if (!served) {
                return;
            }
            call.transformTo(scan.withPushedTopK(topK, hitShape, List.of(), null));
            return;
        }
        // Column collations order rows the FTS / knn scans do not
        // return that way; only a scalar page (bare scan or pushed
        // filter) reproduces Lucene's order from a Lance ordering.
        if (scan.pushedFts().isPresent() || scan.pushedKnn().isPresent()) {
            return;
        }
        LanceTable table = scan.getTable().unwrap(LanceTable.class);
        if (table == null) {
            return;
        }
        Schema arrowSchema = table.arrowSchema();
        Optional<List<ColumnOrdering>> orderings = SortResolution.toOrderings(scan.getRowType(), collations, arrowSchema);
        if (orderings.isEmpty()) {
            return;
        }
        String cursorSql = null;
        if (topK.searchAfter() != null) {
            if (collations.size() != 1 || topK.searchAfter().size() != 1 || topK.searchAfter().get(0) == null) {
                return;
            }
            Optional<RexNode> predicate = SortResolution.cursorPredicate(
                scan.getCluster().getRexBuilder(),
                scan.getRowType(),
                collations.get(0),
                arrowSchema,
                topK.searchAfter().get(0)
            );
            if (predicate.isEmpty()) {
                return;
            }
            Optional<String> sql = RexToLanceSql.print(predicate.get(), scan.getRowType());
            if (sql.isEmpty()) {
                return;
            }
            cursorSql = sql.get();
        }
        call.transformTo(scan.withPushedTopK(topK, hitShape, orderings.get(), cursorSql));
    }

    /** Whether a top-k may land on the scan: bare, or a single pushed filter / FTS / knn (the query the page cuts). */
    private static boolean topKCompatible(LanceTableScan scan) {
        List<PushedOperation> pushed = scan.pushedOperations();
        if (pushed.isEmpty()) {
            return true;
        }
        if (pushed.size() != 1) {
            return false;
        }
        PushedOperation operation = pushed.get(0);
        return operation instanceof PushedFilter || operation instanceof PushedFts || operation instanceof PushedKnn;
    }

    private static RelRule.Done compatibleScan(RelRule.OperandBuilder builder) {
        return builder.operand(LanceTableScan.class).predicate(PushSortLimitIntoLanceScan::topKCompatible).noInputs();
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link PushFilterIntoLanceScan.Config} because this project
     * carries no annotation processor.
     */
    public static final class Config implements RelRule.Config {

        /** The top-k directly over a compatible scan. */
        public static final Config DIRECT = new Config(
            "PushSortLimitIntoLanceScan",
            b0 -> b0.operand(LanceTopK.class).oneInput(PushSortLimitIntoLanceScan::compatibleScan)
        );

        /** The hit shape over the top-k over the scan; both fold, and the scan takes the hit shape's row type. */
        public static final Config HIT_SHAPE = new Config(
            "PushSortLimitIntoLanceScan(HitShape)",
            b0 -> b0.operand(LanceHitShape.class)
                .oneInput(b1 -> b1.operand(LanceTopK.class).oneInput(PushSortLimitIntoLanceScan::compatibleScan))
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
            throw new UnsupportedOperationException("the top-k rule builds no rels through a RelBuilder");
        }

        @Override
        public PushSortLimitIntoLanceScan toRule() {
            return new PushSortLimitIntoLanceScan(this);
        }
    }
}
