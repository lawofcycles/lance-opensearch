/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pushes a {@link LanceTopK} (and the {@link LanceHitShape} over it,
 * when the plan carries one) into the scan below, so the Lance dataset
 * scan returns the hits page already ordered and cut instead of every
 * matching row.
 *
 * <p>Like {@link PushAggregateIntoLanceScan}, the rule matches the
 * concrete logical chain between the top-k and the bare scan (nothing,
 * a {@code Filter}, a {@link LanceFtsMatch} / {@link LanceKnnSearch},
 * or one of those over a {@code Filter}) and folds the whole chain in
 * one call, because a scan another rule pushed lives in a different
 * trait subset than the logical node the top-k's input points at and
 * the operands would never see it. The query folds exactly as its own
 * pushdown rules fold it: the filter as the scan's SQL filter (or the
 * FTS / knn prefilter), refusing when {@link RexToLanceSql} cannot
 * spell the predicate.
 *
 * <p>The top-k itself folds in one of two ways. A score collation
 * ({@code _score} descending, {@code _distance} ascending) requires
 * the chain to carry the FTS or knn whose scan produces that order by
 * itself, must be the only collation (a page mixing score and column
 * order needs Lucene's {@code TopFieldCollector}) and carries no
 * cursor. Column collations require a scalar chain (nothing or a
 * {@code Filter}), resolve through {@link SortResolution#toOrderings}
 * to the Lance {@code ColumnOrdering}s, and accept a single-collation
 * {@code search_after} cursor as the strict bound
 * {@link SortResolution#cursorPredicate} spells, printed to Lance SQL.
 * An empty collation list (a page without a {@code sort} clause) folds
 * over any chain: the page is then the scan's own order (score for
 * FTS / knn, row address otherwise), which is what the executor
 * returns for that shape today.
 *
 * <p>Whenever a collation, the cursor or the chain does not fit, the
 * rule does not transform: the {@link LanceTopK} stays in the plan and
 * the executor answers the shape through the Lucene collector. A
 * {@link LanceHitShape} carrying a {@code post_filter} never folds
 * either: the filter narrows the page after the query matched, so a
 * scan that cut the page first would come back short. The rules
 * terminate because the output scan carries pushed operations, and
 * every operand requires the bare scan.
 */
public final class PushSortLimitIntoLanceScan extends RelRule<PushSortLimitIntoLanceScan.Config> {

    private PushSortLimitIntoLanceScan(Config config) {
        super(config);
    }

    /** One rule per (hit shape present, query chain) pair. */
    public static List<PushSortLimitIntoLanceScan> rules() {
        List<List<Class<? extends RelNode>>> chains = List.of(
            List.of(),
            List.of(Filter.class),
            List.of(LanceFtsMatch.class),
            List.of(LanceFtsMatch.class, Filter.class),
            List.of(LanceKnnSearch.class),
            List.of(LanceKnnSearch.class, Filter.class)
        );
        List<PushSortLimitIntoLanceScan> rules = new ArrayList<>(chains.size() * 2);
        for (List<Class<? extends RelNode>> chain : chains) {
            rules.add(new Config(description(true, chain), b -> hitShapeOperand(b, chain)).toRule());
            rules.add(new Config(description(false, chain), b -> topKOperand(b, chain)).toRule());
        }
        return rules;
    }

    private static String description(boolean hitShape, List<Class<? extends RelNode>> chain) {
        StringBuilder sb = new StringBuilder("PushSortLimitIntoLanceScan(");
        if (hitShape) {
            sb.append("HitShape");
        }
        for (Class<? extends RelNode> node : chain) {
            if (sb.charAt(sb.length() - 1) != '(') {
                sb.append(',');
            }
            sb.append(node.getSimpleName());
        }
        return sb.append(')').toString();
    }

    private static RelRule.Done hitShapeOperand(RelRule.OperandBuilder builder, List<Class<? extends RelNode>> chain) {
        return builder.operand(LanceHitShape.class).oneInput(b -> topKOperand(b, chain));
    }

    private static RelRule.Done topKOperand(RelRule.OperandBuilder builder, List<Class<? extends RelNode>> chain) {
        return builder.operand(LanceTopK.class).oneInput(b -> chainOperand(b, chain));
    }

    private static RelRule.Done chainOperand(RelRule.OperandBuilder builder, List<Class<? extends RelNode>> chain) {
        if (chain.isEmpty()) {
            return builder.operand(LanceTableScan.class).predicate(scan -> scan.pushedOperations().isEmpty()).noInputs();
        }
        return builder.operand(chain.get(0)).oneInput(b -> chainOperand(b, chain.subList(1, chain.size())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        int next = 0;
        LanceHitShape hitShape = call.rel(0) instanceof LanceHitShape shape ? shape : null;
        if (hitShape != null) {
            if (hitShape.postFilter() != null) {
                // The post filter narrows the page after the query
                // matched; a scan that cut the page first would return
                // fewer than the requested rows. Lucene's collector
                // applies the conjunction and cuts afterwards.
                return;
            }
            next = 1;
        }
        LanceTopK topK = call.rel(next);
        LanceTableScan scan = call.rel(call.rels.length - 1);
        Filter filter = null;
        LanceFtsMatch fts = null;
        LanceKnnSearch knn = null;
        for (int i = next + 1; i < call.rels.length - 1; i++) {
            RelNode middle = call.rel(i);
            if (middle instanceof Filter matched) {
                filter = matched;
            } else if (middle instanceof LanceFtsMatch matched) {
                fts = matched;
            } else if (middle instanceof LanceKnnSearch matched) {
                knn = matched;
            }
        }
        String filterSql = null;
        if (filter != null) {
            Optional<String> sql = RexToLanceSql.print(filter.getCondition(), scan.getRowType());
            if (sql.isEmpty()) {
                return;
            }
            filterSql = sql.get();
        }
        LanceTableScan pushedQuery;
        if (fts != null) {
            pushedQuery = scan.withPushedFts(fts, filterSql);
        } else if (knn != null) {
            pushedQuery = scan.withPushedKnn(knn, filterSql);
        } else if (filter != null) {
            pushedQuery = scan.withPushedFilter(filter.getCondition(), filterSql);
        } else {
            pushedQuery = scan;
        }
        List<RelFieldCollation> collations = topK.collations();
        if (collations.isEmpty()) {
            // A page without a sort clause: the scan's own order (score
            // for FTS / knn, row address otherwise) is the page order.
            if (topK.searchAfter() != null) {
                return;
            }
            call.transformTo(pushedQuery.withPushedTopK(topK, hitShape, List.of(), null));
            return;
        }
        boolean score = false;
        for (RelFieldCollation collation : collations) {
            score |= SortResolution.isScoreCollation(pushedQuery.getRowType(), collation);
        }
        if (score) {
            // The FTS / knn scan produces the score order by itself; a
            // second collation or a cursor would need a comparator.
            if (collations.size() != 1 || topK.searchAfter() != null) {
                return;
            }
            String name = pushedQuery.getRowType().getFieldNames().get(collations.get(0).getFieldIndex());
            boolean served = LanceFtsMatch.SCORE_FIELD.equals(name) ? fts != null : knn != null;
            if (!served) {
                return;
            }
            call.transformTo(pushedQuery.withPushedTopK(topK, hitShape, List.of(), null));
            return;
        }
        // Column collations order rows the FTS / knn scans do not
        // return that way; only a scalar page (bare scan or pushed
        // filter) reproduces Lucene's order from a Lance ordering.
        if (fts != null || knn != null) {
            return;
        }
        LanceTable table = scan.getTable().unwrap(LanceTable.class);
        if (table == null) {
            return;
        }
        Schema arrowSchema = table.arrowSchema();
        Optional<List<org.lance.ipc.ColumnOrdering>> orderings = SortResolution.toOrderings(
            pushedQuery.getRowType(),
            collations,
            arrowSchema
        );
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
                pushedQuery.getRowType(),
                collations.get(0),
                arrowSchema,
                topK.searchAfter().get(0)
            );
            if (predicate.isEmpty()) {
                return;
            }
            Optional<String> sql = RexToLanceSql.print(predicate.get(), pushedQuery.getRowType());
            if (sql.isEmpty()) {
                return;
            }
            cursorSql = sql.get();
        }
        call.transformTo(pushedQuery.withPushedTopK(topK, hitShape, orderings.get(), cursorSql));
    }

    /**
     * The rule's configuration: an immutable description and operand
     * shape pair, spelled directly like
     * {@link PushFilterIntoLanceScan.Config} because this project
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
            throw new UnsupportedOperationException("the top-k rule builds no rels through a RelBuilder");
        }

        @Override
        public PushSortLimitIntoLanceScan toRule() {
            return new PushSortLimitIntoLanceScan(this);
        }
    }
}
