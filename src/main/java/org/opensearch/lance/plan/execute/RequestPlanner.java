/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.translate.QueryToRex;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;

import java.util.Objects;
import java.util.Set;

/**
 * The coordinator's planning of one target: the request is translated
 * once ({@link SearchRequestToRel#translateForExecution}), the Volcano
 * planner chooses the per node physical form, and that form is written
 * down as the {@link FragmentPlan} every fragment request of the target
 * carries. The fragment executors do not plan; they apply node local
 * guards to the shipped plan and run it.
 *
 * <p>Two shapes never reach the planner. A query that references an
 * {@code ip} or {@code geo_point} override column has no Lance SQL
 * form (their Lucene form, encoded doc values, differs from what the
 * column stores), and a query outside the translator's vocabulary
 * ({@code match}, {@code query_string}, an unmapped field, ...) has no
 * relational form; both execute through the Lucene composition of the
 * request's own builder under a plan whose kind the envelope selects.
 * A filtered {@code lance_knn} is the exception: its filter is a
 * prefilter by contract and silently dropping it would return wrong
 * nearest rows, so the request is refused with the 400 the executor
 * used to answer.
 */
public final class RequestPlanner {

    private static final Logger LOGGER = LogManager.getLogger(RequestPlanner.class);

    private RequestPlanner() {}

    /**
     * The outcome of planning one target: the plan the fragment
     * requests carry and the per node subtree the coordinator layer
     * wraps ({@link SearchRequestToRel#withCoordinatorLayer}).
     */
    public record Planned(FragmentPlan plan, RelNode perNode) {

        public Planned {
            Objects.requireNonNull(plan, "plan");
            Objects.requireNonNull(perNode, "perNode");
        }

        /** The coordinator plan over {@code fanOut} per node requests, with the reduce the shape selects. */
        public RelNode coordinatorPlan(ExecutionShape shape, int fanOut) {
            MergeExec.ReduceKind reduceKind = shape.hasAggregations() ? MergeExec.ReduceKind.AGGREGATE_INTERNAL
                : shape.hits() ? MergeExec.ReduceKind.HITS_TOP_K
                : MergeExec.ReduceKind.COUNT_SUM;
            return SearchRequestToRel.withCoordinatorLayer(perNode, reduceKind, fanOut);
        }
    }

    /**
     * Plans {@code shape} against {@code model}.
     *
     * @param sqlExcludedColumns the override columns whose predicates
     *     never travel to Lance SQL
     * @throws IllegalArgumentException for a filtered {@code lance_knn}
     *     whose filter cannot travel to the Lance scan (answers 400)
     */
    public static Planned plan(
        ExecutionShape shape,
        LanceSchemas.IndexModel model,
        Set<String> sqlExcludedColumns,
        LancePlannerFactory factory
    ) {
        QueryBuilder query = shape.query();
        LanceKnnQueryBuilder filteredKnn = query instanceof LanceKnnQueryBuilder knn && knn.filter() != null ? knn : null;
        if (filteredKnn != null && QueryToRex.referencesAny(filteredKnn.filter(), sqlExcludedColumns)) {
            throw knnFilterRefusal(
                filteredKnn,
                "predicates on ip and geo_point fields are evaluated over encoded doc values on the Lucene side"
            );
        }
        if (query != null && QueryToRex.referencesAny(query, sqlExcludedColumns)) {
            return luceneFallback(shape, model, factory);
        }
        RelNode logical;
        try {
            logical = SearchRequestToRel.translateForExecution(shape, model, factory);
        } catch (UnsupportedOperationException unsupported) {
            if (filteredKnn != null) {
                throw knnFilterRefusal(filteredKnn, unsupported.getMessage());
            }
            return luceneFallback(shape, model, factory);
        }
        RelNode physical = factory.plan(logical);
        FragmentPlan plan = FragmentPlan.of(physical, shape.hasAggregations(), shape.hits());
        if (filteredKnn != null && (plan.lanceClause() == null || plan.filterSql() == null)) {
            throw knnFilterRefusal(filteredKnn, "the filter has no Lance SQL form");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("lance.plan: index [{}] planned [{}]\n{}", model.indexName(), plan, RelOptUtil.toString(physical));
        }
        return new Planned(plan, physical);
    }

    /**
     * The plan of a query the translator cannot spell: the executor
     * builds the Lucene composition of the request's builder and runs
     * the envelope through Lucene. The per node subtree is the bare
     * scan, or a one column values placeholder when the schema holds a
     * column no Calcite type spells; the coordinator layer only needs a
     * node to wrap.
     */
    private static Planned luceneFallback(ExecutionShape shape, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        RelNode perNode;
        try {
            RelBuilder relBuilder = factory.relBuilder(model.schema());
            relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
            perNode = relBuilder.build();
        } catch (UnsupportedOperationException unsupportedColumn) {
            perNode = factory.relBuilder(model.schema()).values(new String[] { "row" }, 0).build();
        }
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.luceneKind(shape.hasAggregations(), shape.hits()), null);
        LOGGER.debug("lance.plan: index [{}] planned [{}] (query outside the planner's vocabulary)", model.indexName(), plan);
        return new Planned(plan, perNode);
    }

    /**
     * The 400 a filtered {@code lance_knn} answers when its filter
     * cannot travel to the Lance scan, naming the filter clause's
     * builder class so the caller sees which part was refused.
     */
    static IllegalArgumentException knnFilterRefusal(LanceKnnQueryBuilder knn, String reason) {
        return new IllegalArgumentException(
            "[lance_knn] filter type ["
                + knn.filter().getClass().getSimpleName()
                + "] is not supported by the pre-filter translator: "
                + reason
        );
    }
}
