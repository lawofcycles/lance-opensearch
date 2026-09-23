/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortBuilder;

/**
 * Builds per node requests for tests that drive
 * {@link TransportLanceFragmentQueryAction} directly, planning them the
 * way the coordinator does ({@link RequestPlanner} over the index's
 * model from the node's warm cache) so the executor receives the same
 * {@link FragmentPlan} a real fan-out would ship.
 */
public final class FragmentRequests {

    private FragmentRequests() {}

    /**
     * A request planned against the live index, every fragment unless
     * {@code fragmentIds} names some, an exact match count, no
     * {@code post_filter}, no cursor, {@code track_scores} off. The
     * aggregation pushdown setting is read from the cluster settings
     * the way the coordinator reads it.
     */
    public static LanceFragmentQueryRequest planned(
        ClusterService clusterService,
        LanceWarmCache warmCache,
        String tableUri,
        String indexName,
        QueryBuilder query,
        List<SortBuilder<?>> sorts,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds
    ) {
        return planned(
            clusterService,
            warmCache,
            tableUri,
            indexName,
            query,
            null,
            sorts,
            null,
            size,
            aggregations,
            fragmentIds,
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    /** A request planned against the live index with every envelope element spelled out. */
    public static LanceFragmentQueryRequest planned(
        ClusterService clusterService,
        LanceWarmCache warmCache,
        String tableUri,
        String indexName,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds,
        boolean trackScores,
        int trackTotalHitsUpTo
    ) {
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        ExecutionShape shape = new ExecutionShape(query, postFilter, sorts, searchAfter, 0, size, aggregations, false);
        FragmentPlan plan = plan(metadata, warmCache, shape, clusterService);
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            -1L,
            plan,
            query,
            postFilter,
            sorts,
            searchAfter,
            size,
            aggregations,
            fragmentIds,
            trackScores,
            trackTotalHitsUpTo
        );
    }

    /**
     * The plan the coordinator would ship for {@code shape} against the
     * index behind {@code metadata}, costed with the cluster's current
     * settings the way the coordinator costs it (one data node, the
     * table URI's storage kind, the parallelism and the aggregation
     * routing settings).
     */
    public static FragmentPlan plan(IndexMetadata metadata, LanceWarmCache warmCache, ExecutionShape shape, ClusterService clusterService) {
        try {
            LanceSchemas.IndexModel model = LanceSchemas.build(metadata, warmCache);
            String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            return RequestPlanner.plan(
                shape,
                model,
                PlanExecutor.sqlExcludedColumns(LanceOverrides.of(metadata.getSettings())),
                new LancePlannerFactory(1L << 30, 1L << 30),
                RequestPlanner.clusterInputs(1, tableUri, clusterService.getClusterSettings())
            ).plan();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * A request that carries no planned tree: the executor builds the
     * Lucene composition of {@code query} and runs the envelope the
     * request shape selects through Lucene, with {@code filterSql}
     * (may be null) as the scalar filter. For tests of the Lucene side
     * that need no planner.
     */
    public static LanceFragmentQueryRequest lucene(
        String tableUri,
        String indexName,
        String filterSql,
        QueryBuilder query,
        QueryBuilder postFilter,
        List<SortBuilder<?>> sorts,
        Object[] searchAfter,
        int size,
        AggregatorFactories.Builder aggregations,
        List<Integer> fragmentIds,
        boolean trackScores,
        int trackTotalHitsUpTo
    ) {
        boolean hasAggregations = aggregations != null && !aggregations.getAggregatorFactories().isEmpty();
        FragmentPlan plan = FragmentPlan.lucene(FragmentPlan.luceneKind(hasAggregations, size > 0), filterSql);
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            -1L,
            plan,
            query,
            postFilter,
            sorts,
            searchAfter,
            size,
            aggregations,
            fragmentIds,
            trackScores,
            trackTotalHitsUpTo
        );
    }
}
