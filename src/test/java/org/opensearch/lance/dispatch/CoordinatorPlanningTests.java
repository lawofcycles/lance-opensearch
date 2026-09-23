/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.explain.LanceExplainAction;
import org.opensearch.lance.plan.explain.LanceExplainRequest;
import org.opensearch.lance.plan.explain.LanceExplainResponse;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

/**
 * The coordinator's side of planning against a live node: the
 * shard-free rewrite runs before the plan, the plan a target receives is
 * the one the planner chose for the rewritten query, and a search whose
 * query only spells itself out after the rewrite answers from the pushed
 * scan; an empty table with aggregations still runs its single fan-out
 * with a plan and answers the aggregations block.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CoordinatorPlanningTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    private String attach(String indexName, int fragments, int rowsPerFragment) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeHintFixtureTable(dir, indexName, fragments, rowsPerFragment);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
        return tableUri;
    }

    public void testCoordinatorRewriteFoldsTheQueryBeforePlanning() throws Exception {
        String indexName = "coordinator-rewrite";
        attach(indexName, 2, 50);
        TransportLanceCoordinatorAction coordinator = getInstanceFromNode(TransportLanceCoordinatorAction.class);
        QueryBuilder wrapped = new WrapperQueryBuilder("{\"term\":{\"category\":\"c0\"}}");
        QueryBuilder rewritten = coordinator.rewriteAtCoordinator(wrapped, System.currentTimeMillis());
        assertTrue("the wrapper unwraps at the coordinator, saw " + rewritten, rewritten instanceof TermQueryBuilder);
        assertEquals("category", ((TermQueryBuilder) rewritten).fieldName());
        assertNull(coordinator.rewriteAtCoordinator(null, 0L));

        // The plan of the rewritten query carries the term's Lance SQL;
        // the wrapper itself has no relational form and would have left
        // the whole query to Lucene.
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        FragmentPlan planned = FragmentRequests.plan(
            clusterService.state().metadata().index(indexName),
            getInstanceFromNode(LanceWarmCache.class),
            new ExecutionShape(rewritten, null, List.of(), null, 0, 10, null, false),
            clusterService
        );
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, planned.kind());
        assertEquals("category = 'c0'", planned.filterSql());
        FragmentPlan unplanned = FragmentRequests.plan(
            clusterService.state().metadata().index(indexName),
            getInstanceFromNode(LanceWarmCache.class),
            new ExecutionShape(wrapped, null, List.of(), null, 0, 10, null, false),
            clusterService
        );
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, unplanned.kind());
        assertNull(unplanned.filterSql());
    }

    public void testSearchWithAWrapperQueryAnswersFromTheRewrittenPlan() throws Exception {
        String indexName = "coordinator-wrapper-search";
        attach(indexName, 2, 60);
        // category is "c" + (i % 3), null when i % 4 == 3: 120 rows, 40
        // per category before the nulls, 30 of them c0 after.
        BoolQueryBuilder direct = QueryBuilders.boolQuery().filter(new TermQueryBuilder("category", "c0"));
        SearchResponse expected = client().prepareSearch(indexName).setSource(new SearchSourceBuilder().query(direct).size(5)).get();
        SearchResponse viaWrapper = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().query(new WrapperQueryBuilder("{\"term\":{\"category\":\"c0\"}}")).size(5))
            .get();
        assertEquals(expected.getHits().getTotalHits().value(), viaWrapper.getHits().getTotalHits().value());
        assertEquals(5, viaWrapper.getHits().getHits().length);
        assertTrue(viaWrapper.getHits().getTotalHits().value() > 0);
    }

    /**
     * The plan the explain endpoint prints and the plan the coordinator
     * ships agree, shape by shape: a pushed aggregate, a pushed sorted
     * page and a fused filtered knn print on the explain scan under the
     * coordinator's merge and fan out, the {@code fragment_plan} of the
     * answer equals the plan the executor receives, and executing the
     * shipped plan on this node applies no downgrade. The executor's
     * {@code lance.plan} debug line is raised for the run so the log
     * shows the data node's plan next to the explain answer.
     */
    @TestLogging(value = "org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction:DEBUG", reason = "pair the explain answer with the executor's lance.plan line")
    public void testExplainPlanAndShippedPlanAgree() throws Exception {
        String indexName = "coordinator-explain-agree";
        String tableUri = attach(indexName, 3, 100);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        LanceWarmCache warmCache = getInstanceFromNode(LanceWarmCache.class);
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        Map<String, Long> before = FragmentPlanRefiner.refinementCounts();

        // filter + terms: the explain scan carries the pushed aggregate
        // with the filter's SQL, the shipped plan the same aggregate.
        SearchSourceBuilder aggregation = new SearchSourceBuilder().size(0).query(new RangeQueryBuilder("rating").gte(500));
        aggregation.aggregation(AggregationBuilders.terms("c").field("category"));
        LanceExplainResponse aggregationExplain = explain(indexName, aggregation);
        String aggregationPhysical = aggregationExplain.physical();
        LanceFragmentQueryRequest aggregationRequest = FragmentRequests.planned(
            clusterService,
            warmCache,
            tableUri,
            indexName,
            aggregation.query(),
            List.of(),
            0,
            aggregation.aggregations(),
            List.of()
        );
        logger.info(
            "filter + terms explain physical:\n{}explain fragment_plan: {}\nshipped plan: {}",
            aggregationPhysical,
            aggregationExplain.fragmentPlan(),
            aggregationRequest.plan()
        );
        assertTrue(aggregationPhysical, aggregationPhysical.startsWith("MergeExec(reduce=[AGGREGATE_INTERNAL])"));
        assertTrue(aggregationPhysical, aggregationPhysical.contains("FanOutExec(fanOut=[1]"));
        assertTrue(aggregationPhysical, aggregationPhysical.contains("LanceTableScan("));
        assertEquals(LanceExplainResponse.Route.FRAGMENT, aggregationExplain.route());
        assertEquals(aggregationRequest.plan(), aggregationExplain.fragmentPlan());
        assertNull(aggregationExplain.unplanned());
        assertEquals(List.of(), aggregationExplain.refinementsPossible());
        assertTrue(aggregationPhysical, aggregationPhysical.contains("aggregate{groups=1"));
        assertTrue(aggregationPhysical, aggregationPhysical.contains("filter=rating >= 500"));
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, aggregationRequest.plan().kind());
        assertEquals(1, aggregationRequest.plan().aggregate().groupCount());
        assertEquals("rating >= 500", aggregationRequest.plan().filterSql());
        LanceFragmentQueryResponse aggregated = executor.execute(aggregationRequest);
        assertNotNull(aggregated.aggregations());

        // sorted page: the explain scan carries the pushed top-k with
        // the rating ordering, the shipped plan the same ordering.
        SearchSourceBuilder page = new SearchSourceBuilder().size(5)
            .query(new RangeQueryBuilder("rating").gte(500))
            .sort(new FieldSortBuilder("rating").order(SortOrder.DESC));
        LanceExplainResponse pageExplain = explain(indexName, page);
        String pagePhysical = pageExplain.physical();
        LanceFragmentQueryRequest pageRequest = FragmentRequests.planned(
            clusterService,
            warmCache,
            tableUri,
            indexName,
            page.query(),
            page.sorts(),
            5,
            null,
            List.of()
        );
        logger.info(
            "sorted page explain physical:\n{}explain fragment_plan: {}\nshipped plan: {}",
            pagePhysical,
            pageExplain.fragmentPlan(),
            pageRequest.plan()
        );
        assertTrue(pagePhysical, pagePhysical.startsWith("MergeExec(reduce=[HITS_TOP_K])"));
        assertTrue(pagePhysical, pagePhysical.contains("LanceTableScan("));
        assertEquals(pageRequest.plan(), pageExplain.fragmentPlan());
        assertEquals(List.of(), pageExplain.refinementsPossible());
        assertTrue(pagePhysical, pagePhysical.contains("topk{collations=["));
        assertTrue(pagePhysical, pagePhysical.contains("fetch=5"));
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, pageRequest.plan().kind());
        assertEquals(List.of(new FragmentPlan.ScanOrdering("rating", false, false)), pageRequest.plan().topK().orderings());
        assertEquals(5, pageRequest.plan().topK().fetch());
        LanceFragmentQueryResponse paged = executor.execute(pageRequest);
        assertEquals(5, paged.hits().size());

        // knn + filter: the explain scan carries the pushed knn with the
        // filter's SQL, the shipped plan the knn clause and the same SQL.
        float[] vector = new float[8];
        vector[0] = 1f;
        SearchSourceBuilder knn = new SearchSourceBuilder().size(3)
            .query(new LanceKnnQueryBuilder("embedding", vector, 3).filter(new RangeQueryBuilder("rating").gte(500)));
        LanceExplainResponse knnExplain = explain(indexName, knn);
        String knnPhysical = knnExplain.physical();
        LanceFragmentQueryRequest knnRequest = FragmentRequests.planned(
            clusterService,
            warmCache,
            tableUri,
            indexName,
            knn.query(),
            List.of(),
            3,
            null,
            List.of()
        );
        logger.info(
            "knn + filter explain physical:\n{}explain fragment_plan: {}\nshipped plan: {}",
            knnPhysical,
            knnExplain.fragmentPlan(),
            knnRequest.plan()
        );
        assertTrue(knnPhysical, knnPhysical.startsWith("MergeExec(reduce=[HITS_TOP_K])"));
        assertTrue(knnPhysical, knnPhysical.contains("LanceTableScan("));
        assertEquals(knnRequest.plan(), knnExplain.fragmentPlan());
        assertTrue(knnPhysical, knnPhysical.contains("knn{"));
        assertTrue(knnPhysical, knnPhysical.contains("filter=rating >= 500"));
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, knnRequest.plan().kind());
        assertTrue(knnRequest.plan().isKnn());
        assertEquals("rating >= 500", knnRequest.plan().filterSql());
        LanceFragmentQueryResponse nearest = executor.execute(knnRequest);
        assertEquals(3, nearest.hits().size());

        assertEquals("no downgrade on a node without a reader wrapper", before, FragmentPlanRefiner.refinementCounts());
    }

    private LanceExplainResponse explain(String indexName, SearchSourceBuilder source) {
        return client().execute(LanceExplainAction.INSTANCE, new LanceExplainRequest(indexName, source)).actionGet();
    }

    public void testEmptyTableWithAggregationsAnswersTheAggregationsBlock() throws Exception {
        String indexName = "coordinator-empty-aggs";
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeMultiFragmentTable(dir, indexName, 0, 4);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        source.aggregation(AggregationBuilders.max("m").field("id"));
        SearchResponse response = client().prepareSearch(indexName).setSource(source).get();
        assertEquals(0L, response.getHits().getTotalHits().value());
        assertNotNull("the empty aggregation run still answers the block", response.getAggregations());
        Max max = response.getAggregations().get("m");
        assertNotNull(max);
    }
}
