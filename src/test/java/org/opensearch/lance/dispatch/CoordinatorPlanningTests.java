/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

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
        FragmentPlan planned = FragmentRequests.plan(
            getInstanceFromNode(ClusterService.class).state().metadata().index(indexName),
            getInstanceFromNode(LanceWarmCache.class),
            new ExecutionShape(rewritten, null, List.of(), null, 0, 10, null, false)
        );
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, planned.kind());
        assertEquals("category = 'c0'", planned.filterSql());
        FragmentPlan unplanned = FragmentRequests.plan(
            getInstanceFromNode(ClusterService.class).state().metadata().index(indexName),
            getInstanceFromNode(LanceWarmCache.class),
            new ExecutionShape(wrapped, null, List.of(), null, 0, 10, null, false)
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
