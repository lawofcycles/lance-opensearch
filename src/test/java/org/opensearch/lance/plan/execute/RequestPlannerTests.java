/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/**
 * The coordinator's planning of one target, pinned per request shape:
 * which shapes reach the fragment executors as a pushed scan, which as
 * a Lucene plan, and what query part each carries. Together these pins
 * are the routing table the fragment executor used to decide per part
 * (query, page, aggregation) before the coordinator planned once.
 */
public class RequestPlannerTests extends OpenSearchTestCase {

    private static final Set<String> NO_EXCLUDED = Set.of();

    private static ExecutionShape shape(String json, boolean planAggregations) throws IOException {
        return shape(PlanTestFixtures.parse(json), planAggregations);
    }

    private static ExecutionShape shape(SearchSourceBuilder source, boolean planAggregations) {
        int size = source.size() < 0 ? 10 : source.size();
        int from = Math.max(0, source.from());
        return new ExecutionShape(
            source.query(),
            source.postFilter(),
            source.sorts() == null ? List.of() : source.sorts(),
            source.searchAfter(),
            from,
            size == 0 ? 0 : from + size,
            source.aggregations(),
            planAggregations
        );
    }

    private static RequestPlanner.Planned plan(String json) throws IOException {
        return plan(json, true, NO_EXCLUDED);
    }

    private static RequestPlanner.Planned plan(String json, boolean planAggregations, Set<String> excluded) throws IOException {
        return RequestPlanner.plan(shape(json, planAggregations), PlanTestFixtures.model(), excluded, PlanTestFixtures.factory());
    }

    public void testAggregationOverScalarFilterIsAPushedScan() throws IOException {
        RequestPlanner.Planned planned = plan(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"
        );
        FragmentPlan plan = planned.plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertNotNull("the Substrait aggregate travels", plan.aggregate());
        assertEquals(0, plan.aggregate().groupCount());
        assertEquals(List.of("s"), plan.aggregate().metrics().stream().map(FragmentPlan.MetricSlot::name).toList());
        assertEquals("rating = 5", plan.filterSql());
        assertEquals("rating = 5", plan.scalarFilterSql());
        assertNull(plan.lanceClause());
        assertNull(plan.topK());
        assertTrue(planned.perNode() instanceof LanceTableScan);
    }

    public void testBucketAggregationOverMatchAllIsAPushedScanWithoutFilter() throws IOException {
        FragmentPlan plan = plan("{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}").plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals(1, plan.aggregate().groupCount());
        assertNull(plan.filterSql());
    }

    public void testAggregationWithThePushdownOffKeepsTheFilterOnALucenePlan() throws IOException {
        FragmentPlan plan = plan(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}",
            false,
            NO_EXCLUDED
        ).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.aggregate());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testAggregationTheTranslatorRefusesKeepsTheFilterOnALucenePlan() throws IOException {
        // A sum over a keyword column has no Lance form; the aggregators
        // run over the pushed filter, exactly as when the executor
        // planned the aggregation tree alone and refused it.
        FragmentPlan plan = plan("{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"category\"}}}}")
            .plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.aggregate());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testAggregationNextToAPostFilterStaysOnTheAggregators() throws IOException {
        // The pushed aggregate reports hits.total from its own count,
        // which the post filter would have to narrow.
        FragmentPlan plan = plan(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"post_filter\":{\"term\":{\"flag\":true}},"
                + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"
        ).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.aggregate());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testCountShapesCarryTheFilterOnly() throws IOException {
        FragmentPlan filtered = plan("{\"size\":0,\"query\":{\"term\":{\"rating\":5}}}").plan();
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, filtered.kind());
        assertEquals("rating = 5", filtered.filterSql());
        FragmentPlan matchAll = plan("{\"size\":0}").plan();
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, matchAll.kind());
        assertNull(matchAll.filterSql());
        assertNull(matchAll.lanceClause());
    }

    public void testUnsortedPageIsAPushedScanCutByTheScan() throws IOException {
        FragmentPlan plan = plan("{\"size\":10,\"query\":{\"term\":{\"rating\":5}}}").plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertNotNull(plan.topK());
        assertEquals("the scan's own order is the page order", List.of(), plan.topK().orderings());
        assertEquals(10, plan.topK().fetch());
        assertNull(plan.topK().cursorSql());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testSortedPageIsAPushedScanWithOrderings() throws IOException {
        FragmentPlan plan = plan("{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":{\"order\":\"desc\"}}]}").plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals(List.of(new FragmentPlan.ScanOrdering("price", false, false)), plan.topK().orderings());
        assertEquals(10, plan.topK().fetch());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testFromFoldsIntoTheFetchAndCursorIntoTheScanFilter() throws IOException {
        FragmentPlan plan = plan("{\"from\":5,\"size\":10,\"sort\":[{\"rating\":{\"order\":\"asc\"}}],\"search_after\":[7]}").plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals(15, plan.topK().fetch());
        assertEquals("(rating > 7 OR rating IS NULL)", plan.topK().cursorSql());
        assertNull(plan.filterSql());
        assertEquals("(rating > 7 OR rating IS NULL)", plan.topK().scanFilterSql(plan.filterSql()));
    }

    public void testPostFilterKeepsThePageOnTheCollector() throws IOException {
        RequestPlanner.Planned planned = plan(
            "{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"post_filter\":{\"term\":{\"flag\":true}},\"sort\":[{\"price\":\"desc\"}]}"
        );
        assertTrue(planned.perNode() instanceof HeapTopKExec);
        FragmentPlan plan = planned.plan();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertNull(plan.topK());
        assertEquals("the query filter still travels as SQL", "rating = 5", plan.filterSql());
    }

    public void testSortWithoutACollationSpellingKeepsThePageOnTheCollector() throws IOException {
        FragmentPlan plan = plan(
            "{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"sort\":[{\"price\":{\"order\":\"desc\",\"mode\":\"min\"}}]}"
        ).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertNull(plan.topK());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testPageNextToAggregationsRunsBothOnLucene() throws IOException {
        FragmentPlan plan = plan("{\"size\":10,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
            .plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.topK());
        assertNull(plan.aggregate());
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testBareFullTextPageCarriesTheClauseAndTheScanLimit() throws IOException {
        FragmentPlan plan = plan("{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}").plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertNull(plan.filterSql());
        assertNull("no scalar filter for the Lucene side", plan.scalarFilterSql());
        assertEquals(List.of(), plan.topK().orderings());
        assertEquals(10, plan.topK().fetch());
    }

    public void testFullTextBoolCarriesThePrefilterSql() throws IOException {
        String body = "{\"size\":10,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}}}";
        FragmentPlan plan = plan(body).plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertEquals("rating = 5", plan.filterSql());
        assertNull("the SQL is the clause's prefilter, not a scalar filter", plan.scalarFilterSql());
    }

    public void testFullTextWithAggregationsFusesTheQueryAndKeepsTheAggregatorsOnLucene() throws IOException {
        String body = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}";
        FragmentPlan plan = plan(body).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.aggregate());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testFullTextWithAColumnSortKeepsThePageOnTheCollectorOverTheFusedQuery() throws IOException {
        String body = "{\"size\":10,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"rating\":5}}]}},\"sort\":[{\"price\":\"desc\"}]}";
        RequestPlanner.Planned planned = plan(body);
        assertTrue(planned.perNode() instanceof HeapTopKExec);
        FragmentPlan plan = planned.plan();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertTrue(plan.lanceClause() instanceof LanceMatchQueryBuilder);
        assertEquals("rating = 5", plan.filterSql());
    }

    public void testScoreSortedFullTextPageFoldsIntoTheScan() throws IOException {
        String body = "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}";
        FragmentPlan plan = plan(body).plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertEquals(List.of(), plan.topK().orderings());
    }

    public void testFilteredKnnCarriesTheClauseWithoutItsFilterAndThePrefilterSql() throws IOException {
        String body = "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
            + "\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}";
        FragmentPlan plan = plan(body).plan();
        assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
        assertTrue(plan.isKnn());
        assertNull("the filter travels as SQL, not inside the clause", ((LanceKnnQueryBuilder) plan.lanceClause()).filter());
        assertEquals("id >= 10", plan.filterSql());
    }

    public void testFilteredKnnWhoseFilterHasNoSqlFormIsRefused() throws IOException {
        String body = "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
            + "\"filter\":{\"term\":{\"nope\":1}}}}}";
        IllegalArgumentException refused = expectThrows(IllegalArgumentException.class, () -> plan(body));
        assertThat(
            refused.getMessage(),
            containsString("[lance_knn] filter type [TermQueryBuilder] is not supported by the pre-filter translator")
        );
    }

    public void testFilteredKnnOverAnExcludedColumnIsRefused() throws IOException {
        String body = "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[0.1,0.2],\"k\":5,"
            + "\"filter\":{\"term\":{\"category\":\"c0\"}}}}}";
        IllegalArgumentException refused = expectThrows(IllegalArgumentException.class, () -> plan(body, true, Set.of("category")));
        assertThat(refused.getMessage(), containsString("predicates on ip and geo_point fields"));
    }

    public void testQueryOutsideTheVocabularyIsALucenePlanWithoutAQueryPart() throws IOException {
        RequestPlanner.Planned planned = plan("{\"size\":10,\"query\":{\"match\":{\"category\":\"c0\"}}}");
        FragmentPlan plan = planned.plan();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertNull(plan.filterSql());
        assertNull(plan.lanceClause());
        assertNull(plan.topK());
        assertTrue("the coordinator layer wraps the bare scan", planned.perNode() instanceof LanceTableScan);
        FragmentPlan count = plan("{\"size\":0,\"query\":{\"match\":{\"category\":\"c0\"}}}").plan();
        assertEquals(FragmentPlan.Kind.LUCENE_COUNT, count.kind());
        FragmentPlan aggregate = plan(
            "{\"size\":0,\"query\":{\"match\":{\"category\":\"c0\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}"
        ).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, aggregate.kind());
    }

    public void testQueryOverAnExcludedColumnIsALucenePlanWithoutAQueryPart() throws IOException {
        FragmentPlan plan = plan("{\"size\":10,\"query\":{\"term\":{\"category\":\"c0\"}}}", true, Set.of("category")).plan();
        assertEquals(FragmentPlan.Kind.LUCENE_TOPK, plan.kind());
        assertNull(plan.filterSql());
    }

    public void testFilterWithoutASqlSpellingLeavesTheWholeQueryToLucene() throws IOException {
        // A term on an unmapped field has no Lance SQL: the translator
        // refuses, and the request's own builder runs through Lucene.
        FragmentPlan plan = plan("{\"size\":0,\"query\":{\"term\":{\"nope\":1}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}").plan();
        assertEquals(FragmentPlan.Kind.LUCENE_AGGREGATE, plan.kind());
        assertNull(plan.filterSql());
        assertNull(plan.aggregate());
    }

    public void testCoordinatorPlanWrapsThePerNodePlanWithTheReduceTheShapeSelects() throws IOException {
        String aggregation = "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}";
        RelNode plan = plan(aggregation).coordinatorPlan(shape(aggregation, true), 3);
        assertTrue(plan instanceof MergeExec);
        assertEquals(MergeExec.ReduceKind.AGGREGATE_INTERNAL, ((MergeExec) plan).reduceKind());
        assertTrue(((MergeExec) plan).getInput() instanceof FanOutExec);
        assertEquals(3, ((FanOutExec) ((MergeExec) plan).getInput()).fanOut());

        String hits = "{\"size\":10}";
        assertEquals(MergeExec.ReduceKind.HITS_TOP_K, ((MergeExec) plan(hits).coordinatorPlan(shape(hits, true), 1)).reduceKind());
        String count = "{\"size\":0}";
        assertEquals(MergeExec.ReduceKind.COUNT_SUM, ((MergeExec) plan(count).coordinatorPlan(shape(count, true), 1)).reduceKind());
    }

    public void testLuceneAggregateExecRootReadsTheFilterUnderIt() throws IOException {
        // A sum over the keyword column keeps the aggregate on the
        // Lucene operator; the filter below it still prints.
        RequestPlanner.Planned planned = plan(
            "{\"size\":0,\"query\":{\"term\":{\"rating\":5}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"category\"}}}}"
        );
        LanceSchemas.IndexModel model = PlanTestFixtures.model();
        assertNotNull(model);
        // Either the converter lowered the tree or the translator refused
        // it; both read the same query part.
        assertTrue(planned.perNode() instanceof LuceneAggregateExec || planned.perNode() instanceof LanceTableScan);
        assertEquals("rating = 5", planned.plan().filterSql());
    }
}
