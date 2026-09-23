/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The planned dispatch decision through the Volcano run: a body
 * holding an element only the shard path serves plans to a
 * {@link ShardPathFallbackExec} root carrying the reason, exactly the
 * shapes the dispatch filter's hand written checks used to name, and a
 * body without one plans to the Lance scan and stays on the fragment
 * path.
 */
public class PlanToShardPathRuleTests extends OpenSearchTestCase {

    private static RelNode plan(String body) throws IOException {
        return plan(PlanTestFixtures.parse(body));
    }

    private static RelNode plan(SearchSourceBuilder source) {
        LancePlannerFactory factory = PlanTestFixtures.factory();
        return factory.plan(SearchRequestToRel.translateDispatch(source, PlanTestFixtures.model(), factory));
    }

    private static void assertFallsBack(String body, ShardPathReason reason) throws IOException {
        RelNode physical = plan(body);
        assertTrue("the shard path operator answers the shape: " + physical, physical instanceof ShardPathFallbackExec);
        ShardPathFallbackExec fallback = (ShardPathFallbackExec) physical;
        assertEquals(List.of(reason), fallback.reasons());
        assertTrue("the operator runs over the bare scan", fallback.getInput() instanceof LanceTableScan);
        assertTrue(((LanceTableScan) fallback.getInput()).pushedOperations().isEmpty());
    }

    private static void assertDispatchable(String body) throws IOException {
        RelNode physical = plan(body);
        assertFalse("the shape stays on the fragment path: " + physical, physical instanceof ShardPathFallbackExec);
    }

    public void testSuggestFallsBack() throws IOException {
        assertFallsBack("{\"suggest\":{\"s\":{\"text\":\"hello\",\"term\":{\"field\":\"body\"}}}}", ShardPathReason.SUGGEST);
    }

    public void testHighlightFallsBack() throws IOException {
        assertFallsBack("{\"query\":{\"match_all\":{}},\"highlight\":{\"fields\":{\"body\":{}}}}", ShardPathReason.HIGHLIGHT);
    }

    public void testScoreOrderSearchAfterFallsBack() throws IOException {
        assertFallsBack("{\"size\":5,\"search_after\":[100]}", ShardPathReason.SEARCH_AFTER_SCORE);
    }

    public void testCollapseFallsBack() throws IOException {
        assertFallsBack("{\"collapse\":{\"field\":\"category\"}}", ShardPathReason.COLLAPSE);
    }

    public void testRescoreFallsBack() throws IOException {
        assertFallsBack(
            "{\"query\":{\"match_all\":{}},\"rescore\":{\"window_size\":10,\"query\":{\"rescore_query\":{\"match_all\":{}}}}}",
            ShardPathReason.RESCORE
        );
    }

    public void testSiblingPipelineAggregationFallsBack() throws IOException {
        assertFallsBack(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}},\"ab\":{\"avg_bucket\":{\"buckets_path\":\"by>_count\"}}}}",
            ShardPathReason.PIPELINE_AGG
        );
    }

    public void testNestedParentPipelineAggregationFallsBack() throws IOException {
        assertFallsBack(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},"
                + "\"aggs\":{\"c\":{\"sum\":{\"field\":\"id\"}},\"cs\":{\"cumulative_sum\":{\"buckets_path\":\"c\"}}}}}}",
            ShardPathReason.PIPELINE_AGG
        );
    }

    public void testMinScoreFallsBack() throws IOException {
        assertFallsBack("{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0}", ShardPathReason.MIN_SCORE);
    }

    public void testTerminateAfterFallsBack() throws IOException {
        assertFallsBack("{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2}", ShardPathReason.TERMINATE_AFTER);
    }

    public void testStoredFieldsFallsBack() throws IOException {
        assertFallsBack("{\"size\":1,\"stored_fields\":\"_none_\"}", ShardPathReason.STORED_FIELDS);
    }

    public void testDocValueFieldsFallsBack() throws IOException {
        assertFallsBack("{\"size\":1,\"docvalue_fields\":[\"id\"]}", ShardPathReason.DOCVALUE_FIELDS);
    }

    public void testExplainFallsBack() throws IOException {
        assertFallsBack("{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}", ShardPathReason.EXPLAIN_PER_HIT);
    }

    public void testCombinedReasonsRideOneFallback() throws IOException {
        RelNode physical = plan("{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0,\"terminate_after\":2}");
        assertTrue("the shard path operator answers the shape: " + physical, physical instanceof ShardPathFallbackExec);
        assertEquals(List.of(ShardPathReason.MIN_SCORE, ShardPathReason.TERMINATE_AFTER), ((ShardPathFallbackExec) physical).reasons());
    }

    public void testEmptyBodyIsDispatchable() {
        RelNode physical = plan((SearchSourceBuilder) null);
        assertFalse("an empty body stays on the fragment path: " + physical, physical instanceof ShardPathFallbackExec);
    }

    public void testMatchAllIsDispatchable() throws IOException {
        assertDispatchable("{\"size\":5,\"query\":{\"match_all\":{}}}");
    }

    public void testQueryTypeOutsideThePlannerIsDispatchable() throws IOException {
        // The strict translator refuses `match`, but the fragment path
        // ships the builder to the executors and answers it, so the
        // dispatch decision must not depend on the query spelling.
        assertDispatchable("{\"size\":5,\"query\":{\"match\":{\"body\":\"hello\"}}}");
    }

    public void testSortedSearchAfterIsDispatchable() throws IOException {
        assertDispatchable("{\"size\":5,\"sort\":[{\"rating\":\"asc\"}],\"search_after\":[100]}");
    }

    public void testPlainAggregationIsDispatchable() throws IOException {
        assertDispatchable("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}");
    }

    public void testPostFilterIsDispatchable() throws IOException {
        // post_filter is served by the fragment path (the executors
        // AND it into the Lucene query), so it never routes away.
        assertDispatchable("{\"size\":5,\"query\":{\"match_all\":{}},\"post_filter\":{\"term\":{\"category\":\"c0\"}}}");
    }

    public void testShardPathReasonsCollectEveryElement() throws IOException {
        SearchSourceBuilder source = PlanTestFixtures.parse("{\"size\":1,\"stored_fields\":\"_none_\",\"explain\":true,\"min_score\":1.5}");
        assertEquals(
            List.of(ShardPathReason.MIN_SCORE, ShardPathReason.STORED_FIELDS, ShardPathReason.EXPLAIN_PER_HIT),
            SearchRequestToRel.shardPathReasons(source)
        );
    }
}
