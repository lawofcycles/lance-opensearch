/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The explain endpoint answers what the coordinator would execute: the
 * route, the coordinator's physical tree over the per node plan, the
 * fragment plan the data nodes receive, the element that kept an
 * envelope on Lucene, and the refinements a data node could still
 * apply. Every body the runtime accepts is answered; an unknown index
 * answers 404 and a non Lance index 400. Nothing here executes a search
 * through the planner.
 */
public class LanceExplainIT extends LanceRestTestCase {

    private static Response explain(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static String explainOk(String indexName, String body) throws IOException {
        Response response = explain(indexName, body);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return readAll(response);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fragmentPlanOf(String body) {
        Map<String, Object> plan = (Map<String, Object>) parseJson(body).get("fragment_plan");
        assertNotNull("the fragment route carries a fragment plan: " + body, plan);
        return plan;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listOf(String body, String key) {
        return (List<Object>) parseJson(body).get(key);
    }

    private static void attach(String tableUri) throws IOException {
        Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
        assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
    }

    private static void deleteQuietly(String indexName) {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {}
    }

    public void testExplainAnswersTheCoordinatorPlan() throws Exception {
        String suffix = "explain-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            attach(tableUri);

            String body = explainOk(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");
            assertEquals(indexName, stringPath(body, "index"));
            assertEquals("fragment", stringPath(body, "route"));
            String logical = stringPath(body, "logical");
            assertTrue("logical plan carries the aggregate: " + logical, logical.contains("LanceAggregate"));
            assertTrue("logical plan carries the scan: " + logical, logical.contains("LanceTableScan"));
            assertTrue("the aggregation name is the output alias: " + logical, logical.contains("s=[SUM("));
            // The physical tree is the coordinator's: the merge over the
            // fan out (one request per data node of this single node
            // cluster) over the per node scan carrying the aggregate.
            String physical = stringPath(body, "physical");
            assertTrue("the coordinator merge leads: " + physical, physical.startsWith("MergeExec(reduce=[AGGREGATE_INTERNAL])"));
            assertTrue("the fan out width is the data node count: " + physical, physical.contains("FanOutExec(fanOut=[1]"));
            assertTrue("the pushed aggregate appears in the physical plan: " + physical, physical.contains("pushed=[[aggregate{"));
            assertFalse("no filter is pushed without a query: " + physical, physical.contains("filter{"));
            Map<String, Object> plan = fragmentPlanOf(body);
            assertEquals("PUSHED_SCAN", plan.get("kind"));
            assertFalse("no filter SQL without a query: " + body, plan.containsKey("filter_sql"));
            assertFalse("no Lance clause on a scalar shape: " + body, plan.containsKey("lance_clause"));
            @SuppressWarnings("unchecked")
            Map<String, Object> aggregate = (Map<String, Object>) plan.get("aggregate");
            assertEquals(0, aggregate.get("group_count"));
            assertEquals(List.of(Map.of("name", "s", "kind", "SUM")), aggregate.get("metrics"));
            assertTrue("the Substrait length is reported: " + body, ((Number) aggregate.get("substrait_bytes")).intValue() > 0);
            assertFalse("nothing unplanned: " + body, parseJson(body).containsKey("unplanned"));
            assertEquals(List.of(), listOf(body, "refinements_possible"));

            String filteredBody = explainOk(
                indexName,
                "{\"size\":0,\"query\":{\"term\":{\"id\":3}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
            );
            String filteredLogical = stringPath(filteredBody, "logical");
            assertTrue("logical plan carries the filter: " + filteredLogical, filteredLogical.contains("LogicalFilter"));
            // The aggregate rule fires on Aggregate(Filter(scan)) and
            // rebuilds the filter inside the pushed aggregate's input;
            // the filter's Lance SQL rides on the pushed aggregate, so
            // the physical plan carries the scan with the aggregate and
            // its filter visible as one pushed operation.
            String filteredPhysical = stringPath(filteredBody, "physical");
            assertTrue(
                "the aggregate is pushed onto the filtered scan: " + filteredPhysical,
                filteredPhysical.contains("pushed=[[aggregate{")
            );
            assertTrue("the pushed aggregate carries the filter SQL: " + filteredPhysical, filteredPhysical.contains("filter=id = 3"));
            assertFalse("the filter left the physical plan: " + filteredPhysical, filteredPhysical.contains("LogicalFilter"));
            assertEquals("id = 3", fragmentPlanOf(filteredBody).get("filter_sql"));

            String bucketBody = explainOk(
                indexName,
                "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
            );
            String bucketLogical = stringPath(bucketBody, "logical");
            assertTrue("bucket plan carries the aggregate: " + bucketLogical, bucketLogical.contains("LanceAggregate"));
            assertTrue("bucket plan carries the bucket spec: " + bucketLogical, bucketLogical.contains("TERMS{name=by_id"));
            assertTrue("bucket plan carries the metric spec: " + bucketLogical, bucketLogical.contains("AVG{name=a}"));
            String bucketPhysical = stringPath(bucketBody, "physical");
            assertTrue(
                "the pushed aggregate appears in the bucket physical plan: " + bucketPhysical,
                bucketPhysical.contains("pushed=[[aggregate{")
            );
            assertTrue("the pushed shape names the bucket: " + bucketPhysical, bucketPhysical.contains("TERMS{name=by_id"));
            @SuppressWarnings("unchecked")
            Map<String, Object> bucketAggregate = (Map<String, Object>) fragmentPlanOf(bucketBody).get("aggregate");
            assertEquals(1, bucketAggregate.get("group_count"));
            assertEquals(List.of(Map.of("name", "a", "kind", "AVG")), bucketAggregate.get("metrics"));

            // A bucket tree over a query filter folds into one scan as
            // well: the pushed aggregate carries the bucket and the
            // filter's SQL, the plan the fragment executor runs for the
            // same request, instead of the Lucene operator.
            String filteredBucketPhysical = stringPath(
                explainOk(indexName, "{\"size\":0,\"query\":{\"term\":{\"id\":3}},\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\"}}}}"),
                "physical"
            );
            assertTrue(
                "the pushed aggregate appears in the physical plan: " + filteredBucketPhysical,
                filteredBucketPhysical.contains("pushed=[[aggregate{")
            );
            assertTrue("the pushed shape names the bucket: " + filteredBucketPhysical, filteredBucketPhysical.contains("TERMS{name=by_id"));
            assertTrue(
                "the pushed aggregate carries the filter SQL: " + filteredBucketPhysical,
                filteredBucketPhysical.contains("filter=id = 3")
            );
            assertFalse(
                "the Lucene operator does not answer the shape: " + filteredBucketPhysical,
                filteredBucketPhysical.contains("LuceneAggregateExec(")
            );

            // A cardinality metric folds into the scan like any other
            // tree but the pushed form is priced above the aggregator
            // (it is slower), so the physical plan shows the Lucene
            // aggregate operator over the bare scan and the fragment
            // plan runs the aggregators; the cost chose, nothing was
            // refused.
            String cardinalityBody = explainOk(indexName, "{\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"id\"}}}}");
            String cardinalityLogical = stringPath(cardinalityBody, "logical");
            assertTrue(
                "logical plan carries the cardinality spec: " + cardinalityLogical,
                cardinalityLogical.contains("CARDINALITY{name=u}")
            );
            String cardinalityPhysical = stringPath(cardinalityBody, "physical");
            assertTrue(
                "the Lucene aggregate operator appears in the physical plan: " + cardinalityPhysical,
                cardinalityPhysical.contains("LuceneAggregateExec(")
            );
            assertFalse("nothing is pushed into the scan: " + cardinalityPhysical, cardinalityPhysical.contains("pushed=[["));
            Map<String, Object> cardinalityPlan = fragmentPlanOf(cardinalityBody);
            assertEquals("LUCENE_AGGREGATE", cardinalityPlan.get("kind"));
            assertFalse("no aggregate travels to the scan: " + cardinalityBody, cardinalityPlan.containsKey("aggregate"));
            assertFalse("the cost chose, nothing was refused: " + cardinalityBody, parseJson(cardinalityBody).containsKey("unplanned"));

            // A page mixing the score order with a column collation
            // stays on Lucene's collector, so the physical plan shows
            // the heap top-k operator and the fragment plan carries the
            // full text clause without a pushed page.
            String mixedSortBody = explainOk(
                indexName,
                "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"id\":\"asc\"}]}"
            );
            String mixedSortPhysical = stringPath(mixedSortBody, "physical");
            assertTrue("the coordinator merge leads: " + mixedSortPhysical, mixedSortPhysical.startsWith("MergeExec(reduce=[HITS_TOP_K])"));
            assertTrue(
                "the heap top-k operator appears in the physical plan: " + mixedSortPhysical,
                mixedSortPhysical.contains("HeapTopKExec(")
            );
            assertTrue("the operator carries the FTS clause: " + mixedSortPhysical, mixedSortPhysical.contains("fts="));
            Map<String, Object> mixedSortPlan = fragmentPlanOf(mixedSortBody);
            assertEquals("LUCENE_TOPK", mixedSortPlan.get("kind"));
            assertEquals("lance_match", mixedSortPlan.get("lance_clause"));
            assertFalse("no pushed page: " + mixedSortBody, mixedSortPlan.containsKey("top_k"));

            // A pushed sorted page: the fragment plan carries the
            // orderings and the fetch the scan cuts the page with.
            String pageBody = explainOk(indexName, "{\"size\":3,\"query\":{\"range\":{\"id\":{\"gte\":2}}},\"sort\":[{\"id\":\"desc\"}]}");
            assertTrue(stringPath(pageBody, "physical").contains("topk{"));
            Map<String, Object> pagePlan = fragmentPlanOf(pageBody);
            assertEquals("PUSHED_SCAN", pagePlan.get("kind"));
            assertEquals("id >= 2", pagePlan.get("filter_sql"));
            @SuppressWarnings("unchecked")
            Map<String, Object> topK = (Map<String, Object>) pagePlan.get("top_k");
            assertEquals(3, topK.get("fetch"));
            assertEquals(List.of(Map.of("column", "id", "ascending", false, "nulls_first", false)), topK.get("orderings"));
            assertFalse("a first page has no cursor: " + pageBody, topK.containsKey("cursor_sql"));

            // An aggregation the translator refuses (a sum over a text
            // column) runs on the aggregators; the answer names it.
            String refusedBody = explainOk(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"body\"}}}}");
            assertEquals("LUCENE_AGGREGATE", fragmentPlanOf(refusedBody).get("kind"));
            String unplanned = stringPath(refusedBody, "unplanned");
            assertTrue("the refusal names the aggregation: " + unplanned, unplanned.contains("body"));

            // A page next to aggregations runs both through Lucene, as
            // the runtime does; the strict envelope's 400 is gone.
            String pageWithAggregations = explainOk(indexName, "{\"size\":5,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");
            assertEquals("LUCENE_AGGREGATE", fragmentPlanOf(pageWithAggregations).get("kind"));
            assertEquals("size [5] (only 0 with aggregations)", stringPath(pageWithAggregations, "unplanned"));

            // An aggregation off the pushdown shapes (top_hits) reaches
            // the translator, which refuses it by name: the aggregators
            // run over the bare scan. The dispatch filter's allow list,
            // which sends this body to the shard path, is not part of
            // the plan and not of the route.
            String topHits = explainOk(indexName, "{\"size\":0,\"aggs\":{\"t\":{\"top_hits\":{\"size\":1}}}}");
            assertEquals("fragment", stringPath(topHits, "route"));
            assertEquals("LUCENE_AGGREGATE", fragmentPlanOf(topHits).get("kind"));
            assertEquals("aggregation type [top_hits]", stringPath(topHits, "unplanned"));
            assertFalse("nothing is pushed into the scan: " + topHits, stringPath(topHits, "physical").contains("pushed=[["));
        } finally {
            deleteQuietly(indexName);
        }
    }

    public void testExplainShowsThePushdownSettingAsACostDecision() throws Exception {
        String suffix = "explain-setting-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String sum = "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
        try {
            attach(tableUri);

            // lance.aggregation.pushdown is a cost input: off, the pushed
            // scan costs infinity and the Lucene operator answers the
            // shape; nothing is refused, so no element is named as
            // unplanned. Back on, the pushed scan wins again.
            Request disable = new Request("PUT", "/_cluster/settings");
            disable.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":false}}");
            client().performRequest(disable);
            try {
                String off = explainOk(indexName, sum);
                String offPhysical = stringPath(off, "physical");
                assertTrue(
                    "the Lucene operator answers with the pushdown off: " + offPhysical,
                    offPhysical.contains("LuceneAggregateExec(")
                );
                assertFalse("nothing is pushed into the scan: " + offPhysical, offPhysical.contains("pushed=[["));
                assertEquals("LUCENE_AGGREGATE", fragmentPlanOf(off).get("kind"));
                assertFalse("the cost chose, nothing was refused: " + off, parseJson(off).containsKey("unplanned"));
            } finally {
                Request enable = new Request("PUT", "/_cluster/settings");
                enable.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":null}}");
                client().performRequest(enable);
            }
            String on = explainOk(indexName, sum);
            assertTrue("the pushed scan answers with the pushdown on: " + on, stringPath(on, "physical").contains("pushed=[[aggregate{"));
            assertEquals("PUSHED_SCAN", fragmentPlanOf(on).get("kind"));
        } finally {
            deleteQuietly(indexName);
        }
    }

    public void testExplainAcceptsTheRuntimeEnvelopeAndReportsTheShardPathRoute() throws Exception {
        String suffix = "explain-env-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            attach(tableUri);

            // post_filter, once refused by the strict envelope, keeps
            // the page on the collector with the filter's SQL kept.
            String postFilterBody = explainOk(
                indexName,
                "{\"size\":3,\"query\":{\"range\":{\"id\":{\"gte\":2}}},\"post_filter\":{\"term\":{\"id\":4}},\"sort\":[{\"id\":\"desc\"}]}"
            );
            assertEquals("fragment", stringPath(postFilterBody, "route"));
            assertTrue(stringPath(postFilterBody, "physical").contains("HeapTopKExec("));
            Map<String, Object> postFilterPlan = fragmentPlanOf(postFilterBody);
            assertEquals("LUCENE_TOPK", postFilterPlan.get("kind"));
            assertEquals("id >= 2", postFilterPlan.get("filter_sql"));

            // from folds into the fetch every executor returns.
            String fromBody = explainOk(indexName, "{\"from\":2,\"size\":3,\"sort\":[{\"id\":\"asc\"}]}");
            @SuppressWarnings("unchecked")
            Map<String, Object> fromTopK = (Map<String, Object>) fragmentPlanOf(fromBody).get("top_k");
            assertEquals(5, fromTopK.get("fetch"));

            // _source filtering shapes the fetch phase only.
            String sourceBody = explainOk(indexName, "{\"size\":2,\"_source\":[\"id\"],\"sort\":[{\"id\":\"asc\"}]}");
            assertEquals("PUSHED_SCAN", fragmentPlanOf(sourceBody).get("kind"));

            // An empty body plans as _search without one: a match_all
            // page of ten.
            Response empty = client().performRequest(new Request("GET", "/" + indexName + "/_lance/explain"));
            assertEquals(RestStatus.OK.getStatus(), empty.getStatusLine().getStatusCode());
            String emptyBody = readAll(empty);
            @SuppressWarnings("unchecked")
            Map<String, Object> emptyTopK = (Map<String, Object>) fragmentPlanOf(emptyBody).get("top_k");
            assertEquals(10, emptyTopK.get("fetch"));

            // A body only the shard path serves: the route says so, the
            // reasons name the element, the physical root is the shard
            // path operator and no fragment plan is shipped.
            String highlightBody = explainOk(indexName, "{\"size\":3,\"highlight\":{\"fields\":{\"body\":{}}}}");
            assertEquals("shard_path", stringPath(highlightBody, "route"));
            assertEquals(List.of("HIGHLIGHT"), listOf(highlightBody, "reasons"));
            String highlightPhysical = stringPath(highlightBody, "physical");
            assertTrue("the shard path operator leads: " + highlightPhysical, highlightPhysical.startsWith("ShardPathFallbackExec("));
            assertTrue("the operator carries the reason: " + highlightPhysical, highlightPhysical.contains("reasons=[[HIGHLIGHT]]"));
            assertTrue(stringPath(highlightBody, "logical").contains("LanceShardPathShape"));
            Map<String, Object> highlight = parseJson(highlightBody);
            assertFalse("no fragment plan on the shard path: " + highlightBody, highlight.containsKey("fragment_plan"));
            assertFalse("no refinements on the shard path: " + highlightBody, highlight.containsKey("refinements_possible"));

            // Two elements, both named, in the order the checks run.
            String twoReasons = explainOk(
                indexName,
                "{\"size\":3,\"query\":{\"term\":{\"id\":1}},\"suggest\":{\"s\":{\"text\":\"hello\",\"term\":{\"field\":\"body\"}}},"
                    + "\"highlight\":{\"fields\":{\"body\":{}}}}"
            );
            assertEquals("shard_path", stringPath(twoReasons, "route"));
            assertEquals(List.of("SUGGEST", "HIGHLIGHT"), listOf(twoReasons, "reasons"));

            // collapse and rescore run on the fragment executors over
            // the Lucene collector's page: the route stays fragment and
            // the plan is the query root alone, with the second pass
            // named as the unplanned element.
            String collapseBody = explainOk(indexName, "{\"size\":3,\"collapse\":{\"field\":\"id\"}}");
            assertEquals("fragment", stringPath(collapseBody, "route"));
            assertEquals("LUCENE_TOPK", fragmentPlanOf(collapseBody).get("kind"));
            assertEquals("rescore or collapse (a second pass over the Lucene collector's page)", stringPath(collapseBody, "unplanned"));
            String rescoreBody = explainOk(
                indexName,
                "{\"size\":3,\"query\":{\"term\":{\"id\":1}},\"rescore\":{\"query\":{\"rescore_query\":{\"term\":{\"id\":2}}}}}"
            );
            assertEquals("fragment", stringPath(rescoreBody, "route"));
            assertEquals("LUCENE_TOPK", fragmentPlanOf(rescoreBody).get("kind"));
            assertEquals("id = 1", fragmentPlanOf(rescoreBody).get("filter_sql"));

            // min_score is served by the executors' collectors: the
            // route stays on the fragment path and the plan is the query
            // root alone, with the knob named as the unplanned element.
            String minScoreBody = explainOk(indexName, "{\"size\":3,\"query\":{\"term\":{\"id\":1}},\"min_score\":0.5}");
            assertEquals("fragment", stringPath(minScoreBody, "route"));
            assertEquals("LUCENE_TOPK", fragmentPlanOf(minScoreBody).get("kind"));
            assertEquals("min_score or terminate_after (applied by the Lucene collectors)", stringPath(minScoreBody, "unplanned"));

            // The refusal the runtime shares: a filtered lance_knn whose
            // filter has no Lance SQL form answers 400 with the same
            // message the coordinator gives a search.
            ResponseException knn = expectThrows(
                ResponseException.class,
                () -> explain(
                    indexName,
                    "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[1,0,0,0,0,0,0,0],\"k\":3,"
                        + "\"filter\":{\"match\":{\"body\":\"hello\"}}}}}"
                )
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), knn.getResponse().getStatusLine().getStatusCode());
            String reason = readAll(knn.getResponse());
            assertTrue("400 body names the filter clause: " + reason, reason.contains("[lance_knn] filter type [MatchQueryBuilder]"));
            assertTrue("400 body is an illegal_argument_exception: " + reason, reason.contains("illegal_argument_exception"));
        } finally {
            deleteQuietly(indexName);
        }
    }

    public void testExplainPredictsTheSortFieldTypeRefinementOnAnIpSort() throws Exception {
        String suffix = "explain-ip-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeIpTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"overrides\":{\"ip\":{\"type\":\"ip\"}}}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The sort over the ip column folds into the scan as a
            // string ordering; the data node's guard refuses the ip
            // format and moves the page to the collector, which the
            // answer predicts.
            String body = explainOk(indexName, "{\"size\":6,\"sort\":[{\"ip\":\"asc\"}]}");
            assertEquals("fragment", stringPath(body, "route"));
            Map<String, Object> plan = fragmentPlanOf(body);
            assertEquals("PUSHED_SCAN", plan.get("kind"));
            @SuppressWarnings("unchecked")
            Map<String, Object> topK = (Map<String, Object>) plan.get("top_k");
            assertEquals(List.of(Map.of("column", "ip", "ascending", true, "nulls_first", false)), topK.get("orderings"));
            assertEquals(List.of("sort_field_type"), listOf(body, "refinements_possible"));

            // The same page over another column predicts nothing.
            String plain = explainOk(indexName, "{\"size\":6,\"sort\":[{\"id\":\"asc\"}]}");
            assertEquals(List.of(), listOf(plain, "refinements_possible"));
        } finally {
            deleteQuietly(indexName);
        }
    }

    public void testExplainFillsThePlannerStatisticsCache() throws Exception {
        String suffix = "explain-stats-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            // The collector logs its duration at debug; raise the
            // level for the run so the node log carries the line.
            Request debug = new Request("PUT", "/_cluster/settings");
            debug.setJsonEntity("{\"transient\":{\"logger.org.opensearch.lance.plan.metadata\":\"DEBUG\"}}");
            client().performRequest(debug);

            attach(tableUri);

            // Nothing has planned against this table yet: the entry
            // count and the collect time are what other tests left
            // behind (zero on a fresh node).
            String before = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            @SuppressWarnings("unchecked")
            Map<String, Object> nodes = (Map<String, Object>) parseJson(before).get("nodes");
            String nodeId = nodes.keySet().iterator().next();
            int tablesBefore = extractIntPath(before, "nodes", nodeId, "plan", "statistics", "tables");
            int millisBefore = extractIntPath(before, "nodes", nodeId, "plan", "statistics", "collect_millis_total");
            assertTrue("the baseline is a counter: " + before, tablesBefore >= 0 && millisBefore >= 0);

            explainOk(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");

            // The plan construction collected the table's statistics
            // under the snapshot's version: one more entry, and the
            // collect time grew (a collection counts at least one
            // millisecond however fast it ran).
            String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            int tables = extractIntPath(stats, "nodes", nodeId, "plan", "statistics", "tables");
            int millis = extractIntPath(stats, "nodes", nodeId, "plan", "statistics", "collect_millis_total");
            assertEquals("the explained table is cached: " + stats, tablesBefore + 1, tables);
            assertTrue("the first explain collected: " + millisBefore + " -> " + millis, millis > millisBefore);

            // A second explain of the same version reads the cached
            // entry: neither the entry count nor the collect time moves.
            explainOk(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");
            String again = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            assertEquals(tables, extractIntPath(again, "nodes", nodeId, "plan", "statistics", "tables"));
            assertEquals(millis, extractIntPath(again, "nodes", nodeId, "plan", "statistics", "collect_millis_total"));
        } finally {
            try {
                Request reset = new Request("PUT", "/_cluster/settings");
                reset.setJsonEntity("{\"transient\":{\"logger.org.opensearch.lance.plan.metadata\":null}}");
                client().performRequest(reset);
            } catch (Exception ignored) {}
            deleteQuietly(indexName);
        }
    }

    public void testExplainUnknownIndexIs404() {
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> explain("no-such-index", "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}")
        );
        assertEquals(RestStatus.NOT_FOUND.getStatus(), failure.getResponse().getStatusLine().getStatusCode());
    }

    public void testExplainNonLanceIndexIs400() throws Exception {
        String indexName = "plain-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity("{}");
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(create).getStatusLine().getStatusCode());
        try {
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> explain(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}")
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), failure.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(failure.getResponse()).contains("is not a Lance index"));
        } finally {
            deleteQuietly(indexName);
        }
    }
}
