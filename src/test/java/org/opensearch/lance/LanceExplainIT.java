/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The explain endpoint: a supported body answers the logical plan, an
 * unsupported body answers 400 with the translator's message, an
 * unknown index 404, and a non Lance index 400. Nothing here executes
 * a search through the planner.
 */
public class LanceExplainIT extends LanceRestTestCase {

    private static Response explain(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    public void testExplainAnswersLogicalPlanAndRejectsUnsupportedShapes() throws Exception {
        String suffix = "explain-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            Response ok = explain(indexName, "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");
            assertEquals(RestStatus.OK.getStatus(), ok.getStatusLine().getStatusCode());
            String body = readAll(ok);
            assertEquals(indexName, stringPath(body, "index"));
            String logical = stringPath(body, "logical");
            assertTrue("logical plan carries the aggregate: " + logical, logical.contains("LanceAggregate"));
            assertTrue("logical plan carries the scan: " + logical, logical.contains("LanceTableScan"));
            assertTrue("the aggregation name is the output alias: " + logical, logical.contains("s=[SUM("));
            String physical = stringPath(body, "physical");
            assertTrue("physical root is the scan with the pushed aggregate: " + physical, physical.startsWith("LanceTableScan("));
            assertTrue("physical plan names the pushed aggregate: " + physical, physical.contains("pushed=[[aggregate{"));
            assertFalse("no filter is pushed without a query: " + physical, physical.contains("filter{"));

            Response filtered = explain(
                indexName,
                "{\"size\":0,\"query\":{\"term\":{\"id\":3}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), filtered.getStatusLine().getStatusCode());
            String filteredBody = readAll(filtered);
            String filteredLogical = stringPath(filteredBody, "logical");
            assertTrue("logical plan carries the filter: " + filteredLogical, filteredLogical.contains("LogicalFilter"));
            // The aggregate rule fires on Aggregate(Filter(scan)) and
            // rebuilds the filter inside the pushed aggregate's input,
            // so the physical root is the scan with only the aggregate
            // visible as a pushed operation; the filter is absorbed.
            String filteredPhysical = stringPath(filteredBody, "physical");
            assertTrue("filtered physical root is the scan: " + filteredPhysical, filteredPhysical.startsWith("LanceTableScan("));
            assertTrue(
                "the aggregate is pushed onto the filtered scan: " + filteredPhysical,
                filteredPhysical.contains("pushed=[[aggregate{")
            );
            assertFalse("the filter left the physical plan: " + filteredPhysical, filteredPhysical.contains("LogicalFilter"));

            Response bucket = explain(
                indexName,
                "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), bucket.getStatusLine().getStatusCode());
            String bucketBody = readAll(bucket);
            String bucketLogical = stringPath(bucketBody, "logical");
            assertTrue("bucket plan carries the aggregate: " + bucketLogical, bucketLogical.contains("LanceAggregate"));
            assertTrue("bucket plan carries the bucket spec: " + bucketLogical, bucketLogical.contains("TERMS{name=by_id"));
            assertTrue("bucket plan carries the metric spec: " + bucketLogical, bucketLogical.contains("AVG{name=a}"));
            String bucketPhysical = stringPath(bucketBody, "physical");
            assertTrue(
                "bucket physical root is the scan with the pushed aggregate: " + bucketPhysical,
                bucketPhysical.startsWith("LanceTableScan(") && bucketPhysical.contains("pushed=[[aggregate{")
            );
            assertTrue("the pushed shape names the bucket: " + bucketPhysical, bucketPhysical.contains("TERMS{name=by_id"));

            // A bucket tree over a query filter has no aggregate
            // pushdown operand, so the planner answers with the Lucene
            // convention operator over the bare scan instead of the
            // pushed scan or the logical fallback.
            Response filteredBucket = explain(
                indexName,
                "{\"size\":0,\"query\":{\"term\":{\"id\":3}},\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), filteredBucket.getStatusLine().getStatusCode());
            String filteredBucketPhysical = stringPath(readAll(filteredBucket), "physical");
            assertTrue(
                "the Lucene aggregate operator is the physical root: " + filteredBucketPhysical,
                filteredBucketPhysical.startsWith("LuceneAggregateExec(")
            );
            assertTrue("the operator runs over the scan: " + filteredBucketPhysical, filteredBucketPhysical.contains("LanceTableScan"));
            assertFalse("nothing is pushed into the scan: " + filteredBucketPhysical, filteredBucketPhysical.contains("pushed=[["));

            // A page mixing the score order with a column collation
            // stays on Lucene's collector, so the physical plan shows
            // the heap top-k operator.
            Response mixedSortPage = explain(
                indexName,
                "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"id\":\"asc\"}]}"
            );
            assertEquals(RestStatus.OK.getStatus(), mixedSortPage.getStatusLine().getStatusCode());
            String mixedSortPhysical = stringPath(readAll(mixedSortPage), "physical");
            assertTrue("the heap top-k operator is the physical root: " + mixedSortPhysical, mixedSortPhysical.startsWith("HeapTopKExec("));
            assertTrue("the operator carries the FTS clause: " + mixedSortPhysical, mixedSortPhysical.contains("fts="));

            ResponseException terms = expectThrows(
                ResponseException.class,
                () -> explain(indexName, "{\"size\":0,\"aggs\":{\"t\":{\"top_hits\":{\"size\":1}}}}")
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), terms.getResponse().getStatusLine().getStatusCode());
            String reason = readAll(terms.getResponse());
            assertTrue("400 body names the aggregation type: " + reason, reason.contains("aggregation type [top_hits]"));
            assertTrue("400 body is an illegal_argument_exception: " + reason, reason.contains("illegal_argument_exception"));

            ResponseException hits = expectThrows(
                ResponseException.class,
                () -> explain(indexName, "{\"size\":5,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}")
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), hits.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(hits.getResponse()).contains("size [5] (only 0 with aggregations)"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
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
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
