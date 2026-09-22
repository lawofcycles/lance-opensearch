/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Focused subset of the integTest suite that exercises the paths
 * where multi-node behaviour differs from single-node:
 *
 * <ul>
 *   <li>Coordinator fan-out over multiple data nodes for
 *       fragment-mode search.</li>
 *   <li>Namespace metadata propagation through cluster state.</li>
 *   <li>Manager-routed register / unregister through the transport
 *       action.</li>
 * </ul>
 *
 * <p>The rest of the integTest suite runs against a single-node
 * cluster; only tests that depend on node topology live here, so the
 * three-node cluster does not slow down every run.
 *
 * <p>Filtered in and out through Gradle {@code filter} blocks: the
 * default {@code integTest} task excludes this class, and
 * {@code multiNodeIntegTest} includes only it. Both point at the
 * same test source set to avoid duplicating helper code.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceMultiNodeIT extends OpenSearchRestTestCase {

    private static Path sharedRoot() throws IOException {
        String property = System.getProperty("tests.lance.shared_tables_dir");
        assertNotNull("tests.lance.shared_tables_dir must be set by the Gradle build", property);
        Path base = Path.of(property);
        Files.createDirectories(base);
        return base;
    }

    public void testFragmentDispatchAcrossThreeNodes() throws Exception {
        // The coordinator's fan-out must return the full count and hits
        // when the cluster has three data nodes.
        String suffix = "mn-dispatch-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String body = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            // Every hit is materialised from Lance regardless of
            // which node scanned the fragment.
            assertTrue("expected _rowaddr-derived hit ids: " + body, body.contains("\"_id\":\"0-0\""));
            assertTrue("expected _source rendered from Arrow: " + body, body.contains("hello lance"));

            String sumBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"));
            assertEquals(6, extractIntPath(sumBody, "hits", "total", "value"));
            assertEquals(15.0d, extractDoublePath(sumBody, "aggregations", "s", "value"), 0.0d);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testNestedQueryAcrossThreeNodes() throws Exception {
        // A nested query fans out one fragment per node; each executor
        // builds its own doc id layout (child docs before their parent)
        // and the per-node parent hits merge like any other hits.
        String suffix = "mn-nested-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeNestedTable(scratchDir, tableName, 2);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));

            // red appears in rows 0, 1, 3 and 5, spread over the three
            // fragments (rows 0..1, 2..3, 4..5); red and large sit in the
            // same element only in rows 1 and 5.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"bool\":{\"must\":["
                        + "{\"term\":{\"items.color\":\"red\"}},{\"term\":{\"items.size\":\"large\"}}]}}}},\"sort\":[{\"id\":\"asc\"}]}"
                )
            );
            assertEquals("nested red+large across fragments: " + body, 2, extractIntPath(body, "hits", "total", "value"));
            assertEquals("first hit id: " + body, 1, extractIntPath(body, "hits", "hits", "0", "_source", "id"));
            assertEquals("second hit id: " + body, 5, extractIntPath(body, "hits", "hits", "1", "_source", "id"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testStructTermsAggregationAcrossThreeNodes() throws Exception {
        // A terms aggregation on a struct child (dotted field name) fans
        // out one fragment per node; the per-node responses must reduce
        // like any keyword terms, proving the wire format carries
        // nothing struct specific.
        String suffix = "mn-struct-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStructTable(scratchDir, tableName, 2);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));

            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"regions\":{\"terms\":{\"field\":\"meta.region\",\"order\":{\"_key\":\"asc\"}}}}}"
                )
            );
            assertEquals("struct terms total: " + body, 6, extractIntPath(body, "hits", "total", "value"));
            // Fixture regions: east on rows 0, 2, 3; west on 1, 5; south on 4.
            assertEquals("east bucket: " + body, 3, extractIntPath(body, "aggregations", "regions", "buckets", "0", "doc_count"));
            assertEquals("south bucket: " + body, 1, extractIntPath(body, "aggregations", "regions", "buckets", "1", "doc_count"));
            assertEquals("west bucket: " + body, 2, extractIntPath(body, "aggregations", "regions", "buckets", "2", "doc_count"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testExplainAnswersTheSameFromEveryNode() throws Exception {
        // The explain endpoint builds its schema from cluster state and
        // the shared Lance registry, not from the local shard, so a node
        // that does not host the single shard copy answers the same
        // logical plan as the one that does.
        String suffix = "mn-explain-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            Set<String> answers = new HashSet<>();
            for (HttpHost host : getClusterHosts()) {
                try (var pinned = buildClient(restClientSettings(), new HttpHost[] { host })) {
                    Request explain = new Request("GET", "/" + indexName + "/_lance/explain");
                    explain.setJsonEntity("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}");
                    Response response = pinned.performRequest(explain);
                    assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
                    String body = readAll(response);
                    assertTrue("logical plan carries the aggregate on " + host + ": " + body, body.contains("LanceAggregate"));
                    assertTrue("logical plan carries the scan on " + host + ": " + body, body.contains("LanceTableScan"));
                    answers.add(body);
                }
            }
            assertEquals("every node answers the same explain body, saw " + answers, 1, answers.size());
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFtsAcrossFragmentsOnThreeNodeCluster() throws Exception {
        // 12 rows written 4 per file give fragments 0, 1 and 2. The
        // coordinator sends one fragment to each of the three data
        // nodes (only one of which holds the shard copy), so every FTS
        // scan carries its fragment subset. The hits, their
        // per-fragment _id layout and _count must match what the
        // single-node LanceFtsQueryIT asserts for the same table.
        String suffix = "mn-fts-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));

            String helloBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("even rows across three fragments: " + helloBody, 6, extractIntPath(helloBody, "hits", "total", "value"));
            assertEquals(
                "row i lives at fragment i / 4, offset i % 4",
                Set.of("0-0", "0-2", "1-0", "1-2", "2-0", "2-2"),
                new HashSet<>(hitIds(helloBody))
            );

            String singleBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("token 4 appears in one row only: " + singleBody, 1, extractIntPath(singleBody, "hits", "total", "value"));
            assertEquals(List.of("1-0"), hitIds(singleBody));

            String countBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("_count for hello: " + countBody, 6, extractIntPath(countBody, "count"));
            String singleCountBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("_count for token 4: " + singleCountBody, 1, extractIntPath(singleCountBody, "count"));

            // A scalar filter over the same three executors: id >= 4
            // matches no row of fragment 0 and four rows of each of
            // fragments 1 and 2. The three nodes each count their own
            // fragments and the coordinator sums them, so the answers
            // equal the single node ones in LanceSearchDispatchIT.
            String rangeDefault = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}}}")
            );
            assertEquals("range default bound: " + rangeDefault, 8, extractIntPath(rangeDefault, "hits", "total", "value"));
            assertTrue(rangeDefault.contains("\"relation\":\"eq\""));
            String rangeTrue = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":true}")
            );
            assertEquals("range exact: " + rangeTrue, 8, extractIntPath(rangeTrue, "hits", "total", "value"));
            assertTrue(rangeTrue.contains("\"relation\":\"eq\""));
            // A bound of 5: each matching executor scans with a limit
            // of six, gets its four rows back short of the limit and
            // reports them exact; the coordinator sums to eight, above
            // the bound, and answers the bound with gte.
            String rangeBound = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":5}")
            );
            assertEquals("range bound 5: " + rangeBound, 5, extractIntPath(rangeBound, "hits", "total", "value"));
            assertTrue(rangeBound.contains("\"relation\":\"gte\""));
            // A bound of 3 is filled on each of the two matching
            // executors (limit four, four rows), so each reports a
            // lower bound and the coordinator answers gte at the bound.
            String rangeLow = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":3}")
            );
            assertEquals("range bound 3: " + rangeLow, 3, extractIntPath(rangeLow, "hits", "total", "value"));
            assertTrue(rangeLow.contains("\"relation\":\"gte\""));
            String rangeCount = readAll(postJson("/" + indexName + "/_count", "{\"query\":{\"range\":{\"id\":{\"gte\":4}}}}"));
            assertEquals("_count for id >= 4: " + rangeCount, 8, extractIntPath(rangeCount, "count"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testUnboundedFtsOnThreeNodesIsRejectedWithTooManyRequestsWhenTheRequestBreakerIsFull() throws Exception {
        // Each of the three executors buffers the hits of its fragment
        // for a sort by a field and reserves them with its node's
        // request breaker. A refusal on any executor travels back to
        // the coordinator as the executor's CircuitBreakingException
        // and the client sees 429 with the reservation's label; the
        // same request answers 200 again once the limit is restored.
        String suffix = "mn-fts-breaker-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String sorted = "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[{\"id\":\"desc\"}]}";
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            String before = readAll(postJson("/" + indexName + "/_search", sorted));
            assertEquals(List.of("2-2", "2-0", "1-2", "1-0", "0-2", "0-0"), hitIds(before));

            updateClusterSetting("indices.breaker.request.limit", "16b");
            try {
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", sorted));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected circuit_breaking_exception: " + body, body.contains("circuit_breaking_exception"));
                assertTrue("expected the hit buffer label: " + body, body.contains("lance_fts_hits"));
            } finally {
                updateClusterSetting("indices.breaker.request.limit", null);
            }
            String after = readAll(postJson("/" + indexName + "/_search", sorted));
            assertEquals(hitIds(before), hitIds(after));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testUnboundedFtsOnThreeNodesIsRefusedByTheAdmissionGate() throws Exception {
        // With the shard share overridden to one byte and the headroom
        // above any host's free memory, every executor's admission gate
        // refuses the unbounded shape before its scan starts. The
        // CircuitBreakingException of whichever executor answers first
        // travels back to the coordinator and the client sees 429 with
        // the admission label; the same shape answers 200 again once
        // the settings are restored.
        String suffix = "mn-fts-admission-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String sorted = "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[{\"id\":\"desc\"}]}";
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            String before = readAll(postJson("/" + indexName + "/_search", sorted));
            assertEquals(List.of("2-2", "2-0", "1-2", "1-0", "0-2", "0-0"), hitIds(before));

            updateClusterSetting("lance.test.index_cache_shard_share", "1b");
            updateClusterSetting("lance.fts.admission.headroom", "1pb");
            try {
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", sorted));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected circuit_breaking_exception: " + body, body.contains("circuit_breaking_exception"));
                assertTrue("expected the admission label: " + body, body.contains("lance_fts_admission"));
            } finally {
                updateClusterSetting("lance.fts.admission.headroom", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }
            String after = readAll(postJson("/" + indexName + "/_search", sorted));
            assertEquals(hitIds(before), hitIds(after));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * The coordinator hands each fragment to a different data node
     * whether or not that node holds a shard copy, and merges three
     * sorted (or scored) lists. The index keeps the default
     * {@code number_of_replicas: 0}, so exactly one node has an
     * {@code IndexService} and the other two build a temporary one
     * from cluster state per request. The expected ids, sort values,
     * totals and buckets are the values a single node returns for
     * this fixture ({@code LanceTableFactory.writeInterleavedTable}
     * interleaves ids across fragments, so a merge that only
     * concatenated per-node lists would reorder every page).
     */
    public void testFanOutAcrossAllDataNodesWithoutShardCopies() throws Exception {
        String suffix = "mn-noshard-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 3;
        int rowsPerFragment = 4;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String[] requests = new String[] {
            "{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"asc\"}],\"size\":10}",
            "{\"sort\":[{\"ts\":\"desc\"}],\"size\":5}",
            "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},\"size\":12}",
            "{\"from\":2,\"size\":3,\"sort\":[{\"id\":\"asc\"}]}",
            "{\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}" };
        try {
            // Surface the executor's per-request IndexService timing in
            // the cluster log so the cost of the temporary IndexService
            // can be read from build/testclusters/*/logs.
            updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", "DEBUG");
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));
            assertEquals("attach keeps a single shard copy", 1, activeShards(indexName));
            String settingsBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertFalse("attach must not expand replicas: " + settingsBody, settingsBody.contains("auto_expand_replicas"));
            assertTrue("attach keeps number_of_replicas 0: " + settingsBody, settingsBody.contains("\"number_of_replicas\":\"0\""));

            List<Map<String, Object>> responses = new ArrayList<>();
            for (String request : requests) {
                responses.add(parse(readAll(postJson("/" + indexName + "/_search", request))));
            }
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sourceIds(responses.get(0)));
            assertEquals(List.of(11, 10, 9, 8, 7), sourceIds(responses.get(1)));
            // BM25 grows with the term frequency, which is id + 1.
            assertEquals(List.of(11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0), sourceIds(responses.get(2)));
            assertEquals(List.of(2, 3, 4), sourceIds(responses.get(3)));
            assertEquals(3, buckets(responses.get(4)).size());
            for (Map<String, Object> response : responses) {
                assertEquals(fragments * rowsPerFragment, extractIntPath(response, "hits", "total", "value"));
            }
            // Sort values travel with the hits through the merge.
            List<Object> idSortValues = sortValues(responses.get(0));
            for (int i = 0; i < idSortValues.size(); i++) {
                assertEquals(List.of(i), idSortValues.get(i));
            }
            // Every category bucket is a full merge of the three
            // per-node partials: 12 rows over 3 categories.
            long bucketDocs = 0;
            for (Map<String, Object> bucket : buckets(responses.get(4))) {
                bucketDocs += ((Number) bucket.get("doc_count")).longValue();
            }
            assertEquals(fragments * rowsPerFragment, bucketDocs);
            // The scored request must come back in strictly descending
            // score order after the merge; every row has a distinct
            // term frequency so no two scores tie.
            List<Double> scores = scores(responses.get(2));
            for (int i = 1; i < scores.size(); i++) {
                assertTrue("scores not descending: " + scores, scores.get(i - 1) > scores.get(i));
            }
            // The same shapes answer identically on repeat, so the
            // temporary IndexService leaves nothing behind that changes
            // the next request.
            for (int i = 0; i < requests.length; i++) {
                Map<String, Object> again = parse(readAll(postJson("/" + indexName + "/_search", requests[i])));
                assertEquals(requests[i], sourceIds(responses.get(i)), sourceIds(again));
                assertEquals(requests[i], responses.get(i).get("aggregations"), again.get("aggregations"));
            }
            // Matching results would also hold if every fragment ran on
            // the one node that holds the shard, so read the cluster
            // logs: the coordinator names the node of each fragment
            // assignment, and a node that had to build a temporary
            // IndexService logs that it did. With one fragment per data
            // node every data node must appear in the fan-out, and every
            // data node except the single shard host must have built a
            // temporary IndexService.
            int dataNodes = dataNodeCount();
            assertEquals("fixture assumes one fragment per data node", fragments, dataNodes);
            assertBusy(() -> {
                Set<String> fanOutNodes = new HashSet<>();
                Set<String> temporaryIndexServiceNodes = new HashSet<>();
                for (String line : clusterLogLines()) {
                    if (!line.contains("[" + indexName + "]")) {
                        continue;
                    }
                    int at = line.indexOf(" to node [");
                    if (line.contains("lance.dispatch: fan-out index") && at >= 0) {
                        int close = line.indexOf(']', at + " to node [".length());
                        fanOutNodes.add(line.substring(at + " to node [".length(), close));
                    }
                    if (line.contains("lance.dispatch: temporary IndexService for")) {
                        temporaryIndexServiceNodes.add(loggingNodeName(line));
                    }
                }
                assertEquals("fragments went to " + fanOutNodes, dataNodes, fanOutNodes.size());
                assertEquals(
                    "temporary IndexService built on " + temporaryIndexServiceNodes,
                    dataNodes - 1,
                    temporaryIndexServiceNodes.size()
                );
            });
        } finally {
            try {
                updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", null);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Attach is routed to the elected cluster manager, so a request that
     * lands on any other node has to succeed as well. The round-robin
     * {@link #client()} does not say which node answered, so this test
     * pins a REST client to one node that is not the manager, attaches
     * through it, and then checks from the shared client that the index
     * exists cluster-wide. A repeated attach through the same node has to
     * report {@code already_attached}, which exercises the existing-index
     * lookup on the manager too.
     *
     * <p>HTTP 200 alone would also hold for a locally executed attach
     * (without a security plugin the internal create-index header
     * survives transport forwarding), so the test reads the cluster logs
     * as well: the transport action logs the node it runs on at DEBUG,
     * and that node has to be the manager for both attach calls.
     */
    public void testAttachThroughNonManagerNode() throws Exception {
        String suffix = "mn-attach-follower-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String managerName = clusterManagerNodeName();
        HttpHost follower = null;
        String followerName = null;
        for (HttpHost host : getClusterHosts()) {
            String name = localNodeName(host);
            if (!managerName.equals(name)) {
                follower = host;
                followerName = name;
                break;
            }
        }
        assertNotNull("a three-node cluster has at least one node that is not the cluster manager [" + managerName + "]", follower);
        try (var followerClient = buildClient(restClientSettings(), new HttpHost[] { follower })) {
            updateClusterSetting("logger.org.opensearch.lance.attach.TransportLanceAttachAction", "DEBUG");
            Request attach = new Request("POST", "/_lance/attach");
            attach.setJsonEntity("{\"table\":\"" + tableUri + "\"}");
            Response first = followerClient.performRequest(attach);
            assertEquals(RestStatus.OK.getStatus(), first.getStatusLine().getStatusCode());
            String firstBody = readAll(first);
            assertEquals(6, extractIntPath(firstBody, "rows"));
            assertTrue("first attach creates the index: " + firstBody, firstBody.contains("\"already_attached\":false"));

            Response second = followerClient.performRequest(attach);
            assertEquals(RestStatus.OK.getStatus(), second.getStatusLine().getStatusCode());
            String secondBody = readAll(second);
            assertTrue("repeated attach is idempotent: " + secondBody, secondBody.contains("\"already_attached\":true"));

            String body = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));

            // Both attach calls entered the cluster on the follower and
            // must have executed on the manager: every "attaching table"
            // line for this index names the manager and nothing else.
            // Deduplicated into a set because each node writes the same
            // line to its main log and its stdout log.
            String expectedFollower = followerName;
            assertBusy(() -> {
                Set<String> executingNodes = new HashSet<>();
                for (String line : clusterLogLines()) {
                    if (line.contains("lance.attach: attaching table") && line.contains("as index [" + indexName + "]")) {
                        executingNodes.add(loggingNodeName(line));
                    }
                }
                assertFalse("no attach log line for [" + indexName + "] in the cluster logs yet", executingNodes.isEmpty());
                assertEquals(
                    "attach sent to [" + expectedFollower + "] must run on the manager, saw " + executingNodes,
                    Set.of(managerName),
                    executingNodes
                );
            });
        } finally {
            try {
                updateClusterSetting("logger.org.opensearch.lance.attach.TransportLanceAttachAction", null);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Every FTS shape answered by executors that hold a proper subset
     * of the fragments must equal the answer over the whole table. The
     * oracle is the shard path: {@code "explain": true} routes the same
     * request to the one shard, whose reader holds every fragment, so
     * its Lance scan runs unrestricted on one node. Compared per shape:
     * hit ids and order, scores, sort values, {@code hits.total},
     * and terms buckets. The interleaved fixture gives every row a
     * distinct score for {@code lance} and a unique token
     * {@code tok<i>}, so a top 10 has no tie at its boundary. The
     * shapes with {@code track_total_hits: 50} sit below the 300
     * matches of {@code lance}, so the bounded count on a subset
     * executor fills its scan and the merged total must be the bound
     * with {@code gte}.
     */
    public void testFtsOnSubsetExecutorsMatchesWholeTableAnswer() throws Exception {
        String suffix = "mn-fts-subset-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 3;
        int rowsPerFragment = 100;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String oneHit = "{\"lance_match\":{\"field\":\"body\",\"query\":\"tok137\"}}";
        String manyHits = "{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}";
        String terms = "\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}";
        String filtered = "{\"bool\":{\"must\":[" + manyHits + "],\"filter\":[{\"term\":{\"category\":\"c1\"}}]}}";
        // Below the 300 matches of manyHits, so the bounded count on
        // every subset executor fills its scan and hits.total is a
        // lower bound.
        String bound = "\"track_total_hits\":50";
        List<String> shapes = List.of(
            "\"size\":10,\"query\":" + oneHit,
            "\"size\":10,\"query\":" + manyHits,
            "\"size\":10,\"query\":" + oneHit + ",\"sort\":[{\"ts\":\"desc\"}]",
            "\"size\":10,\"query\":" + manyHits + ",\"sort\":[{\"ts\":\"desc\"}]",
            "\"size\":10,\"query\":" + filtered,
            "\"size\":10,\"track_total_hits\":true,\"query\":" + manyHits,
            "\"size\":0,\"query\":" + oneHit + "," + terms,
            "\"size\":0,\"query\":" + manyHits + "," + terms,
            "\"size\":10," + bound + ",\"query\":" + manyHits,
            "\"size\":0," + bound + ",\"query\":" + manyHits,
            "\"size\":10," + bound + ",\"query\":" + manyHits + ",\"sort\":[{\"ts\":\"desc\"}]",
            "\"size\":10," + bound + ",\"query\":" + oneHit
        );
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            for (String shape : shapes) {
                assertFragmentPathMatchesShardPath(indexName, shape);
            }
            // Analytic check of the bare top 10, independent of the
            // oracle: the score grows with the id, row i is
            // (i % 3)-(i / 3).
            Map<String, Object> top = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(1) + "}")));
            assertEquals(List.of(299, 298, 297, 296, 295, 294, 293, 292, 291, 290), sourceIds(top));
            assertEquals(fragments * rowsPerFragment, extractIntPath(top, "hits", "total", "value"));
            Map<String, Object> single = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(0) + "}")));
            assertEquals(List.of(137), sourceIds(single));
            assertEquals(1, extractIntPath(single, "hits", "total", "value"));

            // Analytic check of the bounded total, independent of the
            // oracle. 300 rows match and the bound is 50, so every
            // executor's count scan (limit 51 over the whole table)
            // fills up while its own share of those 51 rows is about
            // a third; the executor must report the bound from the
            // filled scan, not from its share, or the coordinator
            // would sum the shares and answer eq with a value that is
            // neither the total nor the bound.
            for (String shape : shapes.subList(8, 11)) {
                Map<String, Object> bounded = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
                assertEquals(shape, 50, extractIntPath(bounded, "hits", "total", "value"));
                assertEquals(shape, "gte", relation(bounded));
            }
            Map<String, Object> boundedSingle = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(11) + "}")));
            assertEquals(1, extractIntPath(boundedSingle, "hits", "total", "value"));
            assertEquals("eq", relation(boundedSingle));

            // With the probe limit below the match count, the shapes
            // that need every match repeat their scan restricted to the
            // node's fragments and must still agree with the oracle.
            updateClusterSetting("lance.fts.subset_probe_limit", "50");
            for (String shape : shapes) {
                assertFragmentPathMatchesShardPath(indexName, shape);
            }

            // The coordinator logs the fragments it hands to each node;
            // every data node must have received a proper subset, or the
            // requests above never exercised the subset path.
            int dataNodes = dataNodeCount();
            assertEquals("fixture assumes one fragment per data node", fragments, dataNodes);
            assertBusy(() -> {
                Map<String, String> assignments = fanOutAssignments(indexName);
                assertEquals("fragments went to " + assignments, dataNodes, assignments.size());
                for (Map.Entry<String, String> assignment : assignments.entrySet()) {
                    assertEquals(
                        "node " + assignment.getKey() + " got " + assignment.getValue(),
                        1,
                        assignment.getValue().split(",").length
                    );
                }
            });
        } finally {
            try {
                updateClusterSetting("lance.fts.subset_probe_limit", null);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Hits with equal scores or equal sort values come back in the same
     * order from three executors as from one reader over the whole
     * table. The oracle is again the shard path ({@code "explain":
     * true}), whose Lucene collectors break ties by doc id, which on the
     * whole-table reader is fragment order then offset. The doc value
     * fixture has ties everywhere: {@code lance} is repeated
     * {@code (i % 5) + 1} times so sixty rows over the six fragments
     * share the top score, {@code flag} and {@code category} take two
     * and three distinct values. Six fragments over three nodes put
     * fragments 0 and 3 on the first node, 1 and 4 on the second, 2 and
     * 5 on the third, so a merge that fell back to node order would
     * list the ties of fragment 3 before those of fragment 1. Every
     * page below crosses a tie group, and the {@code from} and
     * {@code search_after} pages start inside one; ids and, where the
     * request has them, sort values must agree hit for hit.
     *
     * <p>The score-ordered shapes carry an explicit {@code _score} sort.
     * Without one the executor clips the Lance FTS scan to the page
     * size, and which of the tied rows survive that clip is decided
     * inside Lance, not by row address, so the shard path is not the
     * oracle for the bare shape; that shape is checked for the merge
     * order alone at the end.
     */
    public void testTiedHitsOrderMatchesWholeTableAnswer() throws Exception {
        String suffix = "mn-ties-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 6;
        int rowsPerFragment = 50;
        LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String tied = "{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}";
        String filtered = "{\"bool\":{\"must\":[" + tied + "],\"filter\":[{\"term\":{\"category\":\"c1\"}}]}}";
        String byScore = ",\"sort\":[{\"_score\":\"desc\"}]";
        String byCategoryThenScore = ",\"sort\":[{\"category\":\"asc\"},{\"_score\":\"desc\"}]";
        List<String> shapes = List.of(
            "\"size\":10,\"query\":" + tied + byScore,
            "\"from\":5,\"size\":5,\"query\":" + tied + byScore,
            "\"from\":55,\"size\":10,\"query\":" + tied + byScore,
            "\"size\":10,\"query\":" + filtered + byScore,
            "\"from\":3,\"size\":10,\"query\":" + filtered + byScore,
            "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"flag\":\"desc\"}]",
            "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"rating\":\"desc\"}]"
        );
        // A _score clause next to a field clause: the shard path copies
        // the score sort value into _score, the fragment path does not,
        // so these compare everything but the per-hit _score.
        List<String> mixedShapes = List.of(
            "\"size\":10,\"query\":" + tied + byCategoryThenScore,
            "\"from\":7,\"size\":10,\"query\":" + tied + byCategoryThenScore
        );
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            for (String shape : shapes) {
                assertFragmentPathMatchesShardPath(indexName, shape);
            }
            for (String shape : mixedShapes) {
                assertFragmentPathMatchesShardPath(indexName, shape, false);
            }
            // The top ten of the score sort are the first ten rows with
            // i % 5 == 4, all in fragment 0, and the page starting at
            // 55 crosses from the last fragment into the next score
            // group, which starts in fragment 0 again.
            Map<String, Object> top = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(0) + "}")));
            assertEquals(List.of(4, 9, 14, 19, 24, 29, 34, 39, 44, 49), sourceIds(top));
            Map<String, Object> crossing = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(2) + "}")));
            assertEquals(List.of(279, 284, 289, 294, 299, 3, 8, 13, 18, 23), sourceIds(crossing));

            // A _doc sort orders by fragment then offset on the whole
            // table reader, and the fragment path has to agree on the
            // ids; its per-node doc ids in the sort array differ from
            // the shard path's, so only the ids are compared.
            for (String order : List.of("asc", "desc")) {
                String shape = "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"_score\":\"desc\"},{\"_doc\":\"" + order + "\"}]";
                Map<String, Object> fragmentPath = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
                Map<String, Object> shardPath = parse(readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}")));
                assertEquals(shape, hitIdsOf(shardPath), hitIdsOf(fragmentPath));
            }

            // search_after with a cursor on a tied value skips the whole
            // tie group on both paths, and the next group starts with the
            // lowest row addresses again: after flag 1 (even rows) come
            // the odd rows, fragment by fragment. The cursor is written
            // as the integer the INT sort compares, because the fragment
            // executor hands search_after values to Lucene untyped.
            String withCursor = "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"flag\":\"desc\"}],\"search_after\":[1]";
            assertFragmentPathMatchesShardPath(indexName, withCursor);
            Map<String, Object> secondPage = parse(readAll(postJson("/" + indexName + "/_search", "{" + withCursor + "}")));
            assertEquals(List.of(1, 3, 5, 7, 9, 11, 15, 17, 19, 21), sourceIds(secondPage));

            // Bare shape: the page is ten of the sixty tied rows, and
            // whichever ten the clipped scan kept, the merge lists them
            // by fragment then offset, the same on every request.
            String bare = "{\"size\":10,\"query\":" + tied + "}";
            Map<String, Object> bareFirst = parse(readAll(postJson("/" + indexName + "/_search", bare)));
            List<String> bareIds = hitIdsOf(bareFirst);
            assertEquals(10, bareIds.size());
            List<Double> bareScores = scores(bareFirst);
            for (Double score : bareScores) {
                assertEquals(bareScores.get(0), score, 1e-6d);
            }
            assertRowAddressAscending(bareIds);
            for (int i = 0; i < 3; i++) {
                assertEquals(bareIds, hitIdsOf(parse(readAll(postJson("/" + indexName + "/_search", bare)))));
            }

            // Bounded total over the tie group: 300 rows match and the
            // bound is 20, so every executor's count scan (limit 21 over
            // the whole table) fills with 21 of the sixty tied rows, and
            // which rows Lance returns differs between executors. The
            // own shares can add up to fewer than 21, and the response
            // still has to state the bound itself as the value, as the
            // shard path does whenever the relation is gte.
            String boundedBare = "{\"size\":10,\"track_total_hits\":20,\"query\":" + tied + "}";
            for (int i = 0; i < 3; i++) {
                Map<String, Object> bounded = parse(readAll(postJson("/" + indexName + "/_search", boundedBare)));
                assertEquals(20, extractIntPath(bounded, "hits", "total", "value"));
                assertEquals("gte", relation(bounded));
            }
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Wildcard, regexp and prefix on the {@code lance_text} column and
     * on the keyword column, fanned out over three executors that each
     * hold two of the six fragments. The coordinator's SQL drives both
     * the count only scan and the per executor hits scan, so the
     * totals, the hits and the pages have to be those of the single
     * shard path. Rows are {@code "hello tok<i> grp<i % 25> sp<i %
     * 625> lance..."} for {@code i} in 0..599.
     */
    public void testWildcardRegexpPrefixAcrossThreeNodesMatchWholeTableAnswer() throws Exception {
        String suffix = "mn-pattern-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 6;
        int rowsPerFragment = 100;
        LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String grp3 = "{\"wildcard\":{\"body\":{\"value\":\"*grp3 *\"}}}";
        String byId = ",\"sort\":[{\"id\":\"asc\"}]";
        List<String> shapes = List.of(
            "\"size\":10,\"query\":" + grp3,
            "\"size\":10,\"query\":" + grp3 + byId,
            "\"from\":5,\"size\":10,\"query\":" + grp3 + byId,
            "\"size\":0,\"query\":" + grp3,
            "\"size\":10,\"query\":{\"wildcard\":{\"body\":{\"value\":\"*GRP3 *\",\"case_insensitive\":true}}}" + byId,
            "\"size\":10,\"query\":{\"regexp\":{\"body\":{\"value\":\"hello tok[0-9] .*\"}}}" + byId,
            "\"size\":10,\"query\":{\"prefix\":{\"body\":{\"value\":\"hello tok1\"}}}" + byId,
            "\"size\":10,\"query\":{\"bool\":{\"filter\":[" + grp3 + ",{\"range\":{\"id\":{\"gte\":300}}}]}}" + byId,
            "\"size\":10,\"query\":{\"prefix\":{\"category\":{\"value\":\"c1\"}}}" + byId,
            "\"size\":10,\"query\":{\"regexp\":{\"category\":{\"value\":\"c[12]\"}}}" + byId,
            "\"size\":0,\"query\":{\"wildcard\":{\"category\":{\"value\":\"c*\"}}}"
        );
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            for (String shape : shapes) {
                assertFragmentPathMatchesShardPath(indexName, shape);
            }

            Map<String, Object> page = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(0) + "}")));
            assertEquals(24, extractIntPath(page, "hits", "total", "value"));
            assertEquals("eq", relation(page));
            // The bare page is the first ten matches in row address
            // order, whichever executors held their fragments.
            assertEquals(List.of(3, 28, 53, 78, 103, 128, 153, 178, 203, 228), sourceIds(page));

            String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + grp3 + "}"));
            assertEquals(24, extractIntPath(count, "count"));
            String prefixCount = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"prefix\":{\"body\":{\"value\":\"hello tok1\"}}}}")
            );
            assertEquals(111, extractIntPath(prefixCount, "count"));
            String regexpCount = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"regexp\":{\"body\":{\"value\":\"hello tok[0-9] .*\"}}}}")
            );
            assertEquals(10, extractIntPath(regexpCount, "count"));

            // A Lucene only regexp operator is refused with 400: the
            // coordinator cannot translate it, and the executors' field
            // type refuses the same pattern.
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"regexp\":{\"body\":{\"value\":\"a&b\"}}}}")
            );
            assertEquals(400, failure.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(failure.getResponse()).contains("Rust regex"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** Ids of the form {@code <fragment>-<offset>} ascend by fragment, then by offset. */
    private static void assertRowAddressAscending(List<String> ids) {
        long previous = -1L;
        for (String id : ids) {
            int dash = id.indexOf('-');
            long rowAddr = (Long.parseLong(id.substring(0, dash)) << 32) | Long.parseLong(id.substring(dash + 1));
            assertTrue("ids not in row address order: " + ids, rowAddr > previous);
            previous = rowAddr;
        }
    }

    /**
     * The Substrait aggregation pushdown on three executors, each
     * holding one fragment of the interleaved fixture (ids
     * {@code i % 3 == f} on fragment {@code f}). With
     * {@code lance.aggregation.pushdown} on and off the responses have
     * to be identical: for {@code terms} that covers the merge of three
     * per node partials, {@code sum_other_doc_count} and
     * {@code doc_count_error_upper_bound} included. With
     * {@code terms(id, size 2)} every node has 100 groups of one row and
     * keeps {@code shard_size} 13 of them, so the reduce derives an
     * error of 1 per node and an other count of 87 per node plus the
     * merged buckets it drops; values the aggregators and the pushdown
     * have to agree on exactly.
     * Buckets and counts are also checked against the single shard
     * path, which does not share the per node error and other count
     * (a single shard has no partials to lose buckets across).
     */
    public void testAggregationPushdownAcrossThreeNodesMatchesAggregators() throws Exception {
        String suffix = "mn-agg-pushdown-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 3;
        int rowsPerFragment = 100;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        List<String> shapes = List.of(
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}",
            "\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":2}}}",
            "\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":2,\"show_term_doc_count_error\":true}}}",
            "\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":5,\"order\":{\"_key\":\"desc\"}}}}",
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}},\"m\":{\"max\":{\"field\":\"ts\"}}}}}",
            "\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":150}}},\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},\"a\":{\"avg\":{\"field\":\"id\"}},\"c\":{\"value_count\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"id\",\"interval\":50}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}",
            // nested buckets and composite: the per node partials of
            // the inner terms lose groups the single shard keeps, the
            // date_histogram under terms and the composite pages do not
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":3},"
                + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}}",
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"},"
                + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}}",
            "\"size\":0,\"aggs\":{\"cd\":{\"composite\":{\"size\":5,\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"}}}]},"
                + "\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"aggs\":{\"cd\":{\"composite\":{\"size\":4,\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"}}}],"
                + "\"after\":{\"c\":\"c0\",\"d\":1706745600000}}}}"
        );
        // Shapes whose three per node partials lose groups the single
        // shard keeps: compared with the aggregators only.
        Set<Integer> partialsLoseGroups = Set.of(1, 2, 9);
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));
            assertEquals("fixture assumes one fragment per data node", fragments, dataNodeCount());

            List<Map<String, Object>> pushed = new ArrayList<>();
            for (int i = 0; i < shapes.size(); i++) {
                String shape = shapes.get(i);
                Map<String, Object> response = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
                pushed.add(response);
                assertFragmentPathMatchesShardPath(indexName, shape);
                // Whole aggregations block against the single shard,
                // except for the shapes whose three partials lose
                // groups the single shard keeps.
                if (!partialsLoseGroups.contains(i)) {
                    Map<String, Object> shardPath = parse(
                        readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}"))
                    );
                    assertEquals(shape, shardPath.get("aggregations"), response.get("aggregations"));
                }
            }
            // Analytic check of the size 2 terms, independent of the
            // aggregators: 300 one row groups, 13 kept per node with an
            // error of 1 each, and every row outside the two returned
            // buckets in the other count.
            Map<String, Object> sizeTwo = pushed.get(1);
            assertEquals(300, extractIntPath(sizeTwo, "hits", "total", "value"));
            assertEquals(2, buckets(sizeTwo).size());
            assertEquals(298, extractIntPath(sizeTwo, "aggregations", "by_id", "sum_other_doc_count"));
            assertEquals(3, extractIntPath(sizeTwo, "aggregations", "by_id", "doc_count_error_upper_bound"));

            updateClusterSetting("lance.aggregation.pushdown", "false");
            try {
                for (int i = 0; i < shapes.size(); i++) {
                    Map<String, Object> viaAggregators = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(i) + "}")));
                    assertEquals(shapes.get(i), viaAggregators.get("aggregations"), pushed.get(i).get("aggregations"));
                    assertEquals(shapes.get(i), viaAggregators.get("hits"), pushed.get(i).get("hits"));
                }
            } finally {
                updateClusterSetting("lance.aggregation.pushdown", null);
            }
            // Every data node took part: the pushdown ran on three
            // executors, not on one node holding every fragment.
            assertBusy(() -> {
                Map<String, String> assignments = fanOutAssignments(indexName);
                assertEquals("fragments went to " + assignments, fragments, assignments.size());
            });
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * The wider pushdown shapes on three executors, one fragment of the
     * interleaved fixture each (ids {@code i % 3 == f} on fragment
     * {@code f}, ts {@code 2024-01-01 + id days}, categories c0 to c2).
     * The exact shapes (stats, extended_stats, range, date_range,
     * missing, filter, filters, nested) have to answer the same as the
     * aggregators and as the single shard path; cardinality and tdigest
     * percentiles, which the pushdown builds from the distinct values
     * and a bin histogram, have to land within their tolerance of the
     * shard path's single sketch: a relative 1 % for the count, and for
     * the percentiles 3 % of the value range, the tolerance the single
     * node IT allows two tdigests of the same data. Every request has to
     * leave a pushdown line on all three data nodes.
     */
    @SuppressWarnings("unchecked")
    public void testWiderAggregationPushdownAcrossThreeNodes() throws Exception {
        String suffix = "mn-agg-wider-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 3;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, 100);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        List<String> exact = List.of(
            "\"size\":0,\"aggs\":{\"s\":{\"stats\":{\"field\":\"id\"}},\"e\":{\"extended_stats\":{\"field\":\"id\",\"sigma\":2}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"1d\"},\"aggs\":{\"s\":{\"stats\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"id\",\"keyed\":true,\"ranges\":[{\"to\":100},{\"from\":50,\"to\":250},{\"from\":250},{\"from\":900}]},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_range\":{\"field\":\"ts\",\"format\":\"yyyy-MM-dd\",\"ranges\":[{\"to\":\"2024-02-01\"},{\"from\":\"2024-02-01\",\"to\":\"2024-06-01\"},{\"from\":\"2024-06-01\"}]},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"aggs\":{\"m\":{\"missing\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":100}}}],\"must_not\":[{\"term\":{\"category\":\"c1\"}}]}},\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"}}}}}",
            "\"size\":0,\"aggs\":{\"fs\":{\"filters\":{\"other_bucket_key\":\"rest\",\"filters\":{\"low\":{\"range\":{\"id\":{\"lt\":120}}},\"c0\":{\"term\":{\"category\":\"c0\"}},\"jan\":{\"range\":{\"ts\":{\"lt\":\"2024-02-01\"}}}}},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":30}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"range\":{\"field\":\"id\",\"ranges\":[{\"to\":150},{\"from\":150}]},\"aggs\":{\"e\":{\"extended_stats\":{\"field\":\"id\"}}}}}}}"
        );
        List<String> sketches = List.of(
            "\"size\":0,\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"id\"}}}",
            "\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":200}}},\"aggs\":{\"k\":{\"cardinality\":{\"field\":\"category\"}},\"s\":{\"stats\":{\"field\":\"id\"}}}",
            "\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"id\"}},\"pr\":{\"percentile_ranks\":{\"field\":\"id\",\"values\":[75,225]}}}",
            "\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"id\"}},\"p\":{\"percentiles\":{\"field\":\"id\",\"percents\":[50,90]}}}}}"
        );
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));
            assertEquals("fixture assumes one fragment per data node", fragments, dataNodeCount());
            updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", "DEBUG");
            try {
                List<Map<String, Object>> pushed = new ArrayList<>();
                for (String shape : exact) {
                    pushed.add(assertAggregationsMatchShardPath(indexName, shape));
                }
                List<Map<String, Object>> pushedSketches = new ArrayList<>();
                for (String shape : sketches) {
                    Map<String, Object> fragmentPath = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
                    Map<String, Object> shardPath = parse(
                        readAll(postJson("/" + indexName + "/_search?request_cache=false", "{\"explain\":true," + shape + "}"))
                    );
                    assertSketchesClose(shape, shardPath.get("aggregations"), fragmentPath.get("aggregations"), 300d);
                    pushedSketches.add(fragmentPath);
                }
                // Exactly 300 distinct ids and 3 categories, both in the
                // linear counting range of the default precision.
                assertEquals(300, extractIntPath(pushedSketches.get(0), "aggregations", "u", "value"));
                assertEquals(3, extractIntPath(pushedSketches.get(1), "aggregations", "k", "value"));

                updateClusterSetting("lance.aggregation.pushdown", "false");
                try {
                    for (int i = 0; i < exact.size(); i++) {
                        Map<String, Object> viaAggregators = parse(
                            readAll(postJson("/" + indexName + "/_search", "{" + exact.get(i) + "}"))
                        );
                        assertEquals(exact.get(i), viaAggregators.get("aggregations"), pushed.get(i).get("aggregations"));
                        assertEquals(exact.get(i), viaAggregators.get("hits"), pushed.get(i).get("hits"));
                    }
                } finally {
                    updateClusterSetting("lance.aggregation.pushdown", null);
                }
                // The pushdown answered on every data node for each of the
                // requests above (the shard path and the aggregator run
                // leave no such line).
                assertBusy(() -> {
                    Set<String> nodes = new HashSet<>();
                    int lines = 0;
                    for (String line : clusterLogLines()) {
                        if (line.contains("lance.dispatch: aggregation pushdown for [" + indexName + "]")) {
                            nodes.add(loggingNodeName(line));
                            lines++;
                        }
                    }
                    assertEquals("pushdown answers logged on " + nodes, dataNodeCount(), nodes.size());
                    assertEquals("one pushdown line per data node per request", (exact.size() + sketches.size()) * fragments, lines);
                });
            } finally {
                updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", null);
            }
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Two parsed {@code aggregations} blocks agree up to the sketches'
     * tolerance: the {@code cardinality} values (named {@code u} and
     * {@code k} in the shapes above) within a relative 1 %, a percentile
     * ({@code values} of a {@code percentiles}) within 3 % of
     * {@code range}, a percentile rank (the {@code pr} aggregation)
     * within 3 points; everything else, bucket keys and doc counts
     * included, equal. Recurses into buckets.
     */
    @SuppressWarnings("unchecked")
    private static void assertSketchesClose(String label, Object expected, Object actual, double range) {
        Map<String, Object> expectedAggregations = (Map<String, Object>) expected;
        Map<String, Object> actualAggregations = (Map<String, Object>) actual;
        assertEquals(label, expectedAggregations.keySet(), actualAggregations.keySet());
        for (String name : expectedAggregations.keySet()) {
            Map<String, Object> e = (Map<String, Object>) expectedAggregations.get(name);
            Map<String, Object> a = (Map<String, Object>) actualAggregations.get(name);
            assertEquals(label + " > " + name, e.keySet(), a.keySet());
            if (e.containsKey("values")) {
                Map<String, Object> ev = (Map<String, Object>) e.get("values");
                Map<String, Object> av = (Map<String, Object>) a.get("values");
                assertEquals(label + " > " + name, ev.keySet(), av.keySet());
                boolean ranks = name.equals("pr");
                for (String key : ev.keySet()) {
                    double diff = Math.abs(((Number) ev.get(key)).doubleValue() - ((Number) av.get(key)).doubleValue());
                    double bound = ranks ? 3d : 0.03d * range;
                    assertTrue(label + " > " + name + " " + key + ": " + ev.get(key) + " vs " + av.get(key), diff <= bound);
                }
            } else if (e.containsKey("buckets")) {
                Object eb = e.get("buckets");
                Object ab = a.get("buckets");
                List<Map<String, Object>> expectedBuckets = eb instanceof List
                    ? (List<Map<String, Object>>) eb
                    : new ArrayList<>(((Map<String, Map<String, Object>>) eb).values());
                List<Map<String, Object>> actualBuckets = ab instanceof List
                    ? (List<Map<String, Object>>) ab
                    : new ArrayList<>(((Map<String, Map<String, Object>>) ab).values());
                assertEquals(label + " > " + name, expectedBuckets.size(), actualBuckets.size());
                for (int i = 0; i < expectedBuckets.size(); i++) {
                    Map<String, Object> expectedBucket = new HashMap<>(expectedBuckets.get(i));
                    Map<String, Object> actualBucket = new HashMap<>(actualBuckets.get(i));
                    Map<String, Object> expectedSubs = new HashMap<>();
                    Map<String, Object> actualSubs = new HashMap<>();
                    for (String key : new ArrayList<>(expectedBucket.keySet())) {
                        if (expectedBucket.get(key) instanceof Map) {
                            expectedSubs.put(key, expectedBucket.remove(key));
                            actualSubs.put(key, actualBucket.remove(key));
                        }
                    }
                    assertEquals(label + " > " + name + " bucket " + i, expectedBucket, actualBucket);
                    assertSketchesClose(label + " > " + name + " bucket " + i, expectedSubs, actualSubs, range);
                }
            } else if (e.containsKey("value") && (name.equals("u") || name.equals("k"))) {
                double ev = ((Number) e.get("value")).doubleValue();
                double av = ((Number) a.get("value")).doubleValue();
                assertTrue(label + " > " + name + ": " + ev + " vs " + av, Math.abs(ev - av) <= 0.01d * Math.max(1d, ev));
            } else {
                assertEquals(label + " > " + name, e, a);
            }
        }
    }

    /**
     * The pushdown with parallel group scans on three executors: twelve
     * fragments of 25 rows, so every node holds several fragments and
     * cuts them into up to four scans per request. The responses have
     * to equal the shard path and the aggregators, {@code terms} error
     * and other counts included, and the cluster log has to show
     * answers assembled from more than one scan.
     */
    public void testAggregationPushdownWithParallelScansAcrossThreeNodes() throws Exception {
        String suffix = "mn-agg-parallel-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 12;
        int rowsPerFragment = 25;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        List<String> shapes = List.of(
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}",
            "\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":2,\"show_term_doc_count_error\":true}}}",
            "\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":5,\"order\":{\"_key\":\"desc\"}}}}",
            "\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}},\"m\":{\"max\":{\"field\":\"ts\"}}}}}",
            "\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":150}}},\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},\"a\":{\"avg\":{\"field\":\"id\"}},\"c\":{\"value_count\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"id\",\"interval\":50}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}"
        );
        try {
            updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", "DEBUG");
            updateClusterSetting("lance.aggregation.pushdown_parallelism", "4");
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            List<Map<String, Object>> pushed = new ArrayList<>();
            for (int i = 0; i < shapes.size(); i++) {
                String shape = shapes.get(i);
                Map<String, Object> response = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
                pushed.add(response);
                assertFragmentPathMatchesShardPath(indexName, shape);
                // The size 2 terms loses groups across three partials
                // that a single shard keeps; every other block is equal.
                if (i != 1) {
                    Map<String, Object> shardPath = parse(
                        readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}"))
                    );
                    assertEquals(shape, shardPath.get("aggregations"), response.get("aggregations"));
                }
            }
            // 300 one row groups: each node keeps shard_size 13 of its
            // 100 with an error of 1, whatever the number of scans it
            // merged them from.
            Map<String, Object> sizeTwo = pushed.get(1);
            assertEquals(300, extractIntPath(sizeTwo, "hits", "total", "value"));
            assertEquals(2, buckets(sizeTwo).size());
            assertEquals(298, extractIntPath(sizeTwo, "aggregations", "by_id", "sum_other_doc_count"));
            assertEquals(3, extractIntPath(sizeTwo, "aggregations", "by_id", "doc_count_error_upper_bound"));

            // The aggregators collect in one slice for the comparison:
            // the pushdown cuts the terms once per executor, as one slice
            // does, while several slices each cut their own share and
            // report the doc count error of the merge.
            updateClusterSetting("lance.aggregation.pushdown", "false");
            updateClusterSetting("lance.fragment_path.slices", "1");
            try {
                for (int i = 0; i < shapes.size(); i++) {
                    Map<String, Object> viaAggregators = parse(readAll(postJson("/" + indexName + "/_search", "{" + shapes.get(i) + "}")));
                    assertEquals(shapes.get(i), viaAggregators.get("aggregations"), pushed.get(i).get("aggregations"));
                    assertEquals(shapes.get(i), viaAggregators.get("hits"), pushed.get(i).get("hits"));
                }
            } finally {
                updateClusterSetting("lance.aggregation.pushdown", null);
                updateClusterSetting("lance.fragment_path.slices", null);
            }
            // Every executor announced its answers, and with twelve
            // fragments over three nodes at least one of them merged
            // more than one scan.
            assertBusy(() -> {
                Set<String> nodes = new HashSet<>();
                boolean severalScans = false;
                for (String line : clusterLogLines()) {
                    if (!line.contains("lance.dispatch: aggregation pushdown for [" + indexName + "]")) {
                        continue;
                    }
                    nodes.add(loggingNodeName(line));
                    if (!line.contains(" in 1 scans ")) {
                        severalScans = true;
                    }
                }
                assertEquals("pushdown answers logged on " + nodes, dataNodeCount(), nodes.size());
                assertTrue("no executor merged more than one scan", severalScans);
            });
        } finally {
            try {
                updateClusterSetting("lance.aggregation.pushdown_parallelism", null);
            } catch (Exception ignored) {}
            try {
                updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", null);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * The wider allow list on three executors: every fragment path answer
     * equals the shard path's for the exact aggregations (stats,
     * extended_stats, range, date_range, filters, missing, hdr
     * percentiles, composite with paging over a date_histogram or terms
     * sources), and the sketches (tdigest percentiles, cardinality) stay
     * within their error of the single shard's value. Twelve fragments of
     * 25 rows, four per node: composite pages are merged from three
     * executors, each having applied the {@code after} key on its own.
     * The executors' DEBUG lines show every node answered with its four
     * fragments.
     */
    @SuppressWarnings("unchecked")
    public void testWiderAllowListAcrossThreeNodes() throws Exception {
        String suffix = "mn-allow-list-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 12;
        int rowsPerFragment = 25;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String[] exact = {
            "\"size\":0,\"aggs\":{\"s\":{\"stats\":{\"field\":\"id\"}},\"es\":{\"extended_stats\":{\"field\":\"id\"}}}",
            "\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"id\",\"ranges\":[{\"to\":100},{\"from\":100,\"to\":200},{\"from\":200}]},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_range\":{\"field\":\"ts\",\"format\":\"yyyy-MM-dd\",\"ranges\":[{\"to\":\"2024-04-01\"},{\"from\":\"2024-04-01\"}]}}}",
            "\"size\":0,\"aggs\":{\"fs\":{\"filters\":{\"other_bucket\":true,\"filters\":{\"even\":{\"range\":{\"id\":{\"lt\":150}}},\"c1\":{\"term\":{\"category\":\"c1\"}}}}}}",
            "\"size\":0,\"aggs\":{\"m\":{\"missing\":{\"field\":\"category\"}}}",
            "\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"id\",\"percents\":[10,50,90],\"hdr\":{\"number_of_significant_value_digits\":3}}}}",
            "\"size\":0,\"query\":{\"term\":{\"category\":\"c2\"}},\"aggs\":{\"c\":{\"composite\":{\"size\":4,\"sources\":[{\"month\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}]},\"aggs\":{\"mx\":{\"max\":{\"field\":\"id\"}}}}}" };
        String firstPage =
            "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":7,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"i\":{\"terms\":{\"field\":\"id\"}}}]}}}";
        try {
            updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", "DEBUG");
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            // Fragment path requests issued below; the shard path
            // requests (explain) leave no executor line.
            int fragmentPathRequests = 0;
            for (String shape : exact) {
                assertAggregationsMatchShardPath(indexName, shape);
                fragmentPathRequests++;
            }

            // Composite paging: every page and its after_key equal the
            // shard path's, and the pages cover the 300 (category, id)
            // pairs without a repeat.
            Map<String, Object> page = assertAggregationsMatchShardPath(indexName, firstPage);
            fragmentPathRequests++;
            Map<String, Object> afterKey = (Map<String, Object>) aggregationOf(page, "c").get("after_key");
            assertEquals("c0", afterKey.get("cat"));
            assertEquals(18, ((Number) afterKey.get("i")).intValue());
            List<String> keys = new ArrayList<>();
            for (Map<String, Object> bucket : buckets(page)) {
                keys.add(String.valueOf(bucket.get("key")));
            }
            for (int pages = 0; pages < 3; pages++) {
                String after = "{\"cat\":\"" + afterKey.get("cat") + "\",\"i\":" + afterKey.get("i") + "}";
                page = assertAggregationsMatchShardPath(
                    indexName,
                    "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":7,\"after\":"
                        + after
                        + ",\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"i\":{\"terms\":{\"field\":\"id\"}}}]}}}"
                );
                fragmentPathRequests++;
                for (Map<String, Object> bucket : buckets(page)) {
                    keys.add(String.valueOf(bucket.get("key")));
                }
                afterKey = (Map<String, Object>) aggregationOf(page, "c").get("after_key");
            }
            assertEquals(28, keys.size());
            assertEquals(28, keys.stream().distinct().count());
            assertEquals("{cat=c0, i=81}", keys.get(27));

            // tdigest percentiles: three sketches merged against one. A
            // tdigest at the default compression places a quantile within
            // a couple of percentile points, so the values are compared
            // as a share of the id range (0 to 299), not relatively.
            String tdigest = "{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"id\"}}}}";
            Map<String, Object> viaFragments = parse(readAll(postJson("/" + indexName + "/_search", tdigest)));
            fragmentPathRequests++;
            Map<String, Object> viaShard = parse(
                readAll(postJson("/" + indexName + "/_search?request_cache=false", "{\"explain\":true," + tdigest.substring(1)))
            );
            Map<String, Object> fragmentValues = (Map<String, Object>) aggregationOf(viaFragments, "p").get("values");
            Map<String, Object> shardValues = (Map<String, Object>) aggregationOf(viaShard, "p").get("values");
            assertEquals(shardValues.keySet(), fragmentValues.keySet());
            for (String percentile : shardValues.keySet()) {
                double expected = ((Number) shardValues.get(percentile)).doubleValue();
                double actual = ((Number) fragmentValues.get(percentile)).doubleValue();
                assertTrue(
                    "percentile " + percentile + ": shard path " + expected + ", fragment path " + actual,
                    Math.abs(expected - actual) <= 0.03d * 299d
                );
            }

            // cardinality: 300 distinct ids and 3 categories, both under
            // the default precision threshold, so the merged sketches are
            // within one percent of the truth.
            Map<String, Object> counted = parse(
                readAll(
                    postJson(
                        "/" + indexName + "/_search",
                        "{\"size\":0,\"aggs\":{\"ids\":{\"cardinality\":{\"field\":\"id\"}},\"cats\":{\"cardinality\":{\"field\":\"category\"}}}}"
                    )
                )
            );
            fragmentPathRequests++;
            double ids = ((Number) aggregationOf(counted, "ids").get("value")).doubleValue();
            assertTrue("cardinality(id) " + ids, Math.abs(ids - 300d) <= 3d);
            assertEquals(3, ((Number) aggregationOf(counted, "cats").get("value")).intValue());

            // Every data node executed its four fragments for every one
            // of these requests: the executor logs one line per request
            // with its fragment count and duration, so each node has as
            // many lines as requests were issued and a request that had
            // fallen to the shard path would leave a gap.
            int expectedRequests = fragmentPathRequests;
            assertEquals(13, expectedRequests);
            assertBusy(() -> {
                Map<String, Integer> perNode = new HashMap<>();
                int total = 0;
                for (String line : clusterLogLines()) {
                    if (!line.contains("lance.dispatch: fragment query for [" + indexName + "] over 4 fragments took ")) {
                        continue;
                    }
                    perNode.merge(loggingNodeName(line), 1, Integer::sum);
                    total++;
                }
                int dataNodes = dataNodeCount();
                assertEquals("executor lines on " + perNode, dataNodes, perNode.size());
                assertEquals("executor lines per node " + perNode, expectedRequests * dataNodes, total);
                for (Map.Entry<String, Integer> node : perNode.entrySet()) {
                    assertEquals("executor lines on " + node.getKey(), expectedRequests, node.getValue().intValue());
                }
            });
        } finally {
            try {
                updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", null);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Sliced collection on three executors answers like the shard path.
     * Twelve fragments of 25 rows, four per node, collected in two slices
     * per executor with the pushdown off so every shape runs through the
     * Lucene aggregators: the exact aggregations equal the shard path's
     * and the sketches stay within their error. Every executor's DEBUG
     * line reports its four leaves in two slices.
     */
    @SuppressWarnings("unchecked")
    public void testSlicedCollectionAcrossThreeNodesAnswersLikeTheShardPath() throws Exception {
        String suffix = "mn-slices-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 12;
        int rowsPerFragment = 25;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String[] exact = {
            "\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"show_term_doc_count_error\":true},\"aggs\":{\"s\":{\"stats\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},\"es\":{\"extended_stats\":{\"field\":\"id\"}}}",
            "\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"id\",\"ranges\":[{\"to\":100},{\"from\":100,\"to\":200},{\"from\":200}]},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}}",
            "\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"query\":{\"term\":{\"category\":\"c2\"}},\"aggs\":{\"c\":{\"composite\":{\"size\":4,\"sources\":[{\"month\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}},{\"cat\":{\"terms\":{\"field\":\"category\"}}}]},\"aggs\":{\"mx\":{\"max\":{\"field\":\"id\"}}}}}",
            "\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":40}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}},\"h\":{\"histogram\":{\"field\":\"id\",\"interval\":50}}}",
            "\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"id\",\"percents\":[10,50,90],\"hdr\":{\"number_of_significant_value_digits\":3}}}}" };
        try {
            updateClusterSetting("logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction", "DEBUG");
            updateClusterSetting("lance.aggregation.pushdown", "false");
            updateClusterSetting("lance.fragment_path.slices", "2");
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            int fragmentPathRequests = 0;
            for (String shape : exact) {
                assertAggregationsMatchShardPath(indexName, shape);
                fragmentPathRequests++;
            }

            String tdigest = "{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"id\"}}}}";
            Map<String, Object> viaFragments = parse(readAll(postJson("/" + indexName + "/_search", tdigest)));
            fragmentPathRequests++;
            Map<String, Object> viaShard = parse(
                readAll(postJson("/" + indexName + "/_search?request_cache=false", "{\"explain\":true," + tdigest.substring(1)))
            );
            Map<String, Object> fragmentValues = (Map<String, Object>) aggregationOf(viaFragments, "p").get("values");
            Map<String, Object> shardValues = (Map<String, Object>) aggregationOf(viaShard, "p").get("values");
            assertEquals(shardValues.keySet(), fragmentValues.keySet());
            for (String percentile : shardValues.keySet()) {
                double expected = ((Number) shardValues.get(percentile)).doubleValue();
                double actual = ((Number) fragmentValues.get(percentile)).doubleValue();
                assertTrue(
                    "percentile " + percentile + ": shard path " + expected + ", fragment path " + actual,
                    Math.abs(expected - actual) <= 0.03d * 299d
                );
            }
            Map<String, Object> counted = parse(
                readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"ids\":{\"cardinality\":{\"field\":\"id\"}}}}"))
            );
            fragmentPathRequests++;
            double ids = ((Number) aggregationOf(counted, "ids").get("value")).doubleValue();
            assertTrue("cardinality(id) " + ids, Math.abs(ids - 300d) <= 3d);

            int expectedRequests = fragmentPathRequests;
            String marker = "lance.dispatch: fragment path slices for ["
                + indexName
                + "]: 4 leaves in 2 slices (lance.fragment_path.slices 2)";
            assertBusy(() -> {
                Map<String, Integer> perNode = new HashMap<>();
                for (String line : clusterLogLines()) {
                    if (line.contains(marker)) {
                        perNode.merge(loggingNodeName(line), 1, Integer::sum);
                    }
                }
                int dataNodes = dataNodeCount();
                assertEquals("slice lines on " + perNode, dataNodes, perNode.size());
                for (Map.Entry<String, Integer> node : perNode.entrySet()) {
                    assertEquals("slice lines on " + node.getKey(), expectedRequests, node.getValue().intValue());
                }
            });
        } finally {
            for (String key : new String[] {
                "logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction",
                "lance.aggregation.pushdown",
                "lance.fragment_path.slices" }) {
                try {
                    updateClusterSetting(key, null);
                } catch (Exception ignored) {}
            }
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Run {@code shape} through the fragment path and, with
     * {@code "explain": true}, through the shard path, and assert the two
     * responses carry the same {@code hits.total} and the same
     * {@code aggregations} block. Returns the fragment path response.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> assertAggregationsMatchShardPath(String indexName, String shape) throws IOException {
        Map<String, Object> fragmentPath = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
        Map<String, Object> shardPath = parse(
            readAll(postJson("/" + indexName + "/_search?request_cache=false", "{\"explain\":true," + shape + "}"))
        );
        assertEquals(
            shape,
            ((Map<String, Object>) shardPath.get("hits")).get("total"),
            ((Map<String, Object>) fragmentPath.get("hits")).get("total")
        );
        assertEquals(shape, shardPath.get("aggregations"), fragmentPath.get("aggregations"));
        return fragmentPath;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aggregationOf(Map<String, Object> response, String name) {
        return (Map<String, Object>) ((Map<String, Object>) response.get("aggregations")).get(name);
    }

    /** Name of the elected cluster manager, from {@code GET /_cat/cluster_manager}. */
    private static String clusterManagerNodeName() throws IOException {
        String name = readAll(client().performRequest(new Request("GET", "/_cat/cluster_manager?h=node"))).trim();
        assertFalse("_cat/cluster_manager returned no node name", name.isEmpty());
        return name;
    }

    /**
     * A heap column load the request breaker refuses on one executor
     * reaches the client as HTTP 429. Four fragments of 400 rows over
     * three data nodes put fragments 0 and 3 on the first node and one
     * fragment on each of the other two; with {@code lance.cache.enabled}
     * off every executor materialises {@code rating} in heap, about
     * 3.3 KB per fragment, on top of the 5 KB every aggregator reserves
     * on the same breaker when it is built. A request breaker limit of
     * 10 KB therefore refuses the two fragment node (about 11.7 KB) and
     * lets the others (about 8.4 KB) through. The collection runs in one
     * slice so that the two fragment node builds one aggregator tree, not
     * one per slice. The coordinator forwards
     * the executor's {@code CircuitBreakingException} with its status,
     * and {@code _lance/stats} shows the refusal on exactly one node.
     */
    public void testHeapColumnRefusedOnOneExecutorIs429AtTheCoordinator() throws Exception {
        String suffix = "mn-heap-breaker-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 4;
        int rowsPerFragment = 400;
        LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String sum = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}";
        updateClusterSetting("lance.aggregation.pushdown", "false");
        updateClusterSetting("lance.cache.enabled", "false");
        updateClusterSetting("lance.fragment_path.slices", "1");
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));
            assertEquals("fixture assumes three data nodes", 3, dataNodeCount());

            Map<String, Long> rejectionsBefore = heapFallbackRejectionsByNode();
            String first = readAll(postJson("/" + indexName + "/_search", sum));
            assertEquals(1600, extractIntPath(first, "hits", "total", "value"));
            double expectedSum = 0d;
            for (int i = 0; i < fragments * rowsPerFragment; i++) {
                if (i % 5 != 4) {
                    expectedSum += (i * 37) % 1000;
                }
            }
            assertEquals(expectedSum, extractDoublePath(first, "aggregations", "s", "value"), 0d);
            assertEquals(rejectionsBefore, heapFallbackRejectionsByNode());

            updateClusterSetting("indices.breaker.request.limit", "10kb");
            try {
                ResponseException refused = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", sum));
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.TOO_MANY_REQUESTS.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertTrue(body, body.contains("circuit_breaking_exception"));
                assertTrue(body, body.contains("lance_heap_column:rating"));
                assertTrue(body, body.contains("limit of [10240/10kb]"));
                Map<String, Long> rejectionsAfter = heapFallbackRejectionsByNode();
                int nodesThatRefused = 0;
                for (Map.Entry<String, Long> entry : rejectionsAfter.entrySet()) {
                    long delta = entry.getValue() - rejectionsBefore.get(entry.getKey());
                    assertTrue("node " + entry.getKey() + " refused " + delta + " loads", delta == 0L || delta == 1L);
                    nodesThatRefused += (int) delta;
                }
                assertEquals("the two fragment executor alone refused: " + rejectionsAfter, 1, nodesThatRefused);
            } finally {
                updateClusterSetting("indices.breaker.request.limit", null);
            }

            String again = readAll(postJson("/" + indexName + "/_search", sum));
            assertEquals(expectedSum, extractDoublePath(again, "aggregations", "s", "value"), 0d);
            assertBusy(() -> {
                Map<String, String> assignments = fanOutAssignments(indexName);
                assertEquals("fragments went to " + assignments, 3, assignments.size());
            });
        } finally {
            updateClusterSetting("lance.cache.enabled", null);
            updateClusterSetting("lance.aggregation.pushdown", null);
            updateClusterSetting("lance.fragment_path.slices", null);
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** {@code column_store.heap_fallback_rejections} of every node, keyed by node id. */
    @SuppressWarnings("unchecked")
    private static Map<String, Long> heapFallbackRejectionsByNode() throws IOException {
        Map<String, Object> stats = parse(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        Map<String, Long> rejections = new HashMap<>();
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> columnStore = (Map<String, Object>) ((Map<String, Object>) node.getValue()).get("column_store");
            rejections.put(node.getKey(), ((Number) columnStore.get("heap_fallback_rejections")).longValue());
        }
        return rejections;
    }

    /** Name of the node listening on {@code host}, read through a client pinned to that host alone. */
    @SuppressWarnings("unchecked")
    private String localNodeName(HttpHost host) throws IOException {
        try (var pinned = buildClient(restClientSettings(), new HttpHost[] { host })) {
            Map<String, Object> response = parse(
                readAll(pinned.performRequest(new Request("GET", "/_nodes/_local?filter_path=nodes.*.name")))
            );
            Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
            assertEquals("_nodes/_local names exactly one node: " + response, 1, nodes.size());
            Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
            return (String) node.get("name");
        }
    }

    /**
     * Run {@code shape} (the body of a {@code _search} request without
     * its outer braces) through the fragment path and, with
     * {@code "explain": true} added, through the shard path, and
     * compare the parts of the two responses that describe the result.
     */
    private static void assertFragmentPathMatchesShardPath(String indexName, String shape) throws IOException {
        assertFragmentPathMatchesShardPath(indexName, shape, true);
    }

    /**
     * As {@link #assertFragmentPathMatchesShardPath(String, String)};
     * {@code compareScores} false skips the per-hit {@code _score}
     * comparison, for a sort that has a {@code _score} clause next to a
     * field clause: the shard path copies that clause's sort value into
     * {@code _score}, the fragment path leaves {@code _score} null
     * unless {@code track_scores} is set.
     */
    private static void assertFragmentPathMatchesShardPath(String indexName, String shape, boolean compareScores) throws IOException {
        Map<String, Object> fragmentPath = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
        Map<String, Object> shardPath = parse(readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}")));
        assertEquals(shape, hitIdsOf(shardPath), hitIdsOf(fragmentPath));
        assertEquals(shape, sortValues(shardPath), sortValues(fragmentPath));
        if (compareScores) {
            List<Double> expectedScores = scoresOrNull(shardPath);
            List<Double> actualScores = scoresOrNull(fragmentPath);
            assertEquals(shape, expectedScores.size(), actualScores.size());
            for (int i = 0; i < expectedScores.size(); i++) {
                Double expected = expectedScores.get(i);
                Double actual = actualScores.get(i);
                if (expected == null || actual == null) {
                    assertEquals(shape + " hit " + i, expected, actual);
                } else {
                    assertEquals(shape + " hit " + i, expected, actual, 1e-6d);
                }
            }
        }
        assertEquals(shape, extractIntPath(shardPath, "hits", "total", "value"), extractIntPath(fragmentPath, "hits", "total", "value"));
        assertEquals(shape, relation(shardPath), relation(fragmentPath));
        assertEquals(shape, bucketSummary(shardPath), bucketSummary(fragmentPath));
    }

    @SuppressWarnings("unchecked")
    private static String relation(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return (String) ((Map<String, Object>) hits.get("total")).get("relation");
    }

    private static List<String> hitIdsOf(Map<String, Object> response) {
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            ids.add((String) hit.get("_id"));
        }
        return ids;
    }

    /** {@code _score} of every hit; null entries where the hit carries no score (sorted requests). */
    private static List<Double> scoresOrNull(Map<String, Object> response) {
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            Object score = hit.get("_score");
            scores.add(score == null ? null : ((Number) score).doubleValue());
        }
        return scores;
    }

    /** {@code key=doc_count} of every terms bucket, or an empty list without aggregations. */
    private static List<String> bucketSummary(Map<String, Object> response) {
        if (response.get("aggregations") == null) {
            return List.of();
        }
        List<String> summary = new ArrayList<>();
        for (Map<String, Object> bucket : buckets(response)) {
            summary.add(bucket.get("key") + "=" + ((Number) bucket.get("doc_count")).longValue());
        }
        return summary;
    }

    /**
     * The fragment lists the coordinator logged per node for
     * {@code indexName}, keyed by node id, from the cluster logs. The
     * same assignment is logged on every request, so the map holds one
     * entry per node.
     */
    private static Map<String, String> fanOutAssignments(String indexName) throws IOException {
        Map<String, String> assignments = new HashMap<>();
        String nodeMarker = " to node [";
        String fragmentsMarker = " with fragments [";
        for (String line : clusterLogLines()) {
            if (!line.contains("lance.dispatch: fan-out index [" + indexName + "]")) {
                continue;
            }
            int at = line.indexOf(nodeMarker);
            int fragmentsAt = line.indexOf(fragmentsMarker);
            if (at < 0 || fragmentsAt < 0) {
                continue;
            }
            String node = line.substring(at + nodeMarker.length(), line.indexOf(']', at + nodeMarker.length()));
            int fragmentsStart = fragmentsAt + fragmentsMarker.length();
            String fragments = line.substring(fragmentsStart, line.indexOf(']', fragmentsStart));
            assignments.put(node, fragments);
        }
        return assignments;
    }

    public void testNamespaceRegisterPropagatesToAllNodes() throws Exception {
        // GET reads cluster state on the responding node, so the entry
        // only appears if the registration propagated.
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-mn-register-" + randomAlphaOfLength(8)));
        String path = scratchDir.toString();
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String body = readAll(listing);
            assertTrue("expected namespace " + path + " in listing: " + body, body.contains(path));

            Response unregister = deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            assertEquals(RestStatus.OK.getStatus(), unregister.getStatusLine().getStatusCode());
            String unregisterBody = readAll(unregister);
            assertTrue("expected unregistered:true, saw: " + unregisterBody, unregisterBody.contains("\"unregistered\":true"));

            Response after = client().performRequest(new Request("GET", "/_lance/namespace"));
            String afterBody = readAll(after);
            assertFalse("expected namespace " + path + " to be gone: " + afterBody, afterBody.contains(path));
        } finally {
            // Best-effort cleanup in case an earlier assertion left
            // the namespace registered.
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            } catch (Exception ignored) {}
        }
    }

    /**
     * With three data nodes every fragment response of a request crosses
     * the transport layer. The cluster runs the {@code lance_coordinator}
     * pool with one thread and a queue of one (build.gradle), so a burst
     * of concurrent requests makes each coordinating node refuse some of
     * them. The refused ones must be 429s, the served ones complete, no
     * task may remain, and no transport channel may have been closed:
     * a response the transport layer cannot hand over closes the
     * channel it came on and fails every other request on it.
     */
    public void testCoordinatorPoolOverloadOnThreeNodesClosesNoChannel() throws Exception {
        String suffix = "mn-overload-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 120, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(6, extractIntPath(readAll(attach), "fragments"));

            String body = "{\"size\":5,\"sort\":[{\"id\":\"desc\"}],\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
            int served = 0;
            int rejected = 0;
            try (RestClient wide = LanceRestTestCase.concurrentClient(getClusterHosts(), 32)) {
                for (int round = 0; round < 20 && rejected == 0; round++) {
                    for (LanceRestTestCase.ConcurrentResult result : LanceRestTestCase.postConcurrently(
                        wide,
                        "/" + indexName + "/_search",
                        body,
                        32
                    )) {
                        if (result.status() == RestStatus.OK.getStatus()) {
                            served++;
                            assertEquals(
                                "served request must be complete: " + result.body(),
                                120,
                                extractIntPath(result.body(), "hits", "total", "value")
                            );
                            assertEquals(7140.0d, extractDoublePath(result.body(), "aggregations", "s", "value"), 0.0d);
                            assertEquals(List.of("5-19", "5-18", "5-17", "5-16", "5-15"), hitIds(result.body()));
                        } else if (result.status() == RestStatus.TOO_MANY_REQUESTS.getStatus()) {
                            rejected++;
                            assertTrue(
                                "429 body must carry the pool's rejection: " + result.body(),
                                result.body().contains("rejected execution")
                            );
                            assertTrue("429 body must name the pool: " + result.body(), result.body().contains("lance_coordinator"));
                        } else {
                            fail("unexpected status " + result.status() + " from the fragment path: " + result.body());
                        }
                    }
                }
            }
            assertTrue("expected the one-thread, one-slot pools to refuse a request in 20 bursts of 32", rejected > 0);
            assertTrue("expected the pools to serve requests as well", served > 0);

            assertBusy(() -> {
                String tasks = readAll(
                    client().performRequest(new Request("GET", "/_tasks?actions=*lance/coordinator*,indices:data/read/search*"))
                );
                assertEquals("tasks left behind: " + tasks, 0, LanceRestTestCase.countOccurrences(tasks, "\"action\""));
            });

            String pools = readAll(
                client().performRequest(new Request("GET", "/_cat/thread_pool/lance_coordinator?format=json&h=node_name,rejected"))
            );
            assertTrue("thread pool stats must count the rejections: " + pools, LanceRestTestCase.sumCatColumn(pools, "rejected") > 0);

            for (String line : clusterLogLines()) {
                assertFalse(
                    "a fragment response was refused by the transport layer: " + line,
                    line.contains("exception caught on transport layer")
                );
            }
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** Number of data nodes in the cluster, from {@code GET /_nodes/data:true}. */
    private static int dataNodeCount() throws IOException {
        return extractIntPath(readAll(client().performRequest(new Request("GET", "/_nodes/data:true"))), "_nodes", "total");
    }

    public void testCancelledExecutorOnOneNodeAnswersPartialResultsWithTimedOut() throws Exception {
        // 1,200 rows written 400 per file give fragments 0, 1 and 2, one
        // per data node. A script query that spins per document keeps
        // every executor busy for a second or more; cancelling the
        // executor task of one node while the request runs makes that
        // node answer TaskCancelledException, which the coordinator
        // treats like a node that timed out: the request completes from
        // the other two nodes (800 rows), says timed_out, and reports the
        // count as a lower bound.
        String suffix = "mn-cancel-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 1200, 400);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));
            assertEquals(3, dataNodeCount());

            CompletableFuture<LanceRestTestCase.ConcurrentResult> pending = LanceRestTestCase.postAsync(
                client(),
                "/" + indexName + "/_search",
                "{\"size\":5,\"sort\":[{\"id\":\"asc\"}],\"query\":" + LanceRestTestCase.slowScriptQuery(900_000) + "}"
            );
            List<Map<String, Object>> executors = LanceRestTestCase.awaitFragmentQueryRunning(client(), 3);
            String victim = (String) executors.get(0).get("node");
            String cancelled = readAll(postJson("/_tasks/_cancel?nodes=" + victim + "&actions=*lance/fragment_query*", ""));
            assertTrue("the cancel must name one executor task: " + cancelled, cancelled.contains("lance/fragment_query"));

            LanceRestTestCase.ConcurrentResult result = pending.get(60, TimeUnit.SECONDS);
            assertEquals("the request must complete from the other nodes: " + result.body(), RestStatus.OK.getStatus(), result.status());
            Map<String, Object> response = parse(result.body());
            assertEquals("the response must say a node did not answer: " + result.body(), Boolean.TRUE, response.get("timed_out"));
            assertEquals("two of three fragments answered: " + result.body(), 800, extractIntPath(result.body(), "hits", "total", "value"));
            assertEquals("a partial count is a lower bound: " + result.body(), "gte", relation(response));
            assertEquals("the page comes from the answering nodes", 5, hitList(response).size());

            assertBusy(() -> {
                List<Map<String, Object>> left = LanceRestTestCase.tasksOf(
                    client(),
                    "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*"
                );
                assertEquals("tasks left behind: " + left, 0, left.size());
            });

            // Without a cancellation the same request answers in full.
            String complete = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":5,\"query\":" + LanceRestTestCase.slowScriptQuery(1_000) + "}")
            );
            assertEquals(Boolean.FALSE, parse(complete).get("timed_out"));
            assertEquals(1200, extractIntPath(complete, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testTimeoutAcrossThreeNodesCancelsEveryExecutor() throws Exception {
        // With a 100 ms timeout no executor of the slow query answers in
        // time; the coordinator answers from zero nodes with timed_out
        // and cancels the executor task on each of the three nodes.
        String suffix = "mn-timeout-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 1200, 400);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"timeout\":\"100ms\",\"size\":5,\"query\":" + LanceRestTestCase.slowScriptQuery(900_000) + "}"
                )
            );
            Map<String, Object> response = parse(body);
            assertEquals("the response must say it timed out: " + body, Boolean.TRUE, response.get("timed_out"));
            assertEquals(0, extractIntPath(body, "hits", "total", "value"));
            assertEquals("gte", relation(response));

            assertBusy(() -> {
                List<Map<String, Object>> left = LanceRestTestCase.tasksOf(
                    client(),
                    "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*"
                );
                assertEquals("every timed out executor must have been cancelled, tasks left: " + left, 0, left.size());
            });
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    @SuppressWarnings("unchecked")
    public void testTableAboveTheLuceneBoundFansOutInGroupsPerNode() throws Exception {
        // 120 rows in 6 fragments of 20 over 3 data nodes: 2 fragments (40
        // rows) per node. Under a bound of 20 rows per reader each node's
        // share is cut into 2 groups, so the coordinator sends 6 requests
        // and merges 6 responses; the answers must match the one request
        // per node the default bound gives, the shard comes up green with a
        // reader over the first fragment, GET reaches every row, and a
        // shape only the shard path serves is refused.
        String suffix = "mn-bound-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 120, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String pkTable = "pk-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, pkTable, 12, 4);
        String pkTableUri = scratchDir.resolve(pkTable + ".lance").toString();
        String[] requests = new String[] {
            "{\"query\":{\"match_all\":{}},\"size\":10}",
            "{\"size\":5,\"sort\":[{\"id\":\"desc\"}]}",
            "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}",
            "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"id\",\"size\":3,\"order\":{\"_key\":\"desc\"}}}}}",
            "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[57.4,0,0,0,0,0,0,0],\"k\":3}}}",
            "{\"size\":0,\"track_total_hits\":true,\"query\":{\"range\":{\"id\":{\"gte\":30,\"lt\":100}}}}" };
        try {
            assertEquals("fixture assumes two fragments per data node", 3, dataNodeCount());
            updateClusterSetting("lance.test.max_docs_per_reader", "20");
            String attach = readAll(postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}"));
            assertEquals(attach, 6, extractIntPath(attach, "fragments"));
            assertTrue(attach, attach.contains("\"lucene_bound_exceeded\":true"));
            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=green&timeout=60s"));
            String docStats = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_stats/docs")));
            assertEquals(20, extractIntPath(docStats, "indices", tableName, "primaries", "docs", "count"));
            // The node that hosts the shard reports the table's rows next
            // to the reader's.
            Map<String, Object> lanceStats = parse(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
            Map<String, Object> indexStats = null;
            for (Object node : ((Map<String, Object>) lanceStats.get("nodes")).values()) {
                Map<String, Object> indices = (Map<String, Object>) ((Map<String, Object>) node).get("indices");
                if (indices.containsKey(tableName)) {
                    assertNull("one shard copy, one node reports it", indexStats);
                    indexStats = (Map<String, Object>) indices.get(tableName);
                }
            }
            assertNotNull(lanceStats.toString(), indexStats);
            assertEquals(120, ((Number) indexStats.get("rows")).intValue());
            assertEquals(20, ((Number) indexStats.get("shard_reader_rows")).intValue());
            assertEquals(true, indexStats.get("lucene_bound_exceeded"));

            List<Map<String, Object>> grouped = new ArrayList<>();
            for (String request : requests) {
                grouped.add(parse(readAll(postJson("/" + tableName + "/_search", request))));
            }
            assertEquals(120, extractIntPath(grouped.get(0), "hits", "total", "value"));
            assertEquals(10, sourceIds(grouped.get(0)).size());
            assertEquals(List.of(119, 118, 117, 116, 115), sourceIds(grouped.get(1)));
            assertEquals(
                7140.0d,
                ((Number) ((Map<String, Object>) ((Map<String, Object>) grouped.get(2).get("aggregations")).get("s")).get("value"))
                    .doubleValue(),
                0.0d
            );
            assertEquals(3, buckets(grouped.get(3)).size());
            assertEquals(119, ((Number) buckets(grouped.get(3)).get(0).get("key")).intValue());
            assertEquals(List.of(57, 58, 56), sourceIds(grouped.get(4)));
            assertEquals(70, extractIntPath(grouped.get(5), "hits", "total", "value"));
            String count = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_count")));
            assertEquals(120, extractIntPath(count, "count"));

            // Six fan-out lines for the index, two per node, each naming
            // its group of two.
            assertBusy(() -> {
                Map<String, Integer> groupsPerNode = new HashMap<>();
                for (String line : clusterLogLines()) {
                    if (!line.contains("lance.dispatch: fan-out index [" + tableName + "]") || !line.contains("(group ")) {
                        continue;
                    }
                    int at = line.indexOf(" to node [");
                    String node = line.substring(at + " to node [".length(), line.indexOf(']', at + " to node [".length()));
                    assertTrue(line, line.contains(" of 2, 20 rows)"));
                    groupsPerNode.merge(node, 1, Integer::sum);
                }
                assertEquals("groups per node: " + groupsPerNode, 3, groupsPerNode.size());
                for (int groups : groupsPerNode.values()) {
                    // Two groups per request, over the requests above.
                    assertEquals("groups per node: " + groupsPerNode, 0, groups % 2);
                    assertTrue("groups per node: " + groupsPerNode, groups >= 2 * requests.length);
                }
            });

            // GET through the Lance scan filter reaches rows outside the
            // shard reader; the shard path is refused for the table.
            updateClusterSetting("lance.test.max_docs_per_reader", "4");
            String pkAttach = readAll(postJson("/_lance/attach", "{\"table\":\"" + pkTableUri + "\"}"));
            assertTrue(pkAttach, pkAttach.contains("\"lucene_bound_exceeded\":true"));
            client().performRequest(new Request("GET", "/_cluster/health/" + pkTable + "?wait_for_status=green&timeout=60s"));
            for (String key : List.of("alpha-0", "alpha-5", "alpha-11")) {
                Response hit = client().performRequest(new Request("GET", "/" + pkTable + "/_doc/" + key));
                assertEquals(200, hit.getStatusLine().getStatusCode());
                assertTrue(readAll(hit).contains("\"_id\":\"" + key + "\""));
            }
            ResponseException refused = expectThrows(
                ResponseException.class,
                () -> postJson("/" + pkTable + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}")
            );
            assertEquals(400, refused.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(refused.getResponse()).contains("above the Lucene bound"));

            // The default bound gives one request per node and the same
            // answers.
            updateClusterSetting("lance.test.max_docs_per_reader", null);
            for (int i = 0; i < requests.length; i++) {
                Map<String, Object> oneGroup = parse(readAll(postJson("/" + tableName + "/_search", requests[i])));
                assertEquals(requests[i], sourceIds(grouped.get(i)), sourceIds(oneGroup));
                assertEquals(requests[i], grouped.get(i).get("aggregations"), oneGroup.get("aggregations"));
                assertEquals(
                    requests[i],
                    extractIntPath(grouped.get(i), "hits", "total", "value"),
                    extractIntPath(oneGroup, "hits", "total", "value")
                );
            }
        } finally {
            try {
                updateClusterSetting("lance.test.max_docs_per_reader", null);
            } catch (Exception ignored) {}
            for (String index : List.of(tableName, pkTable)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
        }
    }

    private static String loggingNodeName(String line) {
        int open = -1;
        for (int i = 0; i < 4; i++) {
            open = line.indexOf('[', open + 1);
            assertTrue("unexpected log line shape: " + line, open >= 0);
        }
        int close = line.indexOf(']', open);
        assertTrue("unexpected log line shape: " + line, close > open);
        return line.substring(open + 1, close);
    }

    /**
     * Every line of every node log under the test clusters directory
     * the build passes in {@code tests.lance.cluster_logs_dir}. The
     * testclusters plugin keeps one {@code <task>-<n>/logs/<task>.log}
     * per node; its captured stdout ({@code opensearch.stdout.log})
     * repeats every line and is skipped so counts are per log line.
     */
    private static List<String> clusterLogLines() throws IOException {
        String property = System.getProperty("tests.lance.cluster_logs_dir");
        assertNotNull("tests.lance.cluster_logs_dir must be set by the Gradle build", property);
        List<String> lines = new ArrayList<>();
        try (Stream<Path> files = Files.walk(Path.of(property))) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String name = file.getFileName().toString();
                if (!name.endsWith(".log") || name.startsWith("opensearch.") || !file.getParent().getFileName().toString().equals("logs")) {
                    continue;
                }
                lines.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            }
        }
        return lines;
    }

    private static Response postJson(String path, String body) throws IOException {
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static Response deleteJson(String path, String body) throws IOException {
        Request request = new Request("DELETE", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        String encoded = value == null ? "null" : "\"" + value + "\"";
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + encoded + "}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    private static String readAll(Response response) throws IOException {
        try (var stream = response.getEntity().getContent()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> hitIds(String searchBody) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            Map<String, Object> map = parser.map();
            List<Object> hits = (List<Object>) ((Map<String, Object>) map.get("hits")).get("hits");
            List<String> ids = new ArrayList<>(hits.size());
            for (Object hit : hits) {
                ids.add((String) ((Map<String, Object>) hit).get("_id"));
            }
            return ids;
        }
    }

    private static Map<String, Object> parse(String json) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parser.map();
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static int activeShards(String indexName) throws IOException {
        String body = readAll(client().performRequest(new Request("GET", "/_cluster/health/" + indexName)));
        return extractIntPath(body, "active_shards");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hitList(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    /** {@code _source.id} of every hit, in response order. */
    @SuppressWarnings("unchecked")
    private static List<Integer> sourceIds(Map<String, Object> response) {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            ids.add(((Number) source.get("id")).intValue());
        }
        return ids;
    }

    /** The {@code sort} array of every hit, in response order; null entries for unsorted requests. */
    private static List<Object> sortValues(Map<String, Object> response) {
        List<Object> values = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            values.add(hit.get("sort"));
        }
        return values;
    }

    private static List<Double> scores(Map<String, Object> response) {
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            scores.add(((Number) hit.get("_score")).doubleValue());
        }
        return scores;
    }

    /** Buckets of the first bucket aggregation in the response, or an empty list when there is none. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buckets(Map<String, Object> response) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        for (Object aggregation : aggregations.values()) {
            if (aggregation instanceof Map<?, ?> map && map.get("buckets") instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
        }
        return List.of();
    }

    private static int extractIntPath(Map<String, Object> parsed, String... path) {
        Object value = parsed;
        for (String step : path) {
            if (value instanceof Map<?, ?> map) {
                value = map.get(step);
            } else {
                throw new AssertionError("cannot descend into " + value + " with step " + step);
            }
            if (value == null) {
                throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + parsed);
            }
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
    }

    private static int extractIntPath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.intValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static double extractDoublePath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }
}
