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
import java.util.stream.Stream;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
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
     * {@code tok<i>}, so a top 10 has no tie at its boundary.
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
        List<String> shapes = List.of(
            "\"size\":10,\"query\":" + oneHit,
            "\"size\":10,\"query\":" + manyHits,
            "\"size\":10,\"query\":" + oneHit + ",\"sort\":[{\"ts\":\"desc\"}]",
            "\"size\":10,\"query\":" + manyHits + ",\"sort\":[{\"ts\":\"desc\"}]",
            "\"size\":10,\"query\":" + filtered,
            "\"size\":10,\"track_total_hits\":true,\"query\":" + manyHits,
            "\"size\":0,\"query\":" + oneHit + "," + terms,
            "\"size\":0,\"query\":" + manyHits + "," + terms
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
     * {@code (i % 5) + 1} times so sixty rows over the three fragments
     * share the top score, {@code flag} and {@code category} take two
     * and three distinct values. Every page below crosses a tie group,
     * and the {@code from} and {@code search_after} pages start inside
     * one; ids and, where the request has them, sort values must agree
     * hit for hit.
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
        int fragments = 3;
        int rowsPerFragment = 100;
        LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String tied = "{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}";
        String filtered = "{\"bool\":{\"must\":[" + tied + "],\"filter\":[{\"term\":{\"category\":\"c1\"}}]}}";
        String byScore = ",\"sort\":[{\"_score\":\"desc\"}]";
        List<String> shapes = List.of(
            "\"size\":10,\"query\":" + tied + byScore,
            "\"from\":5,\"size\":5,\"query\":" + tied + byScore,
            "\"from\":55,\"size\":10,\"query\":" + tied + byScore,
            "\"size\":10,\"query\":" + filtered + byScore,
            "\"from\":3,\"size\":10,\"query\":" + filtered + byScore,
            "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"flag\":\"desc\"}]",
            "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"category\":\"asc\"},{\"_score\":\"desc\"}]",
            "\"from\":7,\"size\":10,\"query\":" + tied + ",\"sort\":[{\"category\":\"asc\"},{\"_score\":\"desc\"}]",
            "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"rating\":\"desc\"}]"
        );
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(fragments, extractIntPath(readAll(attach), "fragments"));
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));

            for (String shape : shapes) {
                assertFragmentPathMatchesShardPath(indexName, shape);
            }
            // The top ten of the score sort are the first ten rows with
            // i % 5 == 4, all in fragment 0, and the page starting at
            // 55 crosses from fragment 2 into the next score group.
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

            // search_after with a cursor on a tied (field value, score)
            // pair skips the whole tie group on both paths, and the next
            // group starts with the lowest row addresses again. A cursor
            // needs a field clause: OpenSearch rejects search_after on a
            // bare _score sort.
            List<String> cursorShapes = List.of(
                "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"_score\":\"desc\"},{\"flag\":\"desc\"}]",
                "\"size\":10,\"query\":" + tied + ",\"sort\":[{\"category\":\"asc\"},{\"_score\":\"desc\"}]"
            );
            for (String shape : cursorShapes) {
                Map<String, Object> firstPage = parse(readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}")));
                List<Object> cursor = sortValues(firstPage);
                String withCursor = shape + ",\"search_after\":" + toJson(cursor.get(cursor.size() - 1));
                assertFragmentPathMatchesShardPath(indexName, withCursor);
            }

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

    /** Name of the elected cluster manager, from {@code GET /_cat/cluster_manager}. */
    private static String clusterManagerNodeName() throws IOException {
        String name = readAll(client().performRequest(new Request("GET", "/_cat/cluster_manager?h=node"))).trim();
        assertFalse("_cat/cluster_manager returned no node name", name.isEmpty());
        return name;
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
        Map<String, Object> fragmentPath = parse(readAll(postJson("/" + indexName + "/_search", "{" + shape + "}")));
        Map<String, Object> shardPath = parse(readAll(postJson("/" + indexName + "/_search", "{\"explain\":true," + shape + "}")));
        assertEquals(shape, hitIdsOf(shardPath), hitIdsOf(fragmentPath));
        assertEquals(shape, sortValues(shardPath), sortValues(fragmentPath));
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

    /** Number of data nodes in the cluster, from {@code GET /_nodes/data:true}. */
    private static int dataNodeCount() throws IOException {
        return extractIntPath(readAll(client().performRequest(new Request("GET", "/_nodes/data:true"))), "_nodes", "total");
    }

    /**
     * The node name of a log line, which log4j prints in the fourth
     * bracket: {@code [time][level][logger] [node] message}.
     */
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
     * per node.
     */
    private static List<String> clusterLogLines() throws IOException {
        String property = System.getProperty("tests.lance.cluster_logs_dir");
        assertNotNull("tests.lance.cluster_logs_dir must be set by the Gradle build", property);
        List<String> lines = new ArrayList<>();
        try (Stream<Path> files = Files.walk(Path.of(property))) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                if (!file.toString().endsWith(".log") || !file.getParent().getFileName().toString().equals("logs")) {
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

    /** JSON of a parsed value (a hit's {@code sort} array here), to feed back as {@code search_after}. */
    private static String toJson(Object value) throws IOException {
        try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
            builder.value(value);
            return builder.toString();
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

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buckets(Map<String, Object> response) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> byCategory = (Map<String, Object>) aggregations.get("by_category");
        return (List<Map<String, Object>>) byCategory.get("buckets");
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
