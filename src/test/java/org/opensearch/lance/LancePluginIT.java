/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Smoke integration test for the plugin. Spins up a full OpenSearch test
 * cluster with the plugin installed and drives it through REST. Runs under
 * {@code ./gradlew integTest}.
 *
 * <p>Split into two layers:
 * <ul>
 *   <li>Route-level checks (plugin installed, endpoints reachable,
 *       register / attach error paths) that need no Lance table on disk.</li>
 *   <li>End-to-end checks that write a real Lance table onto the local
 *       filesystem via {@link LanceTableFactory}, register its parent
 *       directory as a namespace, wait for the poll cycle to surface the
 *       table as an OpenSearch index, then exercise match / GET / knn.</li>
 * </ul>
 * The cluster is configured with {@code lance.namespace.poll_cadence=1s}
 * (see {@code build.gradle}) so the end-to-end tests do not wait ten
 * seconds each for surfacing.
 */
// Lance JNI spins up native worker threads that outlive a single test
// method. Randomized test framework flags those as leaks; suppress at the
// suite level to match the plugin's other Lance-touching tests.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LancePluginIT extends OpenSearchRestTestCase {

    public void testPluginIsInstalled() throws IOException {
        // Ask the cluster for its plugin list and confirm the entry exists.
        // This catches classpath / metadata regressions before any Lance code
        // ever gets touched.
        Response response = client().performRequest(new Request("GET", "/_cat/plugins?format=json"));
        String body = readAll(response);
        assertTrue("expected opensearch-lance in _cat/plugins, saw: " + body, body.contains("opensearch-lance"));
    }

    public void testCircuitBreakerIsRegistered() throws IOException {
        // The plugin registers a lance_native breaker via
        // CircuitBreakerPlugin.getCircuitBreaker so operators can see
        // the native memory footprint through the standard
        // _nodes/stats/breaker API without a plugin-specific stats
        // endpoint. Assert that the breaker name and non-zero limit
        // show up on every node in a fresh cluster.
        Response response = client().performRequest(new Request("GET", "/_nodes/stats/breaker"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected lance_native breaker in _nodes/stats/breaker, saw: " + body, body.contains("lance_native"));
    }

    public void testNamespaceEndpointIsRegistered() throws IOException {
        // GET /_lance/namespace lists registered namespaces. On a fresh
        // cluster it returns an empty list, but the important assertion is
        // that the route is wired up and returns a 200 instead of 404.
        Response response = client().performRequest(new Request("GET", "/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testRegisterNamespace() throws IOException {
        String path = scratchPathString("register");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        Response post = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), post.getStatusLine().getStatusCode());

        // The register call should surface the path back through GET.
        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertTrue("expected namespace " + path + " in listing, saw: " + body, body.contains(path));
    }

    public void testRegisterNamespaceIsIdempotent() throws IOException {
        // Registering the same path twice must not duplicate the entry, so a
        // second POST is a no-op. If the polling loop scanned the same
        // catalog twice per cycle every table would surface twice as well.
        String path = scratchPathString("idempotent");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        int occurrences = countOccurrences(body, path);
        assertEquals("registered path should appear exactly once, saw: " + body, 1, occurrences);
    }

    public void testRegisterNamespaceRejectsMissingPath() throws IOException {
        // The `path` field is required. Without validation, the code cast the
        // body value to String and threw a 500. It must now be 400.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonStringPath() throws IOException {
        // A numeric or boolean `path` used to trip a ClassCastException. The
        // handler must catch the type mismatch and return 400.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{\"path\":42}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-string path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonExistentPath() throws IOException {
        // Registering a non-existent directory used to succeed silently and
        // then the poll cycle would list nothing forever. Validate up front.
        String phantom = sharedRoot().resolve("does-not-exist-" + randomAlphaOfLength(8)).toString();
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"path\":\"" + phantom + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-existent path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected 'does not exist' message, saw: " + body, body.contains("does not exist"));
    }

    public void testRegisterNamespaceRejectsFilePath() throws IOException {
        // A file path (not a directory) must also be rejected.
        java.nio.file.Path base = sharedRoot();
        java.nio.file.Path file = java.nio.file.Files.createFile(
            base.resolve("not-a-dir-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT) + ".lance")
        );
        try {
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/namespace", "{\"path\":\"" + file.toString() + "\"}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for file path, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected 'not a directory' message, saw: " + body, body.contains("not a directory"));
        } finally {
            java.nio.file.Files.deleteIfExists(file);
        }
    }

    public void testUnregisterNamespace() throws IOException {
        // Register, then unregister, then verify it disappears from GET.
        String path = scratchPathString("unregister");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        Response register = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

        Response unregister = deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), unregister.getStatusLine().getStatusCode());
        String unregBody = readAll(unregister);
        assertTrue("expected unregistered:true, saw: " + unregBody, unregBody.contains("\"unregistered\":true"));

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertFalse("expected namespace " + path + " to be gone, saw: " + body, body.contains(path));
    }

    public void testUnregisterUnknownNamespaceReturns404() throws IOException {
        String path = scratchPathString("neverreg") + "-x";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 404 for unregister of unknown path, saw " + status, 404, status);
    }

    public void testAttachRejectsMissingTable() throws IOException {
        // POST /_lance/attach with a path that does not exist on disk lets
        // Dataset.open throw. The plugin must surface this as an HTTP error
        // rather than crash the request thread or leak an unhandled 500.
        String bogus = scratchPathString("missing") + ".lance";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + bogus + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for missing table, saw: " + status, status >= 400);
    }

    public void testAttachRejectsMissingTableField() throws IOException {
        // The `table` field is required. Attach must return 400 with a
        // useful message rather than a 500 NullPointerException.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing table field, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [table], saw: " + body, body.contains("[table]"));
    }

    public void testAttachRejectsNonNumericShards() throws IOException {
        // Sending `number_of_shards` as a string previously crashed inside the
        // Object -> Number cast and returned 500. It must be rejected as 400.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"number_of_shards\":\"3\"}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-numeric number_of_shards, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [number_of_shards], saw: " + body, body.contains("[number_of_shards]"));
    }

    public void testAttachRefusesToClaimPlainIndex() throws IOException {
        // If someone (or a previous run) already created a plain OpenSearch
        // index sharing the name attach would default to, we must not return
        // `already_attached: true` and pretend it is a Lance index. The
        // expected outcome is 409 so the operator picks a different `name`.
        String indexName = "plain-collision-" + randomAlphaOfLength(6).toLowerCase(java.util.Locale.ROOT);
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity("{}");
        create.setOptions(create.getOptions().toBuilder().addHeader("Content-Type", "application/json"));
        client().performRequest(create);
        try {
            String tablePath = scratchPathString(indexName) + ".lance";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/attach", "{\"table\":\"" + tablePath + "\",\"name\":\"" + indexName + "\"}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            // Either 409 (index already exists as non-Lance) or another 4xx
            // when the fake table path fails to open. The critical property
            // is that we do NOT return 200 already_attached, which the old
            // attach path did.
            assertTrue("expected 4xx (not 200 already_attached), saw " + status, status >= 400 && status < 500);
        } finally {
            client().performRequest(new Request("DELETE", "/" + indexName));
        }
    }

    public void testAttachAcceptsStorageOptionsAndPersistsInSettings() throws Exception {
        // Local FS tables ignore Lance's object-store credentials, so the
        // payload here is a syntactic smoke test: the plugin has to parse
        // storage_options, persist every entry under
        // index.lance.storage_options.<key>, and still open the dataset
        // successfully. The values (aws_region / aws_endpoint) don't
        // affect the local scan.
        String suffix = "attachso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\",\"storage_options\":{\"aws_region\":\"us-east-1\",\"aws_endpoint\":\"https://s3.example.internal\"}}"
            );
            assertEquals(
                "attach with storage_options failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue("expected persisted aws_region: " + settingsBody, settingsBody.contains("\"aws_region\":\"us-east-1\""));
            assertTrue(
                "expected persisted aws_endpoint: " + settingsBody,
                settingsBody.contains("\"aws_endpoint\":\"https://s3.example.internal\"")
            );

            // Sanity: the persisted options do not prevent the engine from
            // serving reads against the local table.
            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}");
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 4 hits", 4, hits);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testAttachOmittingStorageOptionsPersistsNothing() throws Exception {
        // No storage_options field on the request must not seed any
        // index.lance.storage_options.* entry. Callers depend on this to
        // detect whether a Lance-backed index carries per-table options.
        String suffix = "attachnoso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 2);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertFalse("did not expect any storage_options in settings: " + settingsBody, settingsBody.contains("\"storage_options\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testAttachKeywordOnlyUtf8TableGoesGreen() throws Exception {
        // Regression for the SHA 403576c FieldInfos-duplicate bug: a
        // Utf8 column with no inverted index is loaded through the
        // keyword doc-values path AND through the text-column loop
        // that e21bf3c added for FLS visibility, and the old code
        // added both to the leaf reader's FieldInfos. Shard recovery
        // then failed with IllegalArgumentException: duplicate field
        // names and every FTS-less string-column table was red.
        // Verify the attach succeeds, the shard settles green, and a
        // term-level search against the keyword column returns hits
        // instead of a shards.failed response.
        String suffix = "keywordonly-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeKeywordOnlyTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on keyword-only Utf8 table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            // Health must not stay red. The recovery previously threw
            // IllegalArgumentException during LanceFragmentLeafReader
            // construction and marked the shard failed permanently.
            Response health = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            String healthBody = readAll(health);
            assertFalse("index went red: " + healthBody, healthBody.contains("\"status\":\"red\""));

            // Confirm derive() surfaced the column as keyword by
            // running a term query, which only works on keyword doc
            // values. If FieldInfos was duplicated the shard wouldn't
            // respond at all.
            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"label\":\"row-3\"}}}");
            String searchBody = readAll(search);
            int hits = extractIntPath(searchBody, "hits", "total", "value");
            assertEquals("expected exactly one match for label=row-3, saw: " + searchBody, 1, hits);

            Response mapping = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mappingBody = readAll(mapping);
            assertTrue("expected label mapped as keyword: " + mappingBody, mappingBody.contains("\"label\":{\"type\":\"keyword\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersFilterQueries() throws Exception {
        // Milestone 5-A of the shard-free dispatch prototype: setting
        // lance.dispatch.mode = fragment must route match_all *and*
        // filter queries (term / terms / exists / range / bool) on
        // Lance-backed indices through the plugin's own executor.
        // The count and the returned hits both come from Lance via
        // LanceKnnFilterTranslator, so hits.total.value stays in sync
        // with the number of matching hits regardless of shard state.
        // Query shapes outside the translator's whitelist (match on a
        // text field, geo, script, aggregations, sort, pagination)
        // still route through the standard shard fan-out; those
        // shapes are for later milestones.
        String suffix = "dispatch-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Baseline: shard mode returns the six rows Lance table
            // factory wrote. If this fails the standard path is broken
            // independently of the intercept.
            int baselineHits = extractIntPath(
                readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}")),
                "hits",
                "total",
                "value"
            );
            assertEquals("baseline shard mode should surface the six rows", 6, baselineHits);

            // Flip the cluster to fragment mode and confirm match_all
            // now goes through the plugin's own executor. The executor
            // reads the row count directly from the Lance dataset, so
            // hits.total.value must equal the row count regardless of
            // shard state. Milestone 3 also populates the hits array
            // with synthesised {@code _id}s sourced from Lance's
            // _rowaddr, so the response now looks like a standard
            // OpenSearch search response instead of an empty-hits
            // stub.
            updateClusterSetting("lance.dispatch.mode", "fragment");
            try {
                String matchAllBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
                int matchAllHits = extractIntPath(matchAllBody, "hits", "total", "value");
                assertEquals("fragment mode match_all must return the true row count", 6, matchAllHits);
                // Default size is 10 so a 6-row table returns all six
                // hits. Each hit carries a synthesised _id in the form
                // "<fragmentId>-<offset>" and a _source rendered from
                // the Arrow batch. The LanceTableFactory fixture puts
                // "hello lance " at even offsets and "quick brown fox"
                // at odd offsets in the body column; both must show
                // up in the response.
                assertTrue(
                    "fragment mode match_all must populate the hits array: " + matchAllBody,
                    matchAllBody.contains("\"_id\":\"0-0\"")
                );
                assertTrue(
                    "fragment mode match_all must render _source with the body column: " + matchAllBody,
                    matchAllBody.contains("hello lance")
                );
                assertTrue(
                    "fragment mode match_all must render _source with the id column: " + matchAllBody,
                    matchAllBody.contains("\"id\":0")
                );

                // A size=2 request returns only two hits but keeps the
                // total row count at six.
                String sizeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"size\":2}"));
                assertEquals("size clause must not affect total", 6, extractIntPath(sizeBody, "hits", "total", "value"));
                int returnedHits = countOccurrences(sizeBody, "\"_id\":");
                assertEquals("size=2 must return exactly two hits: " + sizeBody, 2, returnedHits);

                // Milestone 5-A: term queries on numeric columns are
                // now answered by the fragment executor itself, not
                // the shard fall-through. The count and hit metadata
                // both come from the plugin's own path via
                // LanceKnnFilterTranslator -> Dataset.countRows(sql) +
                // ScanOptions.filter(sql).
                String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}}}"));
                assertEquals("fragment mode term must match exactly one row", 1, extractIntPath(termBody, "hits", "total", "value"));
                // The synthesised _id sourced from _rowaddr is the
                // signal that the fragment executor answered the
                // request; shard mode would emit the same value but
                // via a different code path. What matters is that we
                // see the offset of the matching row (id = 3 lives at
                // offset 3 in fragment 0) and its _source column.
                assertTrue("fragment mode term must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));
                assertTrue("fragment mode term must render the matching row: " + termBody, termBody.contains("\"id\":3"));

                // A numeric range covers three rows (id in {2, 3, 4}).
                // Total must be 3 and the returned hits must include
                // id 2 and id 4 (id 3 is proven separately above).
                String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":2,\"lt\":5}}}}"));
                assertEquals("fragment mode range must count matching rows", 3, extractIntPath(rangeBody, "hits", "total", "value"));
                assertTrue("fragment mode range must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
                assertTrue("fragment mode range must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

                // Bool AND of two filters proves nested translation
                // works end-to-end. id >= 2 intersects id = 3, so the
                // response must count and return exactly the id=3 row.
                String boolBody = readAll(
                    postJson(
                        "/" + indexName + "/_search",
                        "{\"query\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":2}}},{\"term\":{\"id\":3}}]}}}"
                    )
                );
                assertEquals("fragment mode bool must count the intersection", 1, extractIntPath(boolBody, "hits", "total", "value"));
                assertTrue("fragment mode bool must return the id=3 hit: " + boolBody, boolBody.contains("\"_id\":\"0-3\""));

                // Query shapes outside the translator's whitelist
                // (match on a text field) still fall through to the
                // shard-based path. The baseline hit count comes from
                // shard mode, so this proves the fall-through is
                // wired correctly. Milestone 5-A intentionally leaves
                // full-text search on the shard executor.
                int matchHits = extractIntPath(
                    readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}")),
                    "hits",
                    "total",
                    "value"
                );
                assertTrue("match query must fall through to shard path and still return hits (got " + matchHits + ")", matchHits > 0);
            } finally {
                updateClusterSetting("lance.dispatch.mode", "shard");
            }

            // Post-recovery: reverting to shard mode brings back real
            // Lance hits without another restart.
            int recoveredHits = extractIntPath(
                readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}")),
                "hits",
                "total",
                "value"
            );
            assertEquals("recovered shard mode should surface the six rows again", 6, recoveredHits);
        } finally {
            try {
                updateClusterSetting("lance.dispatch.mode", "shard");
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersMetricAggregations() throws Exception {
        // Milestone 5-B of the shard-free dispatch prototype: setting
        // lance.dispatch.mode = fragment must let the plugin's own
        // executor answer the five metric aggregations
        // LanceMetricAggregator supports (value_count, sum, avg, min,
        // max), possibly combined with a filter query. The count,
        // hits, and aggregation branches all share the same filter
        // push-down, so a filter query narrows the aggregation the
        // same way it narrows the count. Bucket aggregations, script
        // metrics, and multi-index aggregation continue to route
        // through the standard shard fan-out.
        String suffix = "aggs-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            updateClusterSetting("lance.dispatch.mode", "fragment");
            try {
                // The fixture writes six rows with id = 0..5, so the
                // canonical aggregates are: count = 6, sum = 15,
                // avg = 2.5, min = 0, max = 5. size=0 is standard for
                // aggregation-only requests and avoids paying the
                // hits scan on the same request.
                String body = readAll(
                    postJson(
                        "/" + indexName + "/_search",
                        "{\"size\":0,\"aggs\":{"
                            + "\"c\":{\"value_count\":{\"field\":\"id\"}},"
                            + "\"s\":{\"sum\":{\"field\":\"id\"}},"
                            + "\"a\":{\"avg\":{\"field\":\"id\"}},"
                            + "\"m\":{\"min\":{\"field\":\"id\"}},"
                            + "\"M\":{\"max\":{\"field\":\"id\"}}"
                            + "}}"
                    )
                );
                assertEquals(
                    "aggregation must count matching rows via Dataset.countRows",
                    6,
                    extractIntPath(body, "hits", "total", "value")
                );
                assertEquals("value_count on id must equal row count", 6, extractIntPath(body, "aggregations", "c", "value"));
                assertEquals("sum(id) 0..5 == 15", 15.0d, extractDoublePath(body, "aggregations", "s", "value"), 0.0d);
                assertEquals("avg(id) 0..5 == 2.5", 2.5d, extractDoublePath(body, "aggregations", "a", "value"), 0.0d);
                assertEquals("min(id) == 0", 0.0d, extractDoublePath(body, "aggregations", "m", "value"), 0.0d);
                assertEquals("max(id) == 5", 5.0d, extractDoublePath(body, "aggregations", "M", "value"), 0.0d);

                // Filter query pushes the same SQL predicate into
                // Dataset.countRows and ScanOptions.filter, so
                // sum(id where id >= 2) must equal 2+3+4+5 = 14 with
                // total = 4.
                String filteredBody = readAll(
                    postJson(
                        "/" + indexName + "/_search",
                        "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}}," + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                    )
                );
                assertEquals("filtered aggregation must count filtered rows", 4, extractIntPath(filteredBody, "hits", "total", "value"));
                assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(filteredBody, "aggregations", "s", "value"), 0.0d);

                // Hits + aggregation in the same request must both
                // come from the fragment executor: hits carry the
                // synthesised _rowaddr id, aggregations carry the
                // computed value.
                String hitsPlusAggs = readAll(
                    postJson(
                        "/" + indexName + "/_search",
                        "{\"size\":2,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                    )
                );
                assertEquals("total unchanged when hits also requested", 6, extractIntPath(hitsPlusAggs, "hits", "total", "value"));
                assertTrue("hits section must carry the synthesised _id: " + hitsPlusAggs, hitsPlusAggs.contains("\"_id\":\"0-0\""));
                assertEquals(
                    "sum unchanged when hits also requested",
                    15.0d,
                    extractDoublePath(hitsPlusAggs, "aggregations", "s", "value"),
                    0.0d
                );

                // Direction 1 Stage 2: terms bucket aggregation
                // now runs on the fragment executor via the stock
                // TermsAggregator against per-fragment
                // LanceFragmentLeafReaders. The response must carry
                // one bucket per distinct id value (6 rows, all
                // unique ids 0..5 = 6 buckets, doc_count=1 each).
                String bucketBody = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}")
                );
                assertTrue(
                    "terms aggregation via fragment path carries buckets: " + bucketBody,
                    bucketBody.contains("\"buckets\":")
                );
                assertEquals(6, extractIntPath(bucketBody, "hits", "total", "value"));
            } finally {
                updateClusterSetting("lance.dispatch.mode", "shard");
            }
        } finally {
            try {
                updateClusterSetting("lance.dispatch.mode", "shard");
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    private static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    public void testAttachRejectsNonObjectStorageOptions() throws IOException {
        // Sending storage_options as a string used to slip past parse into
        // Lance and surface as a confusing "map required" native error.
        // Reject at 400 with a message pointing at storage_options.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"storage_options\":\"not-an-object\"}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-object storage_options, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about storage_options: " + body, body.contains("storage_options"));
        assertTrue("expected message about JSON object: " + body, body.contains("JSON object"));
    }

    public void testAttachRejectsNestedStorageOptionsValue() throws IOException {
        // Values must be strings; nested objects would silently
        // toString() at the JNI boundary. Reject up front.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\"," + "\"storage_options\":{\"aws_config\":{\"nested\":\"value\"}}}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for nested storage_options value, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about aws_config: " + body, body.contains("aws_config"));
        assertTrue("expected message about must be a string: " + body, body.contains("must be a string"));
    }

    public void testNamespaceRegisterPropagatesStorageOptionsToAutoSurfacedIndex() throws Exception {
        // Namespace-level storage_options must ride into every auto-
        // surfaced index's settings — that is how a namespace pointing at
        // an S3 root gives every table under it the same credentials
        // without repeating them per table.
        String suffix = "nsso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 2);
        String indexName = tableName;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"path\":\"" + scratchDir.toString() + "\",\"storage_options\":{\"aws_region\":\"eu-west-1\"}}"
            );
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );
            assertBusy(() -> {
                Response cat = client().performRequest(new Request("GET", "/_cat/indices?format=json"));
                String body = readAll(cat);
                assertTrue("waiting for index " + indexName + ", saw: " + body, body.contains("\"" + indexName + "\""));
            });

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue(
                "expected namespace aws_region to propagate: " + settingsBody,
                settingsBody.contains("\"aws_region\":\"eu-west-1\"")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testBuildIndexesOnUnknownIndexFails() throws IOException {
        // The manual build endpoint targets a specific OpenSearch index. When
        // the index does not exist the call must fail rather than silently
        // no-op, so operators using the recovery path see the mistake.
        String unknown = "does-not-exist-" + randomAlphaOfLength(8);
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/build_indexes/" + unknown, "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for unknown index, saw: " + status, status >= 400);
    }

    public void testAttachAndMatch() throws Exception {
        // End-to-end: build a real Lance table, wait for the polling loop to
        // surface it as an OpenSearch index, then confirm a match query
        // routes into the plugin engine and returns the rows whose body
        // matches the search term.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "attachAndMatch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"hello\"}}}");
            String body = readAll(search);
            // 16 rows total, even rows say "hello lance i", odd rows say
            // "quick brown fox i". Half the rows should match.
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 8 hits (even rows), saw response: " + body, 8, totalHits);
        }
    }

    public void testAttachAndGetById() throws Exception {
        // Without a declared primary key on the Lance side (see LanceTableFactory
        // — adding the metadata breaks the C Data serialisation of the
        // FixedSizeList vector column), attach must publish an empty
        // primary_key_field. B10 guarantees that engine.get returns
        // NOT_EXISTS immediately in that case rather than 500-ing on an
        // empty filter column, so GET /_doc must return 404.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(8, "attachAndGet")) {
            String indexName = fixture.indexName();

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue(
                "expected empty primary_key_field in settings, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_field\":\"\"")
            );

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/3"))
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 404 when PK is not declared, saw " + status, 404, status);
        }
    }

    public void testAttachOfPkLessTableDisablesGet() throws Exception {
        // Same intent as testAttachAndGetById but uses a second, independent
        // fixture so the assertion still covers the derivation branch when
        // the primary integer column is not the first field.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "nopkget")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/0"))
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 404 when PK is not declared, saw " + status, 404, status);
        }
    }

    public void testPkLessTableSynthesisesUniqueIdsInSearchResults() throws Exception {
        // Regression for #24: a table with no declared primary key used to
        // emit _id: "0" for every hit because the reader's values[] array
        // stayed at its default long[] zeros. Every hit collapsed to the
        // same id and any client that dedup'd by _id (Dashboards result
        // grids, _mget by hits, etc.) silently lost rows. Synthesised ids
        // must at least be unique within the shard.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "nopksearch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"size\":6}");
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 6 hits, saw response: " + body, 6, totalHits);
            java.util.Set<String> ids = new java.util.HashSet<>();
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, body)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
                for (Object hitObj : hits) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> hit = (java.util.Map<String, Object>) hitObj;
                    ids.add((String) hit.get("_id"));
                }
            }
            assertEquals("expected 6 unique synthesised ids, saw: " + ids + " (body=" + body + ")", 6, ids.size());
        }
    }

    public void testAttachAndKnn() throws Exception {
        // Row i sits at coordinate (i, 0, 0, ...) so the nearest neighbour
        // of (2.4, 0, ...) is row 2 followed by row 3. Vector index build is
        // skipped (256-row floor); the plugin falls back to a brute-force
        // scan, which is fine for a 12-row table.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(12, "attachAndKnn")) {
            String indexName = fixture.indexName();

            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
            );
            String body = readAll(search);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected row 2 as nearest, saw: " + body, 2, firstId);
            assertEquals("expected row 3 as second, saw: " + body, 3, secondId);
        }
    }

    public void testLanceKnnRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnUnknown")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"noSuchField\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField, saw: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceKnnRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnScalar")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"id\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_vector, saw: " + body, body.contains("lance_vector"));
        }
    }

    public void testLanceKnnRejectsDimensionMismatch() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnDim")) {
            String indexName = fixture.indexName();
            // The fixture writes a FixedSizeList<Float32, 8>; a 3-element
            // query vector must be rejected up front rather than reaching
            // Lance.
            String queryVector = "[0.1,0.2,0.3]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for dimension mismatch, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about dimension, saw: " + body, body.contains("dimension"));
        }
    }

    public void testLanceMatchPhraseHonoursPhraseOrder() throws Exception {
        // The custom lance_match_phrase DSL routes into Lance's
        // FullTextQuery.phrase, which honours phrase order using the
        // positions written into the FTS index (LanceTableFactory builds
        // the body_fts index with with_position=true). Even rows say
        // "hello lance i", so "hello lance" hits 8 rows and the reversed
        // "lance hello" hits 0.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseorder")) {
            String indexName = fixture.indexName();

            Response ordered = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}}}"
            );
            int orderedHits = extractIntPath(readAll(ordered), "hits", "total", "value");
            assertEquals("expected 8 hits for 'hello lance' phrase", 8, orderedHits);

            Response reversed = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"lance hello\"}}}"
            );
            int reversedHits = extractIntPath(readAll(reversed), "hits", "total", "value");
            assertEquals("expected 0 hits for 'lance hello' reversed phrase", 0, reversedHits);
        }
    }

    public void testLanceMatchPhraseSlopBridgesGap() throws Exception {
        // Odd rows say "quick brown fox i". "quick fox" with slop=0 must
        // fail (brown between them), slop>=1 must succeed. Confirms the
        // slop parameter reaches Lance.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseslop")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for tight 'quick fox' phrase", 0, strictHits);

            Response withSlop = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\",\"slop\":1}}}"
            );
            int slopHits = extractIntPath(readAll(withSlop), "hits", "total", "value");
            assertEquals("expected 8 hits for 'quick fox' phrase with slop=1", 8, slopHits);
        }
    }

    public void testLanceMatchAndOperatorRestrictsToDocumentsMatchingAllTokens() throws Exception {
        // Even rows say "hello lance i", odd rows say "quick brown fox i".
        // OR "hello quick" would return 16 (every row has one). AND
        // "hello quick" returns 0 because no row has both. lance_match
        // must honour the operator via FullTextQuery.match's Operator
        // parameter.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchand")) {
            String indexName = fixture.indexName();

            Response orQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\"}}}"
            );
            int orHits = extractIntPath(readAll(orQuery), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello quick'", 16, orHits);

            Response andQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(andQuery), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello quick'", 0, andHits);

            // Same 'hello lance' AND both tokens present in even rows.
            Response andSameRow = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello lance\",\"operator\":\"and\"}}}"
            );
            int andSameRowHits = extractIntPath(readAll(andSameRow), "hits", "total", "value");
            assertEquals("expected 8 hits for AND 'hello lance'", 8, andSameRowHits);
        }
    }

    public void testLanceMatchFuzzinessAllowsSingleEdit() throws Exception {
        // Even rows say "hello lance i". "helo" is edit distance 1 from
        // "hello"; without fuzziness the FTS analyzer matches zero rows,
        // with fuzziness=1 it must match all 8 even rows. Confirms
        // fuzziness reaches Lance rather than being silently dropped.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchfuzz")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for exact 'helo'", 0, strictHits);

            Response fuzzy = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\",\"fuzziness\":1}}}"
            );
            int fuzzyHits = extractIntPath(readAll(fuzzy), "hits", "total", "value");
            assertEquals("expected 8 hits for fuzzy 'helo' (edit distance 1 to hello)", 8, fuzzyHits);
        }
    }

    public void testLanceMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match\":{\"field\":\"noSuchField\",\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"id\",\"query\":\"hello\"}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceMultiMatchHitsEitherField() throws Exception {
        // Even rows say body="hello lance i", title="sunny morning i".
        // Odd rows say body="quick brown fox i", title="cloudy morning i".
        // multi_match "hello cloudy" on [body, title] with OR must hit
        // every row: even rows via body:hello, odd rows via title:cloudy.
        // Confirms Lance's multi_match reads both columns and unions the
        // per-field matches.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboth")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits across body+title, saw response above", 16, hits);
        }
    }

    public void testLanceMultiMatchLimitsToListedFields() throws Exception {
        // "morning" only appears in title. Restricting the search to
        // [body] must return 0, while listing [body, title] must return
        // every row.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmscope")) {
            String indexName = fixture.indexName();

            Response bodyOnly = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\"],\"query\":\"morning\"}}}"
            );
            int bodyHits = extractIntPath(readAll(bodyOnly), "hits", "total", "value");
            assertEquals("expected 0 hits when only body is searched", 0, bodyHits);

            Response both = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"morning\"}}}"
            );
            int bothHits = extractIntPath(readAll(both), "hits", "total", "value");
            assertEquals("expected 16 hits when title is included", 16, bothHits);
        }
    }

    public void testLanceMultiMatchAndOperator() throws Exception {
        // multi_match "hello sunny" on [body, title] with AND: only rows
        // whose combined fields contain both tokens should match. Even
        // rows have body:hello + title:sunny; odd rows have neither. So
        // OR returns 8 (even rows) and AND also returns 8. To distinguish
        // OR vs AND semantics, "hello cloudy" AND must return 0 (no row
        // has both hello and cloudy anywhere in body|title), while OR
        // returns 16.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmand")) {
            String indexName = fixture.indexName();

            Response or = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int orHits = extractIntPath(readAll(or), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello cloudy'", 16, orHits);

            Response and = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(and), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello cloudy' (no row has both)", 0, andHits);
        }
    }

    public void testLanceMultiMatchWithBoostsSmoke() throws Exception {
        // Smoke test that per-field boosts parse and reach Lance without
        // erroring out. Even rows match both terms; asserting 8 hits
        // proves the query executed, and using distinct boosts exercises
        // the boosts list path in Lance's MultiMatchQuery.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboosts")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"]," + "\"query\":\"hello sunny\",\"boosts\":[2.0,1.0]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits for even rows matching hello+sunny", 8, hits);
        }
    }

    public void testLanceMultiMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"noSuchField\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMultiMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"id\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceFtsBoostReturnsPositiveMatches() throws Exception {
        // The lance_fts_boost DSL composes two Lance FTS clauses so the
        // positive set defines the hits and the negative clause only
        // affects scoring. With positive "hello" (even rows) and a
        // negative "fox" (odd rows, disjoint), the hit set must equal
        // the positive set (8 even rows). Confirms Lance's BoostQuery
        // wiring and that non-overlapping negatives do not drop hits.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbboost")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"fox\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from positive 'hello'", 8, hits);
        }
    }

    public void testLanceFtsBoostPenalisesOverlappingNegative() throws Exception {
        // Even rows say body="hello lance i", so positive "hello" and
        // negative "lance" match the same 8 rows. Under Lance's
        // BoostQuery, matching rows score positive*negative_boost, so
        // the top _score with negative_boost=0.1 must be strictly less
        // than the top _score of the same positive without any negative
        // wrapper.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbpenalise")) {
            String indexName = fixture.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            String baselineBody = readAll(baseline);
            int baselineHits = extractIntPath(baselineBody, "hits", "total", "value");
            assertEquals("baseline expects 8 hits", 8, baselineHits);
            double baselineScore = extractDoublePath(baselineBody, "hits", "hits", "0", "_score");

            Response boosted = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            String boostedBody = readAll(boosted);
            int boostedHits = extractIntPath(boostedBody, "hits", "total", "value");
            assertEquals("boosted expects 8 hits (same positive set)", 8, boostedHits);
            double boostedScore = extractDoublePath(boostedBody, "hits", "hits", "0", "_score");
            assertTrue("expected boosted score < baseline (" + boostedScore + " vs " + baselineScore + ")", boostedScore < baselineScore);
        }
    }

    public void testLanceFtsBoostRejectsNonLanceFtsClause() throws Exception {
        // The positive clause below is a stock OpenSearch `match`, not a
        // Lance FTS DSL. Lance's boost engine only takes FullTextQuery
        // subclauses, so the plugin must reject with 400 rather than
        // silently falling back to a Lucene bool that would break score
        // composition.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lfbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_boost\":{"
                        + "\"positive\":{\"match\":{\"body\":\"hello\"}},"
                        + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS positive, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS query: " + body, body.contains("Lance FTS query"));
        }
    }

    public void testLanceFtsBoolMustClauseFiltersToMatchingRows() throws Exception {
        // With a single must clause the bool query is equivalent to
        // running the inner Lance FTS DSL directly: must=body:hello →
        // 8 even rows. Confirms Lance's booleanQuery accepts a lone
        // MUST clause and hands hits back through the plugin.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmust")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from must body:hello", 8, hits);
        }
    }

    public void testLanceFtsBoolMustNotExcludesOverlappingClause() throws Exception {
        // Even rows say body="hello lance i", so must=body:hello and
        // must_not=body:lance target the same 8 rows and must_not knocks
        // all of them out. Confirms MUST_NOT reaches Lance rather than
        // being silently ignored.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustnot")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
                    + "\"must_not\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 0 hits after must_not:lance eliminates every hello match", 0, hits);
        }
    }

    public void testLanceFtsBoolShouldUnionsAcrossClauses() throws Exception {
        // Two should clauses on disjoint sets (body:hello even, title:cloudy
        // odd) with no must should return the union — 16 rows. This also
        // exercises the multi-field bool composition.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbshould")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"cloudy\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits from union of body:hello ∪ title:cloudy", 16, hits);
        }
    }

    public void testLanceFtsBoolMustAcrossFieldsIntersects() throws Exception {
        // must=body:hello (even) AND must=title:sunny (even) intersect
        // on the 8 even rows. Confirms MUST clauses on different columns
        // compose as an intersection on Lance's side.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustintersect")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"sunny\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from intersection body:hello ∩ title:sunny", 8, hits);
        }
    }

    public void testLanceFtsBoolRejectsNonLanceFtsClause() throws Exception {
        // Stock OpenSearch `match` is not a Lance FTS DSL. Rejecting at
        // 400 rather than falling back to a Lucene bool keeps score
        // composition on Lance's side and avoids silently mixing two
        // scoring systems on the same query.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"match\":{\"body\":\"hello\"}}]}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS must, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS: " + body, body.contains("Lance FTS"));
        }
    }

    public void testLanceFtsBoolEmptyClausesRejected() throws Exception {
        // Lance's booleanQuery constructor rejects an empty clauses list.
        // The plugin catches this at parse time and returns 400 with a
        // message pointing the caller at the three lists they can fill.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbempty")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_fts_bool\":{}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for empty bool, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about must/should/must_not: " + body, body.contains("must_not"));
        }
    }

    public void testDropColumnMarksLanceTextFieldDroppedAndRejectsQuery() throws Exception {
        // Drop the body column on the Lance side. The polling loop must
        // detect the dropped Lance field id, add lance_dropped=true to
        // the mapping meta, and cause subsequent lance_match queries
        // against `body` to fail with 400 instead of silently returning
        // zero hits.
        //
        // Note: LanceTableFactory.writeTable occasionally fails with
        // "The FixedSizeList type requires an integer parameter" on
        // Lance 11.0.0 when the FFI schema handoff races the first
        // native table creation of a JVM under certain random seeds
        // (observed at seed 2519BC84C706C3F8 and 3CFD7BCD1F5069CD).
        // Retry the setUp once to swallow that flake; a persistent
        // failure still surfaces on the second attempt.
        LanceTestCluster fixture;
        try {
            fixture = LanceTestCluster.setUp(16, "dropbody");
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("FixedSizeList type requires an integer parameter")) {
                fixture = LanceTestCluster.setUp(16, "dropbody");
            } else {
                throw e;
            }
        }
        try (LanceTestCluster f = fixture) {
            String indexName = f.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            int baselineHits = extractIntPath(readAll(baseline), "hits", "total", "value");
            assertEquals("expected 8 baseline hits for body:hello", 8, baselineHits);

            LanceTableFactory.dropColumns(f.tableUri(), java.util.List.of("body"));

            // Wait for the poll to detect the version bump and PutMapping
            // lance_dropped=true. Poll cadence is 1s in build.gradle; the
            // mapping update fires on the next syncTable that sees the
            // dropped id.
            assertBusy(() -> {
                Response mapping = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
                String body = readAll(mapping);
                assertTrue(
                    "expected lance_dropped meta on body after drop, saw: " + body,
                    body.contains("\"body\"") && body.contains("\"lance_dropped\":\"true\"")
                );
            });

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 after body dropped, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected 'no longer exists' message, saw: " + body, body.contains("no longer exists"));
        }
    }

    public void testLanceKnnAppliesFilterAsPreFilter() throws Exception {
        // With row i at coordinate (i, 0, ...), the two rows nearest to
        // (2.4, 0, ...) are id 2 and id 3. A pre-filter of id >= 10 must
        // keep k=2 populated with the two nearest matches among {10..15},
        // i.e. id 10 (distance 7.6) and id 11 (distance 8.6). A post-filter
        // would pull id 2 / 3 into the top-K first, then drop them, and
        // return 0 hits.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilter")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for id >= 10 with k=2, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected id 10 as nearest match >= 10, saw: " + body, 10, firstId);
            assertEquals("expected id 11 as second nearest match, saw: " + body, 11, secondId);
        }
    }

    public void testLanceKnnFilterAcceptsBoolFilterClause() throws Exception {
        // Same expectation as testLanceKnnAppliesFilterAsPreFilter but with
        // the range clause nested inside bool.filter, so the translator's
        // bool + range path is exercised end-to-end.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilterBool")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":10}}}]}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for bool filter id >= 10, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected id 10 as nearest match, saw: " + body, 10, firstId);
        }
    }

    public void testLanceKnnRejectsUnsupportedFilterClause() throws Exception {
        // `match` cannot be lowered to a Lance SQL filter safely (analysis
        // would happen server-side, not in Lance), so the translator
        // rejects it up front with 400. Doing so beats silently degrading
        // recall or returning an obscure Lance parse error.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnPreFilterMatch")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                        + queryVector
                        + ",\"k\":2,\"filter\":{\"match\":{\"body\":\"hello\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unsupported filter, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about MatchQueryBuilder, saw: " + body, body.contains("MatchQueryBuilder"));
        }
    }

    public void testBoolShouldComposesLanceMatchWithLanceKnn() throws Exception {
        // Hybrid-shape query at the shard level: two independent Lance
        // sub-queries fan out inside a bool.should. lance_match:body:hello
        // matches every even row (0, 2, 4, ..., 14; 8 rows). lance_knn
        // near (0.5, 0, ..., 0) with k=2 returns the two closest rows
        // by Euclidean distance, which are id 0 (distance 0.5) and id 1
        // (distance 0.5). The union is 9 rows: id 1 is the only knn hit
        // not already in the match set.
        //
        // This is the same per-shard plumbing neural-search's `hybrid`
        // query relies on: HybridQuery.createWeight iterates the
        // sub-queries, calls createWeight on each, and composes their
        // scorers. As long as our Lance queries honour the Lucene
        // Weight / ScorerSupplier / Scorer contract inside a compound
        // query (as this test proves for bool.should), the hybrid query
        // will drive them identically.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridboolshould")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            // Row 0 satisfies both sub-queries and must score highest of
            // any single-sub-query hit, so the sort by _score lands it
            // first. This is Lucene's bool.should sum-of-child-scores
            // behaviour; hybrid replaces the sum with per-sub-query
            // top-K + coordinator-side normalisation, but the shard-side
            // requirement is the same: each sub-query yields the same
            // scored docs it would on its own.
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 (matches both sub-queries) at top of hits: " + body, 0, topId);
        }
    }

    public void testBoolShouldComposesStockMatchOnLanceTextWithLanceKnn() throws Exception {
        // Stock OpenSearch `match` on a lance_text field goes through
        // LanceTextFieldMapper.termQuery, which builds a LanceFtsQuery
        // for the single-token case. Confirm the composition works the
        // same way when the FTS clause uses the plain match DSL rather
        // than the lance_match DSL: the shard-side composition contract
        // that hybrid depends on does not vary between them.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridstockmatchknn")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"match\":{\"body\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 at top: " + body, 0, topId);
        }
    }

    public void testDefaultSearchReturnsAtLeastTenHits() throws Exception {
        // Regression for the FetchPhase sequential-stored-fields path: with
        // >= 10 adjacent doc ids and no deletions the fetch phase calls
        // getSequentialStoredFieldsReader on the leaf reader. Before the
        // LanceSequentialLeafReader wrapper this threw "requires a
        // CodecReader or a SequentialStoredFieldsLeafReader", so GET
        // /demo/_search with the default size=10 returned 500. Sixteen rows
        // exercises the >= 10 hits case; every hit must materialise cleanly.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "defaultsearch")) {
            String indexName = fixture.indexName();

            Response search = client().performRequest(new Request("GET", "/" + indexName + "/_search"));
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 16 total hits, saw response: " + body, 16, totalHits);
            // Default size is 10; hits array must be full and each entry
            // must carry the Lance-backed _source.
            assertTrue("expected hits[0]._source in response, saw: " + body, body.contains("\"_source\""));
        }
    }

    public void testNullableAndWideIntColumnsBehaveCorrectly() throws Exception {
        // Regression for B4 (per-column presence bitmaps, int8/int16/int64
        // mapping, boolean null). Before the fix a Lance table with any of
        // - nullable int32 / date / timestamp column
        // - int8 or int16 column
        // - a null in a boolean column
        // - int64 that exceeds Integer.MAX_VALUE
        // took the shard red on recovery or produced silently wrong
        // results. The nullable table has 12 rows; id=5 is all-null.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("nullableints")) {
            String indexName = fixture.indexName();

            // Mapping must reflect the Arrow int widths as byte / short / long.
            Response mappingResp = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mapping = readAll(mappingResp);
            assertTrue("expected count8 as byte in mapping: " + mapping, mapping.contains("\"count8\":{\"type\":\"byte\""));
            assertTrue("expected count16 as short in mapping: " + mapping, mapping.contains("\"count16\":{\"type\":\"short\""));
            assertTrue("expected count64 as long in mapping: " + mapping, mapping.contains("\"count64\":{\"type\":\"long\""));

            // Range on count64 must handle > Integer.MAX_VALUE values.
            // count64 = 4_000_000_000 + i, so gte 4_000_000_006 hits i>=6.
            // id=5 is null so exactly six rows should match.
            Response gteResp = postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"query\":{\"range\":{\"count64\":{\"gte\":4000000006}}}}"
            );
            int gteHits = extractIntPath(readAll(gteResp), "hits", "total", "value");
            assertEquals("range count64 gte 4000000006 should hit rows i=6..11", 6, gteHits);

            // exists on any nullable column must skip the all-null row.
            Response existsResp = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"exists\":{\"field\":\"count8\"}}}");
            int existsHits = extractIntPath(readAll(existsResp), "hits", "total", "value");
            assertEquals("exists count8 should count 11 present rows (12 minus one null)", 11, existsHits);

            // flag=true holds when i in {0,3,6,9}: four rows.
            Response flagTrue = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":true}}}");
            int trueHits = extractIntPath(readAll(flagTrue), "hits", "total", "value");
            assertEquals("term flag=true should match i in {0,3,6,9}", 4, trueHits);

            // flag=false must NOT include the null row (id=5). Non-null
            // false rows are i in {1,2,4,7,8,10,11} = 7.
            Response flagFalse = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":false}}}");
            int falseHits = extractIntPath(readAll(flagFalse), "hits", "total", "value");
            assertEquals("term flag=false should count non-null false rows and skip the null row", 7, falseHits);
        }
    }

    public void testStatsAPIsSucceedForLanceIndex() throws Exception {
        // Regression for LanceReadOnlyEngine.docStats() / segmentsStats():
        // OpenSearch's default implementations traverse leaves via
        // Lucene.segmentReader(reader), which throws for Lance leaves and
        // takes out _stats / _cat/indices docs.count / _nodes/stats /
        // _cluster/stats node-wide. Confirm the plugin overrides succeed
        // and the node-level stats aggregation runs without shard failures.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "statsapi")) {
            String indexName = fixture.indexName();

            Response indexStats = client().performRequest(new Request("GET", "/" + indexName + "/_stats"));
            assertEquals(RestStatus.OK.getStatus(), indexStats.getStatusLine().getStatusCode());
            String body = readAll(indexStats);
            int failed = extractIntPath(body, "_shards", "failed");
            assertEquals("expected zero shard failures on _stats, saw: " + body, 0, failed);

            Response segments = client().performRequest(new Request("GET", "/" + indexName + "/_segments"));
            assertEquals(RestStatus.OK.getStatus(), segments.getStatusLine().getStatusCode());

            Response nodesStats = client().performRequest(new Request("GET", "/_nodes/stats/indices/docs"));
            assertEquals(RestStatus.OK.getStatus(), nodesStats.getStatusLine().getStatusCode());
            String nodesBody = readAll(nodesStats);
            int nodesFailed = extractIntPath(nodesBody, "_nodes", "failed");
            assertEquals("expected zero node failures on _nodes/stats, saw: " + nodesBody, 0, nodesFailed);
        }
    }

    /**
     * Fixture that writes a Lance table into a scratch directory, registers
     * that directory as a Lance namespace, waits for the polling loop to
     * surface the table, and cleans everything up on close. The scratch
     * directory sits under {@code java.io.tmpdir} so the child test-cluster
     * process (which lives in a separate work directory) can still open it.
     */
    private static final class LanceTestCluster implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private LanceTestCluster(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        /**
         * Absolute filesystem URI of the underlying Lance table. Tests that
         * need to mutate the table (drop columns, append rows) can hand
         * this to {@link LanceTableFactory}.
         */
        String tableUri() {
            return scratchDir.resolve("demo-" + scratchDir.getFileName().toString().substring("lance-it-".length()) + ".lance").toString();
        }

        static LanceTestCluster setUp(int rowCount, String testHint) throws Exception {
            // Anchor the shared directory at the path build.gradle passes in
            // via tests.lance.shared_tables_dir. Test JVM and cluster JVM see
            // the same location without relying on a system-wide tmpdir.
            Path base = sharedRoot();
            // Lowercase everything. OpenSearch rejects index names that
            // contain any uppercase character, and the surfaced index name
            // is derived from the table directory name.
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeTable(scratchDir, tableName, rowCount);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        static LanceTestCluster setUpNullable(String testHint) throws Exception {
            Path base = sharedRoot();
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeNullableTable(scratchDir, tableName);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        private static LanceTestCluster registerAndWait(Path scratchDir, String indexName) throws Exception {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );

            // Poll cadence is 1s (see build.gradle); the assertBusy default
            // (10s) leaves plenty of headroom for the surface path to run.
            assertBusy(() -> {
                Response cat = client().performRequest(new Request("GET", "/_cat/indices?format=json"));
                String body = readAll(cat);
                assertTrue("waiting for index " + indexName + ", saw: " + body, body.contains("\"" + indexName + "\""));
            });
            // Recovery still races the first request on newer OpenSearch
            // versions; block until the shard is green before returning so
            // GET / search do not see RECOVERING.
            ensureGreen(indexName);
            return new LanceTestCluster(scratchDir, indexName);
        }

        @Override
        public void close() throws IOException {
            // Best-effort cleanup so leftover scratch dirs do not pile up
            // when a test fails.
            deleteRecursively(scratchDir);
        }
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

    private static Path sharedRoot() {
        String configured = System.getProperty("tests.lance.shared_tables_dir");
        if (configured == null || configured.isEmpty()) {
            throw new AssertionError("tests.lance.shared_tables_dir is unset; build.gradle should pass it into the integTest task");
        }
        return Path.of(configured);
    }

    private static String scratchPathString(String label) {
        return sharedRoot().resolve("lance-it-" + label + "-" + randomAlphaOfLength(8)).toString();
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int i = 0;
        while ((i = haystack.indexOf(needle, i)) != -1) {
            count++;
            i += needle.length();
        }
        return count;
    }

    private static String readAll(Response response) throws IOException {
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Small helper to pluck a value out of a JSON response without pulling
     * in Jackson. Traverses object keys or array indices in order.
     */
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

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
