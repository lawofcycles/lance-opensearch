/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * What the stock {@code _snapshot} API captures for a Lance-backed index
 * and what a restore of that snapshot produces. The snapshot carries the
 * index metadata (settings and mapping) plus the shard's empty Lucene
 * commit; the rows and the Lance-side indexes stay in the Lance table.
 * Restore therefore re-creates the index pointing at the same
 * {@code index.lance.table}, and the outcome depends on whether that table
 * is still reachable.
 */
public class LanceSnapshotIT extends LanceRestTestCase {

    public void testRestoreOfNamespaceSurfacedIndexReadsCurrentTable() throws Exception {
        // Snapshot a namespace-surfaced index, advance the table, delete
        // the index, restore. The restored index follows the manifest, so
        // it reads the rows as they are at restore time, not as they were
        // when the snapshot was taken. The namespace poll must leave the
        // restored index alone rather than surface it a second time.
        String repo = "repo-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        String snap = "snap-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        try (LanceTestCluster f = LanceTestCluster.setUp(6, "snapns")) {
            String indexName = f.indexName();
            try {
                putFsRepository(repo);
                String created = snapshotIndex(repo, snap, indexName);
                assertEquals("snapshot state: " + created, "SUCCESS", extractPath(created, "snapshot", "state"));
                assertEquals("snapshot failed shards: " + created, 0, extractIntPath(created, "snapshot", "shards", "failed"));
                assertEquals("snapshot successful shards: " + created, 1, extractIntPath(created, "snapshot", "shards", "successful"));

                String listed = readAll(client().performRequest(new Request("GET", "/_snapshot/" + repo + "/" + snap)));
                assertTrue("snapshot must list the Lance index: " + listed, listed.contains("\"" + indexName + "\""));

                // Only the empty Lucene commit is copied: one segments_N
                // file of a few hundred bytes, no row data.
                String status = readAll(client().performRequest(new Request("GET", "/_snapshot/" + repo + "/" + snap + "/_status")));
                int fileCount = extractIntPath(status, "snapshots", "0", "indices", indexName, "stats", "total", "file_count");
                int sizeInBytes = extractIntPath(status, "snapshots", "0", "indices", indexName, "stats", "total", "size_in_bytes");
                assertEquals("snapshot must hold only the empty Lucene commit: " + status, 1, fileCount);
                assertTrue("snapshot size must be a few hundred bytes, saw " + sizeInBytes + ": " + status, sizeInBytes < 4096);

                // Advance the table after the snapshot: three of six rows
                // survive.
                LanceTableFactory.deleteRows(f.tableUri(), "id >= 3");

                client().performRequest(new Request("DELETE", "/" + indexName));
                ResponseException gone = expectThrows(
                    ResponseException.class,
                    () -> client().performRequest(new Request("GET", "/" + indexName))
                );
                assertEquals(404, gone.getResponse().getStatusLine().getStatusCode());

                String restored = restoreIndex(repo, snap, indexName, true);
                assertEquals("restore failed shards: " + restored, 0, extractIntPath(restored, "snapshot", "shards", "failed"));
                assertEquals("restore successful shards: " + restored, 1, extractIntPath(restored, "snapshot", "shards", "successful"));
                ensureGreen(indexName);

                String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
                assertEquals(
                    "restored index must point at the same table: " + settings,
                    f.tableUri(),
                    extractPath(settings, indexName, "settings", "index", "lance", "table")
                );
                String uuid = (String) extractPath(settings, indexName, "settings", "index", "uuid");

                // Unpinned index: restore reads the manifest as it is now.
                assertEquals(3, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));
                String search = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
                assertEquals(3, extractIntPath(search, "hits", "total", "value"));
                assertEquals(3, engineDocCount(indexName));

                // Three poll cycles (cadence 1s) must not recreate or
                // duplicate the restored index: the poll sees the name
                // taken and skips the table.
                Thread.sleep(3_500);
                String cat = readAll(client().performRequest(new Request("GET", "/_cat/indices?format=json")));
                assertEquals("restored index must appear exactly once: " + cat, 1, countOccurrences(cat, "\"" + indexName + "\""));
                String settingsAfter = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
                assertEquals(
                    "poll must not replace the restored index: " + settingsAfter,
                    uuid,
                    extractPath(settingsAfter, indexName, "settings", "index", "uuid")
                );
                assertEquals(3, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));

                // The poll skips the restored index because the delete
                // dropped it from the tracked set, so a later manifest
                // advance reaches _search (fragment path opens the latest
                // version per query) but not the shard engine behind
                // _count, _stats and GET until an explicit _refresh.
                LanceTableFactory.deleteRows(f.tableUri(), "id >= 2");
                Thread.sleep(3_500);
                String searchAfter = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
                assertEquals(2, extractIntPath(searchAfter, "hits", "total", "value"));
                assertEquals("engine reader of an untracked restored index must not advance", 3, engineDocCount(indexName));
                assertEquals(3, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));
                client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
                assertEquals(2, engineDocCount(indexName));
                assertEquals(2, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));
            } finally {
                try {
                    client().performRequest(new Request("DELETE", "/" + indexName));
                } catch (Exception ignored) {}
                try {
                    deleteJson("/_lance/namespace", "{\"path\":\"" + f.tableUri().substring(0, f.tableUri().lastIndexOf('/')) + "\"}");
                } catch (Exception ignored) {}
                dropFsRepository(repo);
            }
        }
    }

    public void testRestoreKeepsPinnedVersionAndStorageOptions() throws Exception {
        // An attached index pinned to manifest version 1 keeps its pin and
        // its storage_options through snapshot and restore, and the
        // restored index serves the pinned version even after the table
        // moved on.
        String suffix = "snappin-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String repo = "repo-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        String snap = "snap-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"version\":1,\"storage_options\":{\"aws_region\":\"us-east-1\"}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            putFsRepository(repo);
            String created = snapshotIndex(repo, snap, indexName);
            assertEquals("snapshot state: " + created, "SUCCESS", extractPath(created, "snapshot", "state"));

            LanceTableFactory.deleteRows(tableUri, "id >= 3");
            client().performRequest(new Request("DELETE", "/" + indexName));

            String restored = restoreIndex(repo, snap, indexName, true);
            assertEquals("restore failed shards: " + restored, 0, extractIntPath(restored, "snapshot", "shards", "failed"));
            ensureGreen(indexName);

            String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertEquals(tableUri, extractPath(settings, indexName, "settings", "index", "lance", "table"));
            assertEquals("1", extractPath(settings, indexName, "settings", "index", "lance", "version"));
            assertEquals("us-east-1", extractPath(settings, indexName, "settings", "index", "lance", "storage_options", "aws_region"));

            // Version 1 predates the row deletion. Both the shard engine
            // (_count, _stats) and the fragment path behind _search open
            // the pinned manifest, so every surface reports six rows once
            // the engine has opened.
            assertBusy(() -> {
                int count = extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count");
                int statsCount = engineDocCount(indexName);
                String observed = "_count=" + count + " _stats=" + statsCount;
                assertEquals(observed, 6, count);
                assertEquals(observed, 6, statsCount);
            }, 30, TimeUnit.SECONDS);
            String search = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals("pinned _search must read version 1: " + search, 6, extractIntPath(search, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            dropFsRepository(repo);
            deleteRecursively(scratchDir);
        }
    }

    public void testRestoreWithMissingTableLeavesShardUnassigned() throws Exception {
        // The snapshot has no rows to fall back on. When the table is
        // gone at restore time the shard cannot open its engine, the
        // allocation fails on every retry, and the index stays red until
        // it is deleted and restored again with the table in place.
        String suffix = "snapmiss-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        Path tablePath = scratchDir.resolve(tableName + ".lance");
        Path movedPath = scratchDir.resolve(tableName + ".moved");
        String indexName = tableName;
        String repo = "repo-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        String snap = "snap-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tablePath + "\"}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            putFsRepository(repo);
            String created = snapshotIndex(repo, snap, indexName);
            assertEquals("snapshot state: " + created, "SUCCESS", extractPath(created, "snapshot", "state"));

            client().performRequest(new Request("DELETE", "/" + indexName));
            Files.move(tablePath, movedPath, StandardCopyOption.ATOMIC_MOVE);

            // Two allocation attempts instead of the default five keep the
            // test short; the failure shape is the same on every retry.
            String accepted = restoreIndex(repo, snap, indexName, false, "{\"index.allocation.max_retries\":2}");
            assertEquals("restore must be accepted: " + accepted, Boolean.TRUE, extractPath(accepted, "accepted"));

            // The first allocation attempt fails while the engine opens
            // the table: Lance reports the dataset as not found.
            assertBusy(() -> {
                String explain = allocationExplain(indexName);
                assertEquals("allocation explain: " + explain, "ALLOCATION_FAILED", extractPath(explain, "unassigned_info", "reason"));
                String details = (String) extractPath(explain, "unassigned_info", "details");
                assertTrue("allocation explain must name the missing table: " + explain, details.contains("was not found"));
            }, 30, TimeUnit.SECONDS);

            // Retries keep failing until index.allocation.max_retries and
            // the shard is left unassigned, so the index stays red. The
            // retries after the first one fail on the shard lock: the
            // engine constructor threw after ReadOnlyEngine had taken its
            // store reference, so the store of the failed shard never
            // closes and never releases the lock.
            assertBusy(() -> {
                String health = readAll(client().performRequest(new Request("GET", "/_cluster/health/" + indexName)));
                assertEquals("index health: " + health, "red", extractPath(health, "status"));
                String shards = readAll(
                    client().performRequest(new Request("GET", "/_cat/shards/" + indexName + "?format=json&h=state,unassigned.reason"))
                );
                assertEquals("shard state: " + shards, "UNASSIGNED", extractPath(shards, "0", "state"));
                assertEquals("unassigned reason: " + shards, "ALLOCATION_FAILED", extractPath(shards, "0", "unassigned.reason"));
                String explain = allocationExplain(indexName);
                assertEquals("allocation explain: " + explain, 2, extractIntPath(explain, "unassigned_info", "failed_allocation_attempts"));
                assertEquals("allocation explain: " + explain, "no", extractPath(explain, "can_allocate"));
                assertTrue(
                    "later retries fail on the shard lock held by the failed shard: " + explain,
                    ((String) extractPath(explain, "unassigned_info", "details")).contains("ShardLockObtainFailedException")
                );
            }, 60, TimeUnit.SECONDS);

            // The index exists with its settings. _count (shard path) has
            // no shard to run on; _search (fragment path) opens the table
            // itself and fails on the missing dataset.
            String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertEquals(tablePath.toString(), extractPath(settings, indexName, "settings", "index", "lance", "table"));
            ResponseException count = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_count"))
            );
            ResponseException search = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}")
            );
            String observed = "_count="
                + count.getResponse().getStatusLine().getStatusCode()
                + " body="
                + readAll(count.getResponse())
                + " _search="
                + search.getResponse().getStatusLine().getStatusCode()
                + " body="
                + readAll(search.getResponse());
            assertEquals(observed, 503, count.getResponse().getStatusLine().getStatusCode());
            assertEquals(observed, 400, search.getResponse().getStatusLine().getStatusCode());

            // Putting the table back and retrying the failed allocation
            // does not recover the index: once the restore has failed, the
            // restore_in_progress decider refuses to allocate the primary
            // again and asks for the index to be deleted and restored anew.
            Files.move(movedPath, tablePath, StandardCopyOption.ATOMIC_MOVE);
            Response reroute = postJson("/_cluster/reroute?retry_failed=true", "{}");
            assertEquals(RestStatus.OK.getStatus(), reroute.getStatusLine().getStatusCode());
            assertBusy(() -> {
                String explain = allocationExplain(indexName);
                assertEquals("allocation explain after retry_failed: " + explain, "no", extractPath(explain, "can_allocate"));
                assertTrue(
                    "restore_in_progress decider must refuse the retry: " + explain,
                    explain.contains("shard has failed to be restored from the snapshot")
                );
            }, 30, TimeUnit.SECONDS);
            String healthAfterRetry = readAll(client().performRequest(new Request("GET", "/_cluster/health/" + indexName)));
            assertEquals("index health after retry_failed: " + healthAfterRetry, "red", extractPath(healthAfterRetry, "status"));

            // Delete and restore again with the table in place.
            client().performRequest(new Request("DELETE", "/" + indexName));
            String restored = restoreIndex(repo, snap, indexName, true);
            assertEquals("second restore failed shards: " + restored, 0, extractIntPath(restored, "snapshot", "shards", "failed"));
            ensureGreen(indexName);
            assertEquals(6, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            dropFsRepository(repo);
            deleteRecursively(scratchDir);
        }
    }

    private static void putFsRepository(String repo) throws IOException {
        Path location = Files.createDirectories(repoRoot().resolve(repo));
        Request request = new Request("PUT", "/_snapshot/" + repo);
        request.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + location + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(
            "repository register failed: " + readAll(response),
            RestStatus.OK.getStatus(),
            response.getStatusLine().getStatusCode()
        );
    }

    private static void dropFsRepository(String repo) {
        try {
            client().performRequest(new Request("DELETE", "/_snapshot/" + repo));
        } catch (Exception ignored) {}
        try {
            deleteRecursively(repoRoot().resolve(repo));
        } catch (Exception ignored) {}
    }

    private static String snapshotIndex(String repo, String snap, String indexName) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repo + "/" + snap + "?wait_for_completion=true");
        request.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        Response response = client().performRequest(request);
        String body = readAll(response);
        assertEquals("snapshot create failed: " + body, RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return body;
    }

    private static String restoreIndex(String repo, String snap, String indexName, boolean waitForCompletion) throws IOException {
        return restoreIndex(repo, snap, indexName, waitForCompletion, null);
    }

    private static String restoreIndex(String repo, String snap, String indexName, boolean waitForCompletion, String indexSettingsJson)
        throws IOException {
        Request request = new Request("POST", "/_snapshot/" + repo + "/" + snap + "/_restore?wait_for_completion=" + waitForCompletion);
        String indexSettings = indexSettingsJson == null ? "" : ",\"index_settings\":" + indexSettingsJson;
        request.setJsonEntity("{\"indices\":\"" + indexName + "\"" + indexSettings + "}");
        Response response = client().performRequest(request);
        String body = readAll(response);
        // Both forms answer 200; without wait_for_completion the body is
        // {"accepted":true}.
        assertEquals("restore failed: " + body, RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return body;
    }

    private static String allocationExplain(String indexName) throws IOException {
        return readAll(postJson("/_cluster/allocation/explain", "{\"index\":\"" + indexName + "\",\"shard\":0,\"primary\":true}"));
    }

    /**
     * Row count as the shard engine's reader sees it ({@code _stats}
     * docs.count comes from {@code Engine#docStats}). {@code _count} reads
     * the same reader through the shard path because its
     * {@code track_total_hits} flag keeps it off the fragment path;
     * {@code _search} opens the table anew per query.
     */
    private static int engineDocCount(String indexName) throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/docs")));
        return extractIntPath(stats, "indices", indexName, "primaries", "docs", "count");
    }

    /**
     * Pluck an arbitrary value out of a JSON document. Steps are object keys
     * or array indices, applied in order.
     */
    private static Object extractPath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = json.trim().startsWith("[") ? parser.list() : parser.map();
            for (String step : path) {
                if (value instanceof Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            return value;
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }
}
