/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.io.PathUtils;

/**
 * {@code index.lance.index_placement = node_local}: attach a table whose
 * directory the OpenSearch process cannot write to, build FTS indexes
 * into per-node shallow clones, query them, and verify the source
 * directory stays untouched, while the same build with the default
 * {@code in_table} placement fails with Lance's permission error (the
 * motivating behaviour). Also covers the follow-forward path: an append
 * to a writable source re-creates the clone at the new version.
 */
public class LanceIndexPlacementIT extends LanceRestTestCase {

    public void testNodeLocalBuildOnReadOnlySource() throws Exception {
        Path scratch = Files.createDirectories(sharedRoot().resolve("lance-it-placement-" + randomSuffix()));
        String indexName = "placement-ro-" + randomSuffix();
        String tableUri = LanceTableFactory.writeKeywordOnlyTable(scratch, indexName, 6);
        Path tableDir = Path.of(tableUri);
        long manifestsBefore = countManifests(tableDir);
        makeReadOnly(tableDir);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"name\":\"" + indexName + "\",\"index_placement\":\"node_local\"}"
            );
            assertEquals(200, attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);

            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"label\"]}"));
            assertTrue("expected label in the merged fts built list: " + build, build.contains("\"fts\":[\"label\"]"));
            assertTrue("expected a per-node nodes block: " + build, build.contains("\"nodes\":{"));

            // The keyword -> lance_text flip lands through the rebuild the
            // build action performs; wait for the mapping to show it.
            assertBusy(() -> {
                String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
                assertTrue("label must map to lance_text after the build: " + mapping, mapping.contains("\"type\":\"lance_text\""));
            });
            ensureGreen(indexName);

            assertBusy(() -> {
                String hits = readAll(
                    postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"label\",\"query\":\"3\"}}}")
                );
                assertEquals("lance_match must hit row 3 through the clone: " + hits, 1, extractIntPath(hits, "hits", "total", "value"));
            });

            // The source took no commit: no new manifest, no _indices
            // directory.
            assertEquals("the source must gain no manifest", manifestsBefore, countManifests(tableDir));
            assertFalse("the source must have no _indices entry", Files.exists(tableDir.resolve("_indices")));

            // The clone shows up in the stats with its size and the
            // source version it was cloned at.
            String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            assertTrue("stats must report the clone under local_clones: " + stats, stats.contains("\"" + indexName + "\""));
            assertTrue("stats must report local_clone_bytes: " + stats, stats.contains("\"local_clone_bytes\""));

            // Deleting the index removes the clone directory.
            client().performRequest(new Request("DELETE", "/" + indexName));
            assertBusy(() -> {
                String after = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
                int idx = after.indexOf("\"local_clones\"");
                assertTrue("local_clones block expected: " + after, idx >= 0);
                assertFalse(
                    "the clone entry must disappear after the index is deleted: " + after,
                    after.substring(idx).contains("\"" + indexName + "\"")
                );
            });
        } finally {
            makeWritable(tableDir);
            deleteRecursively(scratch);
        }
    }

    public void testInTableBuildOnReadOnlySourceFails() throws Exception {
        // Pins the motivating behaviour: the same read-only table with the
        // default placement reports the build as failed with Lance's
        // permission error.
        Path scratch = Files.createDirectories(sharedRoot().resolve("lance-it-placement-" + randomSuffix()));
        String indexName = "placement-intable-" + randomSuffix();
        String tableUri = LanceTableFactory.writeKeywordOnlyTable(scratch, indexName, 6);
        Path tableDir = Path.of(tableUri);
        makeReadOnly(tableDir);
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"name\":\"" + indexName + "\"}");
            assertEquals(200, attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"label\"]}")
            );
            String body = readAll(failure.getResponse());
            assertEquals(
                "expected 500 for a build on a read-only source: " + body,
                500,
                failure.getResponse().getStatusLine().getStatusCode()
            );
            assertTrue("expected label under failed.fts: " + body, body.contains("\"failed\""));
            client().performRequest(new Request("DELETE", "/" + indexName));
        } finally {
            makeWritable(tableDir);
            deleteRecursively(scratch);
        }
    }

    public void testNodeLocalFollowsSourceAdvance() throws Exception {
        Path scratch = Files.createDirectories(sharedRoot().resolve("lance-it-placement-" + randomSuffix()));
        String indexName = "placement-advance-" + randomSuffix();
        String tableUri = LanceTableFactory.writeMultiFragmentTable(scratch, indexName, 8, 4);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"name\":\"" + indexName + "\",\"index_placement\":\"node_local\"}"
            );
            assertEquals(200, attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);
            long versionBefore = statsCloneVersion(indexName);
            assertTrue("the clone must record a source version, saw " + versionBefore, versionBefore > 0);

            LanceTableFactory.appendRows(tableUri, 100, 4);
            // The poll notices the advance, the engine re-clones, and the
            // new rows become searchable through the clone.
            assertBusy(() -> {
                String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":{\"match_all\":{}}}"));
                assertEquals("the appended rows must surface: " + count, 12, extractIntPath(count, "count"));
                long versionAfter = statsCloneVersion(indexName);
                assertTrue(
                    "the clone marker must advance with the source, before " + versionBefore + " after " + versionAfter,
                    versionAfter > versionBefore
                );
            });
            client().performRequest(new Request("DELETE", "/" + indexName));
        } finally {
            deleteRecursively(scratch);
        }
    }

    private long statsCloneVersion(String indexName) throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
        // The stats body nests local_clones per node; find the entry for
        // this index and read its source_version.
        int idx = stats.indexOf("\"" + indexName + "\":{\"local_clone_bytes\"");
        if (idx < 0) {
            return -1;
        }
        int versionKey = stats.indexOf("\"source_version\":", idx);
        int end = versionKey + "\"source_version\":".length();
        int stop = end;
        while (stop < stats.length() && (Character.isDigit(stats.charAt(stop)) || stats.charAt(stop) == '-')) {
            stop++;
        }
        return Long.parseLong(stats.substring(end, stop));
    }

    private static String randomSuffix() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    private static long countManifests(Path datasetDir) throws IOException {
        Path versions = datasetDir.resolve("_versions");
        if (!Files.isDirectory(versions)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(versions)) {
            return files.count();
        }
    }

    private static void makeReadOnly(Path dir) throws IOException {
        assumeTrue(
            "POSIX permissions are required to make the source read-only",
            PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix")
        );
        Set<PosixFilePermission> dirPerms = PosixFilePermissions.fromString("r-xr-xr-x");
        Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("r--r--r--");
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
                Files.setPosixFilePermissions(path, Files.isDirectory(path) ? dirPerms : filePerms);
            }
        }
    }

    private static void makeWritable(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Set<PosixFilePermission> dirPerms = PosixFilePermissions.fromString("rwxr-xr-x");
        Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("rw-r--r--");
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted((a, b) -> a.getNameCount() - b.getNameCount()).toList()) {
                Files.setPosixFilePermissions(path, Files.isDirectory(path) ? dirPerms : filePerms);
            }
        }
    }
}
