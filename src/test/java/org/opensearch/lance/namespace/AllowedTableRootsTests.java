/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

public class AllowedTableRootsTests extends OpenSearchTestCase {

    public void testEmptyAllowlistAcceptsEverything() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of());
        assertTrue(roots.isEmpty());
        assertTrue(roots.allows("/data/lance/demo.lance"));
        assertTrue(roots.allows("s3://bucket/prefix/table.lance"));
        assertTrue(roots.allows("/etc/passwd"));
    }

    public void testNullAllowlistAcceptsEverything() {
        // The setting default arrives as an empty list, but code paths that
        // construct AllowedTableRoots directly should also survive null.
        AllowedTableRoots roots = new AllowedTableRoots(null);
        assertTrue(roots.isEmpty());
        assertTrue(roots.allows("/anything"));
    }

    public void testSinglePathRootMatchesChildren() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        assertFalse(roots.isEmpty());
        assertTrue(roots.allows("/data/lance/demo.lance"));
        assertTrue(roots.allows("/data/lance/sub/dir/demo.lance"));
    }

    public void testSinglePathRootRejectsSiblingPrefix() {
        // Sibling directories that share the same textual prefix must not
        // slip through: `/data/lance` must not match `/data/lance-old`.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        assertFalse(roots.allows("/data/lance-old/demo.lance"));
    }

    public void testRootWithTrailingSlashBehavesLikeWithout() {
        AllowedTableRoots a = new AllowedTableRoots(List.of("/data/lance"));
        AllowedTableRoots b = new AllowedTableRoots(List.of("/data/lance/"));
        for (String candidate : List.of("/data/lance/demo.lance", "/data/lance-old", "/data/other/x.lance")) {
            assertEquals("mismatch for " + candidate, a.allows(candidate), b.allows(candidate));
        }
    }

    public void testS3UriRoot() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/prefix"));
        assertTrue(roots.allows("s3://bucket/prefix/table.lance"));
        assertFalse(roots.allows("s3://bucket/other/table.lance"));
        assertFalse(roots.allows("s3://other-bucket/prefix/table.lance"));
    }

    public void testMultipleRootsBehaveAsUnion() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance", "s3://bucket/prefix"));
        assertTrue(roots.allows("/data/lance/demo.lance"));
        assertTrue(roots.allows("s3://bucket/prefix/table.lance"));
        assertFalse(roots.allows("/tmp/demo.lance"));
    }

    public void testNullOrEmptyCandidateIsRejected() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        assertFalse(roots.allows(null));
        assertFalse(roots.allows(""));
    }

    public void testEmptyOrBlankRootsAreDropped() {
        // Configuration accidents (empty entries in a list setting) must not
        // silently allow every path by turning into a "" root.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("", "/data/lance"));
        assertFalse(roots.isEmpty());
        assertTrue(roots.allows("/data/lance/demo.lance"));
        assertFalse(roots.allows("/tmp/demo.lance"));
    }

    public void testDotDotSegmentInLocalPathIsRejected() {
        // The string starts with the root but names a file outside it.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance/"));
        assertFalse(roots.allows("/data/lance/../etc/passwd"));
        assertFalse(roots.allows("/data/lance/sub/../../etc/passwd"));
    }

    public void testDotSegmentInLocalPathIsAccepted() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        assertTrue(roots.allows("/data/lance/./t.lance"));
        assertTrue(roots.allows("/data/lance/sub/../t.lance"));
    }

    public void testCandidateEqualToRootIsAccepted() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        assertTrue(roots.allows("/data/lance/"));
        assertTrue(roots.allows("/data/lance"));
    }

    public void testMissingLocalPathUnderRootIsAccepted() throws IOException {
        // Attach may name a table that is still being written; only the
        // ancestors that exist take part in the symlink resolution.
        Path root = createTempDir();
        AllowedTableRoots roots = new AllowedTableRoots(List.of(root.toString()));
        assertTrue(roots.allows(root.resolve("new.lance").toString()));
        assertTrue(roots.allows(root.resolve("missing-dir").resolve("new.lance").toString()));
        assertFalse(roots.allows(root.resolveSibling("elsewhere").resolve("new.lance").toString()));
    }

    public void testRootGivenWithoutSchemeAcceptsItsOwnRealPath() throws IOException {
        // On platforms where the temp directory is itself reached through
        // a symlink (macOS /var -> /private/var) the root and the
        // candidate canonicalise to the same real path.
        Path root = createTempDir();
        AllowedTableRoots roots = new AllowedTableRoots(List.of(root.toString()));
        assertTrue(roots.allows(root.toRealPath().resolve("t.lance").toString()));
    }

    public void testSymlinkUnderRootPointingOutsideIsRejected() throws IOException {
        Path base = createTempDir();
        Path root = Files.createDirectories(base.resolve("lance"));
        Path outside = Files.createDirectories(base.resolve("outside"));
        Path link = root.resolve("escape");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            assumeTrue("symlinks cannot be created here: " + e, false);
        }
        AllowedTableRoots roots = new AllowedTableRoots(List.of(root.toString()));
        assertFalse(roots.allows(link.toString()));
        assertFalse(roots.allows(link.resolve("t.lance").toString()));
        assertFalse(roots.allows(link.resolve("not-yet-written").resolve("t.lance").toString()));
        assertTrue(roots.allows(root.resolve("t.lance").toString()));
    }

    public void testSymlinkUnderRootPointingInsideIsAccepted() throws IOException {
        Path base = createTempDir();
        Path root = Files.createDirectories(base.resolve("lance"));
        Path target = Files.createDirectories(root.resolve("real"));
        Path link = root.resolve("alias");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            assumeTrue("symlinks cannot be created here: " + e, false);
        }
        AllowedTableRoots roots = new AllowedTableRoots(List.of(root.toString()));
        assertTrue(roots.allows(link.resolve("t.lance").toString()));
    }

    public void testUriSchemeCaseIsFolded() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/prefix/"));
        assertTrue(roots.allows("S3://bucket/prefix/table"));
        assertTrue(roots.allows("s3://BUCKET/prefix/table"));
        assertTrue(roots.allows("s3://bucket/prefix/t"));
        // The key itself keeps its case: object store keys are case sensitive.
        assertFalse(roots.allows("s3://bucket/PREFIX/table"));
    }

    public void testPercentEncodedDotDotInUriIsRejected() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/prefix/"));
        assertFalse(roots.allows("s3://bucket/prefix/%2e%2e/other"));
        assertFalse(roots.allows("s3://bucket/prefix/../other"));
        assertFalse(roots.allows("s3://bucket/prefix/%2E%2E/other"));
        assertFalse(roots.allows("s3://bucket/prefix/sub%2F..%2F..%2Fother"));
    }

    public void testDotDotClimbingAboveTheBucketIsRejected() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/"));
        assertFalse(roots.allows("s3://bucket/../other/table"));
        assertTrue(roots.allows("s3://bucket/sub/../table"));
    }

    public void testPercentEncodedSegmentUnderRootIsAccepted() {
        // A percent encoded character that does not spell a dot segment is
        // decoded and compared as the key the store will see, so the root
        // and the candidate may spell the same key with different escapes.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/pre~fix"));
        assertTrue(roots.allows("s3://bucket/pre%7Efix/table"));
        assertTrue(roots.allows("s3://bucket/pre~fix/table"));
    }

    public void testUnparseableUriIsRejected() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/prefix/"));
        assertFalse(roots.allows("s3://bucket/prefix/a b"));
        assertFalse(roots.allows("s3://bucket/prefix/%zz"));
    }

    public void testUriWithUserInfoOrPortIsRejected() {
        // The bucket is the only authority a table URI names; a user or a
        // port would be folded into the compared key by a plain lower case.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("s3://bucket/prefix/"));
        assertFalse(roots.allows("s3://someone@bucket/prefix/table"));
        assertFalse(roots.allows("s3://bucket:9000/prefix/table"));
        assertTrue(roots.allows("s3://BUCKET/prefix/table"));
    }

    public void testRootWithDotDotAboveTheBucketFailsConstruction() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AllowedTableRoots(List.of("s3://bucket/../other"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("s3://bucket/../other"));
    }

    public void testCanonicalRootsEndWithSlash() {
        AllowedTableRoots roots = new AllowedTableRoots(List.of("S3://Bucket/prefix", "s3://bucket"));
        assertEquals(List.of("s3://bucket/prefix/", "s3://bucket/"), roots.configuredRoots());
    }
}
