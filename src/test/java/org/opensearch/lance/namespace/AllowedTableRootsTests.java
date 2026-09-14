/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

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
}
