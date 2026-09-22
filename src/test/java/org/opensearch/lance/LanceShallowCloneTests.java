/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.Ref;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pins the three Lance {@code shallowClone} facts the {@code node_local}
 * placement relies on, so a lance-core upgrade that changes any of them
 * fails here rather than in an integration test:
 *
 * <ol>
 *   <li>A shallow clone of a source directory this process cannot write
 *       to succeeds (the clone writes only at the destination).</li>
 *   <li>{@code createIndex} on the clone commits to the clone's manifest
 *       chain only: the source gains no manifest and no {@code _indices}
 *       entry.</li>
 *   <li>The clone still serves its cloned version after the source
 *       advanced (append-only advances leave the referenced data files
 *       in place).</li>
 * </ol>
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceShallowCloneTests extends OpenSearchTestCase {

    public void testShallowCloneFactsOnReadOnlySource() throws Exception {
        assumeTrue(
            "POSIX permissions are required to make the source read-only",
            PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix")
        );
        Path scratch = createTempDir();
        String sourceUri = LanceTableFactory.writeMultiFragmentTable(scratch, "clone-src", 12, 4);
        Path sourceDir = Path.of(sourceUri);
        long sourceManifestsBefore = countManifests(sourceDir);
        long sourceVersion;
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
        ) {
            sourceVersion = source.version();
        }

        makeReadOnly(sourceDir);
        try {
            Path cloneDir = createTempDir().resolve("clone.lance");
            // Fact 1: cloning from a read-only source succeeds.
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
            ) {
                source.shallowClone(cloneDir.toString(), Ref.ofMain(sourceVersion), Map.of()).close();
            }

            // Fact 2: createIndex commits to the clone only. The fixture's
            // `id` column carries no index in the source, so a BTREE build
            // in the clone is a fresh commit.
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset clone = Dataset.open().allocator(allocator).uri(cloneDir.toString()).build()
            ) {
                assertEquals("the clone starts at the source's version number", sourceVersion, clone.version());
                clone.createIndex(
                    IndexOptions.builder(
                        Collections.singletonList("id"),
                        IndexType.BTREE,
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build()
                    ).withIndexName("id_btree").build()
                );
            }
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset clone = Dataset.open().allocator(allocator).uri(cloneDir.toString()).build()
            ) {
                assertTrue("the clone must carry the index built into it", clone.version() > sourceVersion);
                assertFalse(
                    "describeIndices on the clone must list the id index",
                    clone.describeIndices(new IndexCriteria.Builder().forColumn("id").build()).isEmpty()
                );
            }
            assertEquals("the source must gain no manifest from the clone's build", sourceManifestsBefore, countManifests(sourceDir));
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
            ) {
                assertTrue(
                    "the source must not see the index committed into the clone",
                    source.describeIndices(new IndexCriteria.Builder().forColumn("id").build()).isEmpty()
                );
            }

            // Fact 3: after the source advances a version, the clone still
            // serves its cloned rows.
            makeWritable(sourceDir);
            LanceTableFactory.appendRows(sourceUri, 100, 4);
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
            ) {
                assertTrue("the append must advance the source", source.version() > sourceVersion);
            }
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset clone = Dataset.open().allocator(allocator).uri(cloneDir.toString()).build()
            ) {
                assertEquals("the clone keeps serving the cloned rows after the source advanced", 12, clone.countRows());
            }
        } finally {
            // Leave the temp tree writable so the framework can delete it.
            makeWritable(sourceDir);
        }
    }

    private static long countManifests(Path datasetDir) throws Exception {
        Path versions = datasetDir.resolve("_versions");
        if (!Files.isDirectory(versions)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(versions)) {
            return files.count();
        }
    }

    private static void makeReadOnly(Path dir) throws Exception {
        Set<PosixFilePermission> dirPerms = PosixFilePermissions.fromString("r-xr-xr-x");
        Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("r--r--r--");
        try (Stream<Path> walk = Files.walk(dir)) {
            // Children first so the parent stays traversable while the
            // permissions are applied.
            for (Path path : walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
                Files.setPosixFilePermissions(path, Files.isDirectory(path) ? dirPerms : filePerms);
            }
        }
    }

    private static void makeWritable(Path dir) throws Exception {
        Set<PosixFilePermission> dirPerms = PosixFilePermissions.fromString("rwxr-xr-x");
        Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("rw-r--r--");
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path path : walk.sorted((a, b) -> a.getNameCount() - b.getNameCount()).toList()) {
                Files.setPosixFilePermissions(path, Files.isDirectory(path) ? dirPerms : filePerms);
            }
        }
    }
}
