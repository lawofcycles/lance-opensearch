/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests of the {@link LanceLocalClones} lifecycle: the marker round
 * trip, clone reuse and re-creation, the forward-only guard of a
 * non-exact {@code ensure}, and directory removal.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceLocalClonesTests extends OpenSearchTestCase {

    public void testMarkerRoundTrip() throws Exception {
        LanceLocalClones clones = new LanceLocalClones(createTempDir(), null, null);
        Files.createDirectories(clones.rootDir().resolve("uuid-1"));
        LanceLocalClones.Marker written = new LanceLocalClones.Marker("/data/table.lance", 7L, "/clones/uuid-1/v7/table.lance");
        clones.writeMarker("uuid-1", written);
        assertEquals(written, clones.readMarker("uuid-1"));
        // current() requires the clone directory to exist.
        assertEquals(Optional.empty(), clones.current("uuid-1"));
    }

    public void testEnsureReusesRecreatesAndKeepsNewer() throws Exception {
        Path scratch = createTempDir();
        String sourceUri = LanceTableFactory.writeMultiFragmentTable(scratch, "clones-src", 12, 4);
        long v1;
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
        ) {
            v1 = source.version();
        }
        LanceLocalClones clones = new LanceLocalClones(createTempDir(), null, null);

        LanceLocalClones.CloneLocation first = clones.ensure("uuid-a", sourceUri, StorageOptions.empty(), v1, false);
        assertTrue("first contact must create the clone", first.recreated());
        assertTrue(Files.isDirectory(Path.of(first.uri())));
        assertEquals(Optional.of(new LanceLocalClones.Marker(sourceUri, v1, first.uri())), clones.current("uuid-a"));

        LanceLocalClones.CloneLocation again = clones.ensure("uuid-a", sourceUri, StorageOptions.empty(), v1, false);
        assertFalse("a matching clone is reused", again.recreated());
        assertEquals(first.uri(), again.uri());

        // Source advances; ensure at the new version re-creates and drops
        // the old versioned directory.
        LanceTableFactory.appendRows(sourceUri, 100, 4);
        long v2;
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
        ) {
            v2 = source.version();
        }
        LanceLocalClones.CloneLocation advanced = clones.ensure("uuid-a", sourceUri, StorageOptions.empty(), v2, false);
        assertTrue(advanced.recreated());
        assertFalse("the stale versioned directory is dropped", Files.exists(Path.of(first.uri())));

        // A racing request for the older version must not tear the newer
        // clone down when ensure is non-exact.
        LanceLocalClones.CloneLocation stale = clones.ensure("uuid-a", sourceUri, StorageOptions.empty(), v1, false);
        assertFalse(stale.recreated());
        assertEquals(advanced.uri(), stale.uri());

        // An exact ensure at the older version (the engine following a tag
        // that moved backwards) does re-clone.
        LanceLocalClones.CloneLocation backward = clones.ensure("uuid-a", sourceUri, StorageOptions.empty(), v1, true);
        assertTrue(backward.recreated());
        assertEquals(Optional.of(v1), clones.current("uuid-a").map(LanceLocalClones.Marker::sourceVersion));

        clones.delete("uuid-a");
        assertEquals(Optional.empty(), clones.current("uuid-a"));
        assertFalse(Files.exists(clones.rootDir().resolve("uuid-a")));
    }

    public void testTableLocationFallsBackToSourceWithoutClone() throws Exception {
        Settings nodeLocalSettings = indexSettingsBuilder("placement-loc").put(
            LanceEngineFactory.INDEX_PLACEMENT_SETTING,
            LanceLocalClones.PLACEMENT_NODE_LOCAL
        ).build();
        assertTrue(LanceLocalClones.isNodeLocal(nodeLocalSettings));
        assertFalse(LanceLocalClones.isNodeLocal(indexSettingsBuilder("placement-loc").build()));

        LanceLocalClones clones = new LanceLocalClones(createTempDir(), null, null);
        LanceLocalClones.setInstance(clones);
        try {
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("placement-loc").settings(nodeLocalSettings).numberOfShards(1).numberOfReplicas(0).build(),
                Settings.EMPTY
            );
            // No clone on this node yet: the source comes back, flagged
            // node-local so a caller that can create the clone knows to.
            LanceTableLocation before = LanceTableLocation.forNode(indexSettings);
            assertEquals("/data/demo.lance", before.uri());
            assertTrue(before.nodeLocal());

            Path scratch = createTempDir();
            String sourceUri = LanceTableFactory.writeMultiFragmentTable(scratch, "loc-src", 8, 4);
            long version;
            try (
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset source = Dataset.open().allocator(allocator).uri(sourceUri).build()
            ) {
                version = source.version();
            }
            clones.ensure("placement-loc", sourceUri, StorageOptions.empty(), version, false);
            LanceTableLocation after = LanceTableLocation.forNode(indexSettings);
            assertTrue(after.nodeLocal());
            assertNotEquals("/data/demo.lance", after.uri());
            assertTrue(after.uri(), after.uri().contains("lance-local"));
            assertTrue(after.storageOptions().asMap() == null || after.storageOptions().asMap().isEmpty());

            // in_table placement always resolves to the source.
            IndexSettings inTable = new IndexSettings(
                IndexMetadata.builder("placement-loc")
                    .settings(indexSettingsBuilder("placement-loc").build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                Settings.EMPTY
            );
            LanceTableLocation source = LanceTableLocation.forNode(inTable);
            assertEquals("/data/demo.lance", source.uri());
            assertFalse(source.nodeLocal());
        } finally {
            LanceLocalClones.setInstance(null);
        }
    }

    private static Settings.Builder indexSettingsBuilder(String indexName) {
        return Settings.builder()
            .put(LanceEngineFactory.TABLE_SETTING, "/data/demo.lance")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexName + "-uuid");
    }
}
