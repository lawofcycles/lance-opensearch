/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.lance.Dataset;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceIndexWarmer.Mode;
import org.opensearch.lance.engine.LanceIndexWarmer.State;
import org.opensearch.lance.engine.LanceIndexWarmer.TableStatus;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceIndexWarmer} against a local table carrying one index of
 * every kind it knows: the scans it issues load without error under both
 * modes, the status it publishes names every index, and {@code none}
 * records a skipped table without touching it.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceIndexWarmerTests extends OpenSearchTestCase {

    private RootAllocator allocator;
    private LanceWarmCache cache;
    private ExecutorService executor;
    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeIndexedFixtureTable(scratchDir, "warmer-" + getTestName(), 2, 150);
        allocator = new RootAllocator(Long.MAX_VALUE);
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        if (cache != null) {
            cache.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    private IndexMetadata indexMetadata(String name) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
            .put(LanceEngineFactory.TABLE_SETTING, uri)
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }

    private static TableStatus awaitFinished(LanceIndexWarmer warmer, String index) throws Exception {
        assertBusy(() -> {
            Optional<TableStatus> status = warmer.status(index);
            assertTrue(status.isPresent());
            State state = status.get().state();
            assertTrue("still " + state, state != State.PENDING && state != State.RUNNING);
        }, 60, java.util.concurrent.TimeUnit.SECONDS);
        return warmer.status(index).get();
    }

    public void testMetadataModeWarmsEveryIndex() throws Exception {
        LanceIndexWarmer warmer = new LanceIndexWarmer(cache, executor, Mode.METADATA);
        warmer.schedule(indexMetadata("warm-metadata"));
        TableStatus status = awaitFinished(warmer, "warm-metadata");
        assertEquals(status.toString(), State.DONE, status.state());
        assertEquals(uri, status.table());
        assertTrue(status.version() >= 1L);
        assertTrue(status.startedAtMillis() > 0L);
        Map<String, LanceIndexWarmer.IndexStatus> byName = new HashMap<>();
        for (LanceIndexWarmer.IndexStatus index : status.indexes()) {
            byName.put(index.name(), index);
        }
        assertEquals(byName.toString(), 4, byName.size());
        assertEquals(State.DONE, byName.get("rating_btree").state());
        assertEquals("rating", byName.get("rating_btree").column());
        assertEquals(State.DONE, byName.get("category_bitmap").state());
        assertEquals(State.DONE, byName.get("body_fts").state());
        assertEquals(State.DONE, byName.get("embedding_ivf").state());
        // The warm-up leased the snapshot the requests will use and gave
        // it back: it stays cached, unreferenced.
        assertEquals(1, cache.snapshotCount());
        assertEquals(1L, cache.snapshotBuildCount());
        warmer.close();
    }

    public void testAllModeReadsEveryPage() throws Exception {
        LanceIndexWarmer warmer = new LanceIndexWarmer(cache, executor, Mode.ALL);
        warmer.schedule(indexMetadata("warm-all"));
        TableStatus status = awaitFinished(warmer, "warm-all");
        assertEquals(status.toString(), State.DONE, status.state());
        assertEquals(Mode.ALL, status.mode());
        for (LanceIndexWarmer.IndexStatus index : status.indexes()) {
            assertEquals(index.toString(), State.DONE, index.state());
        }
        warmer.close();
    }

    public void testNoneModeRecordsASkippedTableAndOpensNothing() throws Exception {
        LanceIndexWarmer warmer = new LanceIndexWarmer(cache, executor, Mode.NONE);
        warmer.schedule(indexMetadata("warm-none"));
        TableStatus status = awaitFinished(warmer, "warm-none");
        assertEquals(State.SKIPPED, status.state());
        assertTrue(status.indexes().isEmpty());
        assertEquals(0L, cache.datasetOpenCount());
        warmer.close();
    }

    public void testMissingTableFails() throws Exception {
        LanceIndexWarmer warmer = new LanceIndexWarmer(cache, executor, Mode.METADATA);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, "missing-uuid")
            .put(LanceEngineFactory.TABLE_SETTING, createTempDir().resolve("missing.lance").toString())
            .build();
        warmer.schedule(IndexMetadata.builder("warm-missing").settings(settings).build());
        TableStatus status = awaitFinished(warmer, "warm-missing");
        assertEquals(State.FAILED, status.state());
        assertEquals(1, status.indexes().size());
        assertEquals(State.FAILED, status.indexes().get(0).state());
        assertTrue(status.indexes().get(0).detail(), status.indexes().get(0).detail().startsWith("could not open the table"));
        warmer.close();
    }

    public void testWarmScanShapesPerIndexType() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Map<String, LanceField> byName = new HashMap<>();
            for (LanceField field : dataset.getLanceSchema().fields()) {
                byName.put(field.getName(), field);
            }
            List<IndexDescription> descriptions = dataset.describeIndices();
            assertEquals(4, descriptions.size());
            for (IndexDescription description : descriptions) {
                LanceField field = null;
                for (LanceField candidate : byName.values()) {
                    if (candidate.getId() == description.getFieldIds().get(0)) {
                        field = candidate;
                    }
                }
                assertNotNull(description.getName(), field);
                assertNotNull(description.getIndexType(), LanceIndexWarmer.warmScan(field, description.getIndexType(), Mode.METADATA));
                assertNotNull(description.getIndexType(), LanceIndexWarmer.warmScan(field, description.getIndexType(), Mode.ALL));
            }
            // A type without a warm-up scan yields none.
            assertNull(LanceIndexWarmer.warmScan(byName.get("rating"), "ZoneMap", Mode.METADATA));
            // A vector index over a non float32 column has no probe vector.
            assertNull(LanceIndexWarmer.warmScan(byName.get("rating"), "IVF_PQ", Mode.METADATA));
        }
    }

    public void testMinimumLiteralCoversTheIndexedTypes() {
        assertEquals("-2147483648", LanceIndexWarmer.minimumLiteral(new ArrowType.Int(32, true)));
        assertEquals("-9223372036854775807", LanceIndexWarmer.minimumLiteral(new ArrowType.Int(64, true)));
        assertEquals("0", LanceIndexWarmer.minimumLiteral(new ArrowType.Int(32, false)));
        assertEquals("''", LanceIndexWarmer.minimumLiteral(new ArrowType.Utf8()));
        assertEquals("false", LanceIndexWarmer.minimumLiteral(new ArrowType.Bool()));
        assertEquals(
            "arrow_cast(-9223372036854775807, 'Timestamp(Microsecond, None)')",
            LanceIndexWarmer.minimumLiteral(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))
        );
        assertEquals(
            "arrow_cast(-9223372036854775807, 'Timestamp(Millisecond, Some(\"UTC\"))')",
            LanceIndexWarmer.minimumLiteral(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))
        );
        assertNull(LanceIndexWarmer.minimumLiteral(new ArrowType.Binary()));
    }
}
