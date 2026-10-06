/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.lance.Dataset;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.MappingDerivation;
import org.opensearch.lance.engine.LanceWarmCache.Lease;
import org.opensearch.lance.engine.LanceWarmCache.Snapshot;
import org.opensearch.lance.engine.LanceWarmCache.SnapshotKey;
import org.opensearch.test.IndexSettingsModule;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Unit-level assertions for {@link LanceEngineFactory}: the setting keys
 * that {@code LancePlugin.getSettings()} and the README both refer to, so a
 * rename cannot slip through unnoticed, and the engine constructor's
 * failure path, which must hand back everything {@code ReadOnlyEngine}
 * acquired before the Lance table turned out to be unreachable.
 *
 * <p>{@link EngineTestCase} supplies the shard id, thread pool, translog
 * plumbing and the {@link EngineConfig} builder; the store below is the
 * same shape {@code IndexShard} hands to the engine factory (empty Lucene
 * commit with sequence-number user data and a translog UUID).
 *
 * <p>Lance keeps a native thread pool alive per JVM, which trips the
 * default thread-leak scanner; the same suppression is applied in every
 * test class that touches Lance.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceEngineFactoryTests extends EngineTestCase {

    public void testTableSettingKey() {
        assertEquals("index.plugins.lance.table", LanceEngineFactory.TABLE_SETTING);
    }

    public void testPrimaryKeyFieldSettingKey() {
        assertEquals("index.plugins.lance.primary_key_field", LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING);
    }

    public void testPrimaryKeyTypeSettingKey() {
        assertEquals("index.plugins.lance.primary_key_type", LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING);
    }

    public void testMultiFieldsSettingKey() {
        assertEquals("index.plugins.lance.multi_fields", LanceEngineFactory.MULTI_FIELDS_SETTING);
    }

    public void testMultiFieldsSerialiseDeserialiseRoundTrip() {
        // Empty map round-trips to empty string and back to empty map so
        // absence of a multi_fields clause never persists a setting.
        assertEquals("", MappingDerivation.serialiseMultiFields(java.util.Collections.emptyMap()));
        assertTrue(MappingDerivation.deserialiseMultiFields("").isEmpty());
        assertTrue(MappingDerivation.deserialiseMultiFields(null).isEmpty());

        // Nested map with one keyword sub-field survives the JSON round
        // trip so the engine sees exactly what attach persisted.
        java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> in = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> bodySubs = new java.util.LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        in.put("body", bodySubs);
        String json = MappingDerivation.serialiseMultiFields(in);
        assertEquals("{\"body\":{\"raw\":\"keyword\"}}", json);

        java.util.Map<String, java.util.LinkedHashMap<String, String>> out = MappingDerivation.deserialiseMultiFields(json);
        assertEquals(1, out.size());
        assertEquals("keyword", out.get("body").get("raw"));
    }

    public void testPrimaryKeyTypeFromSettingFallsBackToLong() {
        // Empty and unknown strings must return LONG so indices created
        // before the setting existed continue to open with the integer
        // lookup path. Known values map to their enum. NONE is only ever set at
        // runtime when the field name is empty, but the enum still round
        // trips through fromSetting so callers that persist "none"
        // (e.g. a future migration tool) see the same value on read.
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting(""));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting(null));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting("gibberish"));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting("long"));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.KEYWORD, LanceEngineFactory.LancePrimaryKeyType.fromSetting("keyword"));
        assertEquals(
            LanceEngineFactory.LancePrimaryKeyType.UNSIGNED_LONG,
            LanceEngineFactory.LancePrimaryKeyType.fromSetting("unsigned_long")
        );
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.NONE, LanceEngineFactory.LancePrimaryKeyType.fromSetting("none"));
    }

    public void testEngineOpenFailureReleasesStoreAndWriteLock() throws Exception {
        // A table path that does not exist makes Lance's Dataset.open throw
        // inside the engine constructor, after ReadOnlyEngine has already
        // taken store.incRef(), the IndexWriter write lock and a reader on
        // the empty commit. Those must be handed back so the shard's store
        // can close (and release the node-level ShardLock) and a retry on
        // the same store reports the Lance error again rather than a lock
        // conflict.
        Path missingTable = createTempDir().resolve("missing.lance");
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(LanceEngineFactory.TABLE_SETTING, missingTable.toString())
            .build();
        IndexSettings lanceSettings = IndexSettingsModule.newIndexSettings("lance", settings, LanceSettings.TABLE_SETTING);
        Path translogPath = createTempDir("translog-lance");

        try (Store lanceStore = createStore(lanceSettings, newDirectory())) {
            lanceStore.createEmpty(Version.CURRENT.luceneVersion);
            String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            lanceStore.associateIndexWithNewTranslog(translogUuid);
            EngineConfig config = config(lanceSettings, lanceStore, translogPath, newMergePolicy(), null);
            LanceEngineFactory factory = new LanceEngineFactory();

            int refCountBefore = lanceStore.refCount();

            RuntimeException first = expectThrows(RuntimeException.class, () -> factory.newReadWriteEngine(config));
            assertLanceNotFound("first open", first);
            assertEquals("store reference taken by ReadOnlyEngine must be released", refCountBefore, lanceStore.refCount());

            // The write lock is free again: obtaining it here would fail
            // with LockObtainFailedException if the failed engine still
            // held it.
            try (Lock lock = lanceStore.directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                assertNotNull(lock);
            }

            // Second attempt on the same store: the same Lance failure,
            // not a lock conflict left behind by the first attempt.
            RuntimeException second = expectThrows(RuntimeException.class, () -> factory.newReadWriteEngine(config));
            assertLanceNotFound("second open", second);
            assertEquals(refCountBefore, lanceStore.refCount());
        }
    }

    public void testEngineReaderIsAViewOverTheWarmCacheSnapshot() throws Exception {
        // The engine's whole-table reader leases the same (index uuid,
        // version) snapshot the fragment path acquires, so a node holds one
        // dataset per table version; the lease follows the reader through a
        // refresh swap and is given back when the engine closes.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "engine-" + getTestName(), 2, 100);
        String indexUuid = "engine-cache-uuid";
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put(LanceEngineFactory.TABLE_SETTING, uri)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, "id")
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, "long")
            .build();
        IndexSettings lanceSettings = IndexSettingsModule.newIndexSettings(
            "lance",
            settings,
            LanceSettings.TABLE_SETTING,
            LanceSettings.PRIMARY_KEY_FIELD_SETTING,
            LanceSettings.PRIMARY_KEY_TYPE_SETTING
        );
        Path translogPath = createTempDir("translog-lance-cache");

        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
            Store lanceStore = createStore(lanceSettings, newDirectory())
        ) {
            lanceStore.createEmpty(Version.CURRENT.luceneVersion);
            String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            lanceStore.associateIndexWithNewTranslog(translogUuid);
            EngineConfig config = config(lanceSettings, lanceStore, translogPath, newMergePolicy(), null);

            Snapshot initial;
            try (Engine engine = new LanceEngineFactory(cache).newReadWriteEngine(config)) {
                assertEquals("the engine open built one snapshot", 1L, cache.snapshotBuildCount());
                assertEquals(1, cache.snapshotCount());
                long version;
                try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
                    version = dataset.version();
                }
                initial = cache.snapshot(new SnapshotKey(indexUuid, version));
                assertNotNull(initial);
                assertEquals("the engine's reader holds the lease", 1, initial.refCount());

                // A fragment path request for the same version finds the
                // engine's snapshot: no second open, no second schema pass.
                try (
                    Lease fragmentPath = cache.acquire(
                        indexUuid,
                        uri,
                        StorageOptions.empty(),
                        Optional.of(version),
                        "id",
                        LanceEngineFactory.LancePrimaryKeyType.LONG,
                        LanceOverrides.EMPTY
                    )
                ) {
                    assertSame(initial, fragmentPath.snapshot());
                    assertEquals(1L, cache.snapshotBuildCount());
                    assertEquals(1L, cache.datasetOpenCount());
                    assertEquals(1L, cache.snapshotHitCount());
                    assertEquals(2, initial.refCount());
                }
                assertEquals(1, initial.refCount());

                // GET reads through the snapshot; docStats reads the same
                // version's manifest and agrees with the snapshot's total.
                DocsStats docStats = engine.docStats();
                assertEquals(200, docStats.getCount());
                assertEquals(0, docStats.getDeleted());
                assertEquals(initial.dataFileSizes().knownBytes(), docStats.getTotalSizeInBytes());
                try (
                    Engine.GetResult hit = engine.get(new Engine.Get(true, true, "150", new Term("_id", "150")), engine::acquireSearcher)
                ) {
                    assertTrue("GET must resolve id 150 through the snapshot's dataset", hit.exists());
                }
                try (
                    Engine.GetResult miss = engine.get(new Engine.Get(true, true, "9999", new Term("_id", "9999")), engine::acquireSearcher)
                ) {
                    assertFalse(miss.exists());
                }

                // The table advances: refresh leases the new version and
                // the old reader, gone with the last searcher, gives its
                // lease back. Retire stays the namespace poll's job, so
                // the old snapshot is idle in the cache, not closed.
                try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
                    dataset.delete("id = 150");
                }
                engine.refresh("test");
                assertEquals(2L, cache.snapshotBuildCount());
                assertEquals(2, cache.snapshotCount());
                assertEquals("the swapped out reader released the old snapshot", 0, initial.refCount());
                assertFalse(initial.isRetired());
                assertFalse(initial.isClosed());
                assertEquals(199, engine.docStats().getCount());
                assertEquals(1, engine.docStats().getDeleted());
                try (
                    Engine.GetResult deleted = engine.get(
                        new Engine.Get(true, true, "150", new Term("_id", "150")),
                        engine::acquireSearcher
                    )
                ) {
                    assertFalse("the deleted row is gone from the new version", deleted.exists());
                }
                // Refreshing at the same version swaps nothing.
                engine.refresh("test-again");
                assertEquals(2L, cache.snapshotBuildCount());
            }
            for (Snapshot snapshot : List.of(initial)) {
                assertEquals("the closed engine holds no lease", 0, snapshot.refCount());
            }
            assertEquals("closing the engine retires nothing; the snapshots stay for the next reader", 2, cache.snapshotCount());
            assertEquals(0, cache.retiredSnapshotCount());
        }
    }

    public void testEngineWithoutACacheOpensItsOwnDataset() throws Exception {
        // The no-cache path stays as it was: the reader owns a dataset and
        // reports no snapshot version.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "engine-nocache-" + getTestName(), 1, 50);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(LanceEngineFactory.TABLE_SETTING, uri)
            .build();
        IndexSettings lanceSettings = IndexSettingsModule.newIndexSettings("lance", settings, LanceSettings.TABLE_SETTING);
        Path translogPath = createTempDir("translog-lance-nocache");
        try (Store lanceStore = createStore(lanceSettings, newDirectory())) {
            lanceStore.createEmpty(Version.CURRENT.luceneVersion);
            String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            lanceStore.associateIndexWithNewTranslog(translogUuid);
            EngineConfig config = config(lanceSettings, lanceStore, translogPath, newMergePolicy(), null);
            try (Engine engine = new LanceEngineFactory().newReadWriteEngine(config)) {
                assertEquals(50, engine.docStats().getCount());
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    assertEquals(-1L, LanceDirectoryReader.snapshotVersionOf(searcher.getIndexReader()));
                }
            }
        }
    }

    public void testEveryReadSeesTheTableVersionAndKeepsTheReaderWhenItDidNotMove() throws Exception {
        // GET refreshes the reader before it reads: a row committed to
        // the table is answered by the next GET with no refresh call, and
        // a read at an unchanged version opens nothing (the snapshot build
        // count and the dataset open count of the cache stand still).
        // docStats reads the manifest of the followed version and touches
        // the reader not at all. A pinned engine never refreshes.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStringPkTable(scratchDir, "engine-fresh-" + getTestName(), 4, 2);
        long attached;
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            attached = dataset.version();
        }
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true)
        ) {
            Settings following = stringPkSettings(uri, "engine-fresh-uuid", -1L);
            IndexSettings followingSettings = stringPkIndexSettings(following);
            Path translogPath = createTempDir("translog-lance-fresh");
            try (Store lanceStore = createStore(followingSettings, newDirectory())) {
                lanceStore.createEmpty(Version.CURRENT.luceneVersion);
                String translogUuid = Translog.createEmptyTranslog(
                    translogPath,
                    SequenceNumbers.NO_OPS_PERFORMED,
                    shardId,
                    primaryTerm.get()
                );
                lanceStore.associateIndexWithNewTranslog(translogUuid);
                EngineConfig config = config(followingSettings, lanceStore, translogPath, newMergePolicy(), null);
                try (Engine engine = new LanceEngineFactory(cache).newReadWriteEngine(config)) {
                    assertEquals(1L, cache.snapshotBuildCount());
                    long opensAfterOpen = cache.datasetOpenCount();
                    assertEquals(4, engine.docStats().getCount());
                    try (
                        Engine.GetResult hit = engine.get(
                            new Engine.Get(true, true, "alpha-3", new Term("_id", "alpha-3")),
                            engine::acquireSearcher
                        )
                    ) {
                        assertTrue(hit.exists());
                    }
                    try (
                        Engine.GetResult miss = engine.get(
                            new Engine.Get(true, true, "alpha-5", new Term("_id", "alpha-5")),
                            engine::acquireSearcher
                        )
                    ) {
                        assertFalse(miss.exists());
                    }
                    assertEquals("reads at an unchanged version swap no reader", 1L, cache.snapshotBuildCount());
                    assertEquals("reads at an unchanged version build no snapshot", opensAfterOpen, cache.datasetOpenCount());
                    assertEquals(attached, ((LanceEngineFactory.LanceReadOnlyEngine) engine).servedVersion());

                    // alpha-4 and alpha-5 are committed: docStats counts six
                    // from the manifest while the reader still serves the
                    // attached version, then the next GET answers alpha-5
                    // and swaps the reader, no refresh call.
                    LanceTableFactory.appendStringPkRows(uri, 4, 2);
                    assertEquals(6, engine.docStats().getCount());
                    assertEquals("docStats acquires no searcher and swaps no reader", 1L, cache.snapshotBuildCount());
                    assertEquals(attached, ((LanceEngineFactory.LanceReadOnlyEngine) engine).servedVersion());
                    try (
                        Engine.GetResult hit = engine.get(
                            new Engine.Get(true, true, "alpha-5", new Term("_id", "alpha-5")),
                            engine::acquireSearcher
                        )
                    ) {
                        assertTrue("the committed row is read on the next GET", hit.exists());
                    }
                    assertEquals("the GET swapped the reader to the new version", 2L, cache.snapshotBuildCount());
                    assertEquals(attached + 1, ((LanceEngineFactory.LanceReadOnlyEngine) engine).servedVersion());
                    assertEquals(6, engine.docStats().getCount());
                    assertEquals(2L, cache.snapshotBuildCount());
                    assertEquals(
                        "a following engine reads the manifest on every call",
                        3L,
                        ((LanceEngineFactory.LanceReadOnlyEngine) engine).docStatsManifestReads()
                    );
                }
            }

            // Pinned to the version attached at: the appended rows stay out
            // and nothing is opened per read.
            Settings pinned = stringPkSettings(uri, "engine-pinned-uuid", attached);
            IndexSettings pinnedSettings = stringPkIndexSettings(pinned);
            Path pinnedTranslog = createTempDir("translog-lance-pinned");
            try (Store lanceStore = createStore(pinnedSettings, newDirectory())) {
                lanceStore.createEmpty(Version.CURRENT.luceneVersion);
                String translogUuid = Translog.createEmptyTranslog(
                    pinnedTranslog,
                    SequenceNumbers.NO_OPS_PERFORMED,
                    shardId,
                    primaryTerm.get()
                );
                lanceStore.associateIndexWithNewTranslog(translogUuid);
                EngineConfig config = config(pinnedSettings, lanceStore, pinnedTranslog, newMergePolicy(), null);
                try (Engine engine = new LanceEngineFactory(cache).newReadWriteEngine(config)) {
                    LanceEngineFactory.LanceReadOnlyEngine lanceEngine = (LanceEngineFactory.LanceReadOnlyEngine) engine;
                    long builds = cache.snapshotBuildCount();
                    long opens = cache.datasetOpenCount();
                    assertEquals(0L, lanceEngine.docStatsManifestReads());

                    // Concurrent first callers: one reads the manifest, the
                    // others wait for its values.
                    int callers = 4;
                    CountDownLatch start = new CountDownLatch(1);
                    List<Thread> threads = new ArrayList<>(callers);
                    List<Exception> failures = new CopyOnWriteArrayList<>();
                    List<DocsStats> answers = new CopyOnWriteArrayList<>();
                    for (int i = 0; i < callers; i++) {
                        Thread thread = new Thread(() -> {
                            try {
                                start.await();
                                answers.add(engine.docStats());
                            } catch (Exception e) {
                                failures.add(e);
                            }
                        }, getTestName() + "-docStats-" + i);
                        thread.start();
                        threads.add(thread);
                    }
                    start.countDown();
                    for (Thread thread : threads) {
                        thread.join(30_000L);
                    }
                    assertTrue(failures.toString(), failures.isEmpty());
                    assertEquals(callers, answers.size());
                    for (DocsStats answer : answers) {
                        assertEquals(4, answer.getCount());
                    }
                    assertEquals("one caller read the manifest, the rest waited for it", 1L, lanceEngine.docStatsManifestReads());

                    assertEquals(4, engine.docStats().getCount());
                    assertEquals(4, engine.docStats().getCount());
                    assertEquals("a pinned engine reads the manifest once", 1L, lanceEngine.docStatsManifestReads());
                    try (
                        Engine.GetResult miss = engine.get(
                            new Engine.Get(true, true, "alpha-5", new Term("_id", "alpha-5")),
                            engine::acquireSearcher
                        )
                    ) {
                        assertFalse("a pinned engine does not see the appended row", miss.exists());
                    }
                    assertEquals(builds, cache.snapshotBuildCount());
                    assertEquals(opens, cache.datasetOpenCount());
                    assertEquals(attached, lanceEngine.servedVersion());
                }
            }
        }
    }

    private static Settings stringPkSettings(String uri, String indexUuid, long pinnedVersion) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put(LanceEngineFactory.TABLE_SETTING, uri)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, "key")
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, "keyword");
        if (pinnedVersion >= 0) {
            settings.put(LanceEngineFactory.VERSION_SETTING, pinnedVersion);
        }
        return settings.build();
    }

    private static IndexSettings stringPkIndexSettings(Settings settings) {
        return IndexSettingsModule.newIndexSettings(
            "lance",
            settings,
            LanceSettings.TABLE_SETTING,
            LanceSettings.PRIMARY_KEY_FIELD_SETTING,
            LanceSettings.PRIMARY_KEY_TYPE_SETTING,
            LanceSettings.VERSION_SETTING
        );
    }

    private static void assertLanceNotFound(String attempt, RuntimeException e) {
        String chain = describe(e);
        assertFalse(
            attempt + " must not fail on the IndexWriter lock: " + chain,
            chain.contains(LockObtainFailedException.class.getSimpleName())
        );
        assertTrue(attempt + " must surface Lance's not-found error: " + chain, chain.contains("was not found"));
        assertFalse(attempt + " must not be wrapped in an EngineException: " + chain, e instanceof EngineException);
    }

    private static String describe(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append(" <- ");
        }
        return sb.toString();
    }
}
