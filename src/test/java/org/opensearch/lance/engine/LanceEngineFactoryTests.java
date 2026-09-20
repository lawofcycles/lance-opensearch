/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.file.Path;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.lance.LancePlugin;
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
        assertEquals("index.lance.table", LanceEngineFactory.TABLE_SETTING);
    }

    public void testPrimaryKeyFieldSettingKey() {
        assertEquals("index.lance.primary_key_field", LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING);
    }

    public void testPrimaryKeyTypeSettingKey() {
        assertEquals("index.lance.primary_key_type", LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING);
    }

    public void testMultiFieldsSettingKey() {
        assertEquals("index.lance.multi_fields", LanceEngineFactory.MULTI_FIELDS_SETTING);
    }

    public void testMultiFieldsSerialiseDeserialiseRoundTrip() {
        // Empty map round-trips to empty string and back to empty map so
        // absence of a multi_fields clause never persists a setting.
        assertEquals("", org.opensearch.lance.rest.RestAttachAction.serialiseMultiFields(java.util.Collections.emptyMap()));
        assertTrue(org.opensearch.lance.rest.RestAttachAction.deserialiseMultiFields("").isEmpty());
        assertTrue(org.opensearch.lance.rest.RestAttachAction.deserialiseMultiFields(null).isEmpty());

        // Nested map with one keyword sub-field survives the JSON round
        // trip so the engine sees exactly what attach persisted.
        java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> in = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> bodySubs = new java.util.LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        in.put("body", bodySubs);
        String json = org.opensearch.lance.rest.RestAttachAction.serialiseMultiFields(in);
        assertEquals("{\"body\":{\"raw\":\"keyword\"}}", json);

        java.util.Map<String, java.util.LinkedHashMap<String, String>> out = org.opensearch.lance.rest.RestAttachAction
            .deserialiseMultiFields(json);
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
        IndexSettings lanceSettings = IndexSettingsModule.newIndexSettings("lance", settings, LancePlugin.TABLE_SETTING);
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
