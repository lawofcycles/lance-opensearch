/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.lance.Dataset;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceServedVersions;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for {@link LanceIndexFreshnessService}: the check runs
 * against real Lance tables, with the shard replaced by a stand-in that
 * records refreshes and answers the mapping comparison from the mapping
 * it holds, and the client replaced by one that records the requests
 * the check sends.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceIndexFreshnessServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private RecordingClient client;
    private LanceIndexFreshnessService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = new RecordingClient(threadPool);
        // A one hour cadence keeps the scheduled check from firing during
        // a test; the tests drive check() themselves.
        service = new LanceIndexFreshnessService(client, threadPool, TimeValue.timeValueHours(1), null, new LanceServedVersions());
    }

    @Override
    public void tearDown() throws Exception {
        service.close();
        client.close();
        ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testMoveWithoutSchemaChangeRefreshesAndSendsNoMappingUpdate() throws Exception {
        String tableUri = writeTable("nochange");
        FakeShard shard = FakeShard.overTable("nochange", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        assertNotNull(entry);

        // The first check derives even without a move, and finds the
        // mapping the index was created with: nothing to send.
        LanceIndexFreshnessService.Outcome first = service.check(entry);
        assertFalse(first.moved());
        assertFalse(first.mappingChanged());
        assertEquals(0, shard.refreshes.get());
        assertEquals(0, client.count(PutMappingAction.NAME));

        long before = shard.servedVersion();
        LanceTableFactory.appendRows(tableUri, 6, 4);
        LanceIndexFreshnessService.Outcome second = service.check(entry);
        assertTrue("an append is a move", second.moved());
        assertEquals(before, second.servedVersion());
        assertTrue(second.targetVersion() > before);
        assertFalse("same schema, same mapping", second.mappingChanged());
        assertEquals("the shard is refreshed once", 1, shard.refreshes.get());
        assertEquals("the reader now serves the new version", second.targetVersion(), shard.servedVersion());
        assertEquals("no PutMapping for an unchanged mapping", 0, client.count(PutMappingAction.NAME));

        var stats = service.stats();
        assertEquals(1, stats.tracked());
        assertEquals(2, stats.checks());
        assertEquals(1, stats.moves());
        assertEquals(0, stats.mappingUpdates());
        assertEquals(2, stats.mappingUnchanged());
        assertEquals(0, stats.rebuilds());
        assertEquals(0, stats.failures());
        assertTrue(stats.lastCheckMillis() > 0L);
    }

    public void testMoveWithANewColumnSendsOnePutMapping() throws Exception {
        String tableUri = writeTable("newcol");
        FakeShard shard = FakeShard.overTable("newcol", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);
        assertEquals(0, client.count(PutMappingAction.NAME));

        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome outcome = service.check(entry);
        assertTrue(outcome.moved());
        assertTrue("a new column changes the mapping", outcome.mappingChanged());
        assertEquals(1, client.count(PutMappingAction.NAME));
        PutMappingRequest sent = (PutMappingRequest) client.requests(PutMappingAction.NAME).get(0);
        assertTrue("the update carries the new column: " + sent.source(), sent.source().contains("\"score\""));
        assertEquals(1, shard.refreshes.get());
        assertEquals(1, service.stats().mappingUpdates());
        assertEquals(1, service.stats().mappingUnchanged());

        // The next check sees the same schema again: no second update.
        LanceTableFactory.appendRows(tableUri, 6, 2);
        LanceIndexFreshnessService.Outcome again = service.check(entry);
        assertTrue(again.moved());
        assertFalse(again.mappingChanged());
        assertEquals(1, client.count(PutMappingAction.NAME));
    }

    public void testTagMovingBackwardsIsAMove() throws Exception {
        String tableUri = writeTable("tagback");
        long versionA = currentVersion(tableUri);
        LanceTableFactory.appendRows(tableUri, 6, 4);
        long versionB = currentVersion(tableUri);
        assertTrue(versionB > versionA);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.tags().create("release", versionA);
        }
        // The shard serves the latest version while the index follows a
        // tag that points at the older one.
        FakeShard shard = FakeShard.overTable(
            "tagback",
            tableUri,
            Settings.builder().put(LanceEngineFactory.TAG_SETTING, "release").build()
        );
        assertEquals(versionB, shard.servedVersion());
        LanceIndexFreshnessService.Tracked entry = service.track(shard);

        LanceIndexFreshnessService.Outcome outcome = service.check(entry);
        assertTrue("a tag behind the served version is a move", outcome.moved());
        assertEquals(versionB, outcome.servedVersion());
        assertEquals(versionA, outcome.targetVersion());
        assertEquals(1, shard.refreshes.get());
        assertEquals(versionA, shard.servedVersion());

        // Moving the tag forward again is a move too; standing still is not.
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.tags().update("release", versionB);
        }
        assertTrue(service.check(entry).moved());
        assertFalse(service.check(entry).moved());
        assertEquals(2, shard.refreshes.get());
    }

    public void testPinnedIndexIsNeverTracked() throws Exception {
        String tableUri = writeTable("pinned");
        FakeShard shard = FakeShard.overTable(
            "pinned",
            tableUri,
            Settings.builder().put(LanceEngineFactory.VERSION_SETTING, currentVersion(tableUri)).build()
        );
        assertNull("a pinned index never advances, so it is not checked", service.track(shard));
        assertFalse(service.isTracked("pinned"));
        assertEquals(0, service.stats().tracked());
        // Not Lance backed at all: not tracked either.
        assertNull(service.track(FakeShard.overTable("plain", null, Settings.EMPTY)));
    }

    public void testShardCloseStopsTheTask() throws Exception {
        String tableUri = writeTable("closing");
        FakeShard shard = FakeShard.overTable("closing", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        assertTrue(service.isTracked("closing"));
        assertFalse("the scheduled check is live while the shard is open", entry.isCancelled());

        service.beforeIndexShardClosed(new ShardId(new Index("closing", shard.indexUuid()), 0), null, Settings.EMPTY);
        assertFalse(service.isTracked("closing"));
        assertTrue("closing the shard cancels its check", entry.isCancelled());
        assertNull(service.trackedEntry("closing"));
    }

    public void testTypeChangeRebuildsTheIndex() throws Exception {
        String tableUri = writeTable("rebuild");
        // The shard's settings view carries the node's secure settings
        // (the keystore seed) next to the index settings; the recreate
        // must copy the index.lance.* keys and nothing else.
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString("keystore.seed", "seed");
        Settings settings = Settings.builder()
            .put("index.lance.uncovered_fragment_policy", "immediate")
            .put("index.number_of_shards", 1)
            .put("index.refresh_interval", "5s")
            .setSecureSettings(secure)
            .build();
        FakeShard shard = FakeShard.overTable("rebuild", tableUri, settings);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);
        assertEquals(0, client.count(DeleteIndexAction.NAME));

        // The preflight merge refuses the derived mapping: a keyword to
        // lance_text flip. The check deletes and re-creates the index.
        shard.typeConflict = true;
        LanceTableFactory.appendRows(tableUri, 6, 1);
        LanceIndexFreshnessService.Outcome outcome = service.check(entry);
        assertTrue(outcome.moved());
        assertTrue(outcome.rebuilt());
        assertTrue(outcome.mappingChanged());
        assertEquals(1, client.count(DeleteIndexAction.NAME));
        assertEquals(1, client.count(CreateIndexAction.NAME));
        assertEquals("no PutMapping is attempted for a type conflict", 0, client.count(PutMappingAction.NAME));
        assertEquals("a rebuilt index is not refreshed by this check; its new shard starts fresh", 0, shard.refreshes.get());
        CreateIndexRequest create = (CreateIndexRequest) client.requests(CreateIndexAction.NAME).get(0);
        assertEquals("rebuild", create.index());
        assertEquals(tableUri, create.settings().get(LanceEngineFactory.TABLE_SETTING));
        assertEquals("1", create.settings().get("index.number_of_shards"));
        assertEquals(
            "the previous index's lance settings are carried",
            "immediate",
            create.settings().get("index.lance.uncovered_fragment_policy")
        );
        assertFalse(
            "no secure setting travels with the create: " + create.settings().keySet(),
            create.settings().keySet().contains("keystore.seed")
        );
        assertFalse(
            "only index.lance.* keys are carried: " + create.settings().keySet(),
            create.settings().hasValue("index.refresh_interval")
        );
        assertTrue("the new mapping is the derived one", create.mappings().contains("\"properties\""));
        assertEquals(1, service.stats().rebuilds());
        assertEquals(0, service.stats().failures());
    }

    public void testFailedRebuildIsAFailureNotARebuild() throws Exception {
        String tableUri = writeTable("rebuildfail");
        FakeShard shard = FakeShard.overTable("rebuildfail", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);

        // The delete the rebuild starts with is refused: the check throws
        // (the manual sync answers with the error), counts a failure, and
        // the index is neither recreated nor refreshed.
        shard.typeConflict = true;
        client.failing = DeleteIndexAction.NAME;
        LanceTableFactory.appendRows(tableUri, 6, 1);
        IllegalStateException failure = expectThrows(IllegalStateException.class, () -> service.check(entry));
        assertTrue(failure.getMessage(), failure.getMessage().contains("rebuild of rebuildfail"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("delete refused"));
        assertEquals(1, client.count(DeleteIndexAction.NAME));
        assertEquals(0, client.count(CreateIndexAction.NAME));
        assertEquals(0, shard.refreshes.get());
        assertEquals("the attempt is counted", 1, service.stats().rebuilds());
        assertEquals("and so is the failure", 1, service.stats().failures());
    }

    public void testRefusedMappingUpdateLeavesMappingUnchangedAndReportsTheError() throws Exception {
        String tableUri = writeTable("refused");
        FakeShard shard = FakeShard.overTable("refused", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);
        assertTrue(service.stats().mappingErrors().isEmpty());

        // The cluster manager refuses the PutMapping for a reason other
        // than a type change: no rebuild, the index keeps its mapping,
        // so the outcome reports no mapping change and carries the
        // refusal, which the stats show too.
        client.failing = PutMappingAction.NAME;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome refused = service.check(entry);
        assertTrue(refused.moved());
        assertFalse("a refused update did not change the mapping", refused.mappingChanged());
        assertFalse(refused.rebuilt());
        assertNotNull("the outcome carries the refusal", refused.mappingError());
        assertTrue(refused.mappingError(), refused.mappingError().contains("refused"));
        assertEquals(1, client.count(PutMappingAction.NAME));
        assertEquals(0, client.count(DeleteIndexAction.NAME));
        assertEquals("the reader still advances", 1, shard.refreshes.get());
        assertEquals(Map.of("refused", refused.mappingError()), service.stats().mappingErrors());
        assertEquals(1, service.stats().failures());
        assertEquals("a refused update is not counted as applied", 0, service.stats().mappingUpdates());

        // The next move derives again and finds the mapping it derived
        // already in place (nothing to send): the refusal is cleared.
        client.failing = null;
        LanceTableFactory.appendRows(tableUri, 6, 2);
        LanceIndexFreshnessService.Outcome cleared = service.check(entry);
        assertTrue(cleared.moved());
        assertFalse(cleared.mappingChanged());
        assertNull(cleared.mappingError());
        assertTrue(service.stats().mappingErrors().isEmpty());

        // An acknowledged update reports the change and no error.
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("rank", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome applied = service.check(entry);
        assertTrue(applied.mappingChanged());
        assertNull(applied.mappingError());
        assertEquals(1, service.stats().mappingUpdates());

        // Untracking the index (its shard left this node) drops the entry too.
        client.failing = PutMappingAction.NAME;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("grade", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        assertNotNull(service.check(entry).mappingError());
        assertEquals(1, service.stats().mappingErrors().size());
        service.untrack("refused");
        assertTrue(service.stats().mappingErrors().isEmpty());
    }

    public void testRefusedMappingUpdateStaysUntilAnUpdateIsAcknowledged() throws Exception {
        String tableUri = writeTable("ackclears");
        FakeShard shard = FakeShard.overTable("ackclears", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);

        client.failing = PutMappingAction.NAME;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome refused = service.check(entry);
        assertNotNull(refused.mappingError());
        assertEquals(Map.of("ackclears", refused.mappingError()), service.stats().mappingErrors());

        // The table stands still: the next checks derive nothing (the
        // mapping was derived once already) and have nothing to apply.
        // The mapping the index should have is still the refused one,
        // so the refusal stays for as many checks as it takes.
        for (int i = 0; i < 3; i++) {
            LanceIndexFreshnessService.Outcome idle = service.check(entry);
            assertFalse(idle.moved());
            assertFalse(idle.mappingChanged());
            assertNull(idle.mappingError());
            assertEquals(
                "the refusal survives an idle check",
                Map.of("ackclears", refused.mappingError()),
                service.stats().mappingErrors()
            );
        }
        assertEquals("no second PutMapping was sent", 1, client.count(PutMappingAction.NAME));

        // An acknowledged update for the index clears it.
        client.failing = null;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("rank", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome applied = service.check(entry);
        assertTrue(applied.moved());
        assertTrue(applied.mappingChanged());
        assertNull(applied.mappingError());
        assertEquals(2, client.count(PutMappingAction.NAME));
        assertTrue(service.stats().mappingErrors().isEmpty());
    }

    public void testRefusedMappingUpdateStaysUntilALaterVersionAppliesCleanly() throws Exception {
        String tableUri = writeTable("laterclears");
        FakeShard shard = FakeShard.overTable("laterclears", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        service.check(entry);

        client.failing = PutMappingAction.NAME;
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            dataset.addColumns(List.of(new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        }
        LanceIndexFreshnessService.Outcome refused = service.check(entry);
        assertNotNull(refused.mappingError());
        long refusedVersion = refused.targetVersion();
        assertEquals(1, service.stats().mappingErrors().size());

        // Idle checks at the refused version keep the entry.
        LanceIndexFreshnessService.Outcome idle = service.check(entry);
        assertFalse(idle.moved());
        assertEquals(refusedVersion, idle.targetVersion());
        assertEquals(1, service.stats().mappingErrors().size());

        // A later version whose derived mapping is already in place (the
        // shard stand-in took the mapping when it compared it, the way an
        // operator's own PutMapping would put it there) supersedes the
        // refused derivation: nothing to send, and the refusal is gone.
        client.failing = null;
        LanceTableFactory.appendRows(tableUri, 6, 2);
        LanceIndexFreshnessService.Outcome later = service.check(entry);
        assertTrue(later.moved());
        assertTrue(later.targetVersion() > refusedVersion);
        assertFalse(later.mappingChanged());
        assertNull(later.mappingError());
        assertEquals("no second PutMapping was sent", 1, client.count(PutMappingAction.NAME));
        assertTrue(service.stats().mappingErrors().isEmpty());
    }

    public void testDriftDetectionThatCannotReadTheMappingCountsAsAFailure() throws Exception {
        // The drift detector reads the mapping's field meta on every
        // derivation. When that read fails the check still completes
        // (the reader advances, the derived mapping is compared), but
        // the drift of that version goes undetected, which the stats
        // count as a failure of the check.
        String tableUri = writeTable("driftread");
        FakeShard shard = FakeShard.overTable("driftread", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        client.failing = GetMappingsAction.NAME;

        LanceIndexFreshnessService.Outcome outcome = service.check(entry);
        assertTrue("the check itself completes", outcome.checked());
        assertFalse(outcome.mappingChanged());
        assertEquals(1, client.count(GetMappingsAction.NAME));
        assertEquals("the unreadable mapping is a failure", 1, service.stats().failures());

        // A check that derives nothing does not run the detector, so no
        // further failure; the next derivation runs it again.
        client.failing = null;
        service.check(entry);
        assertEquals(1, service.stats().failures());
        LanceTableFactory.appendRows(tableUri, 6, 2);
        service.check(entry);
        assertEquals(2, client.count(GetMappingsAction.NAME));
        assertEquals(1, service.stats().failures());
    }

    public void testDriftDetectionThatCannotMarkADroppedFieldCountsAsAFailure() throws Exception {
        // The mapping names a field whose Lance field id the table no
        // longer has: a drop. The detector sends one PutMapping to mark
        // it lance_dropped; when the cluster manager refuses it, the
        // stale name stays unmarked and the stats count the failure.
        String tableUri = writeTable("driftmark");
        FakeShard shard = FakeShard.overTable("driftmark", tableUri, Settings.EMPTY);
        LanceIndexFreshnessService.Tracked entry = service.track(shard);
        client.mappingSource = Map.of(
            "properties",
            Map.of("gone", Map.of("type", "long", "meta", Map.of("lance_field_id", "999", "lance_arrow_type", "Int(64, true)")))
        );
        client.failing = PutMappingAction.NAME;

        LanceIndexFreshnessService.Outcome outcome = service.check(entry);
        assertTrue(outcome.checked());
        assertFalse("the check's own mapping comparison found nothing to send", outcome.mappingChanged());
        assertNull("the refused update is the detector's, not the derivation's", outcome.mappingError());
        assertEquals("the detector sent the one PutMapping", 1, client.count(PutMappingAction.NAME));
        PutMappingRequest sent = (PutMappingRequest) client.requests(PutMappingAction.NAME).get(0);
        assertTrue("the update marks the stale name: " + sent.source(), sent.source().contains("\"lance_dropped\":\"true\""));
        assertEquals(1, service.stats().failures());
        assertTrue("the detector's refusal is not a refused derivation", service.stats().mappingErrors().isEmpty());
    }

    private static String writeTable(String hint) throws Exception {
        Path dir = createTempDir();
        String name = "demo-" + hint.toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        Files.createDirectories(dir);
        return LanceTableFactory.writeMultiFragmentTable(dir, name, 6, 3);
    }

    private static long currentVersion(String tableUri) {
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            return dataset.version();
        }
    }

    /**
     * A shard stand-in over a real table: it serves the version it was
     * "opened" at, follows the table (or the tag) on refresh the way the
     * engine does, and holds the mapping the index has so the comparison
     * answers from real derived mappings.
     */
    private static final class FakeShard implements LanceIndexFreshnessService.TrackedShard {

        private final String indexName;
        private final String indexUuid = randomAlphaOfLength(12);
        private final String tableUri;
        private final Settings settings;
        private long served;
        private Map<String, Object> currentMapping;
        final AtomicInteger refreshes = new AtomicInteger();
        volatile boolean typeConflict;

        private FakeShard(String indexName, String tableUri, Settings settings) {
            this.indexName = indexName;
            this.tableUri = tableUri;
            this.settings = settings;
        }

        static FakeShard overTable(String indexName, String tableUri, Settings extra) throws Exception {
            Settings.Builder settings = Settings.builder().put(extra);
            if (tableUri != null) {
                settings.put(LanceEngineFactory.TABLE_SETTING, tableUri);
            }
            FakeShard shard = new FakeShard(indexName, tableUri, settings.build());
            if (tableUri != null) {
                try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
                    shard.served = dataset.version();
                    shard.currentMapping = parse(RestAttachAction.derive(dataset, LanceOverrides.EMPTY, true).mappingJson());
                }
            }
            return shard;
        }

        @Override
        public String indexName() {
            return indexName;
        }

        @Override
        public String indexUuid() {
            return indexUuid;
        }

        @Override
        public Settings settings() {
            return settings;
        }

        @Override
        public long servedVersion() {
            return served;
        }

        @Override
        public LanceIndexFreshnessService.MappingComparison compareMapping(String mappingJson) {
            if (typeConflict) {
                return LanceIndexFreshnessService.MappingComparison.TYPE_CONFLICT;
            }
            Map<String, Object> derived = parse(mappingJson);
            if (derived.equals(currentMapping)) {
                return LanceIndexFreshnessService.MappingComparison.UNCHANGED;
            }
            currentMapping = derived;
            return LanceIndexFreshnessService.MappingComparison.CHANGED;
        }

        @Override
        public void refresh(String source) {
            refreshes.incrementAndGet();
            String tag = settings.get(LanceEngineFactory.TAG_SETTING, "");
            try (Dataset latest = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
                served = tag.isEmpty() ? latest.version() : latest.tags().getVersion(tag);
            }
        }

        private static Map<String, Object> parse(String json) {
            return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
        }
    }

    /**
     * A no-op client that records the requests it receives, fails the one
     * action named in {@link #failing}, and answers a mapping read with
     * {@link #mappingSource} (an empty mapping when null) so the drift
     * detector sees a real response instead of the no-op's null.
     */
    private static final class RecordingClient extends NoOpClient {
        private final List<String> actionNames = new CopyOnWriteArrayList<>();
        private final List<ActionRequest> requests = new CopyOnWriteArrayList<>();
        volatile String failing;
        volatile Map<String, Object> mappingSource;

        RecordingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            actionNames.add(action.name());
            requests.add(request);
            if (action.name().equals(failing)) {
                listener.onFailure(new IllegalStateException(action.name() + " refused: delete refused by the test client"));
                return;
            }
            if (action.name().equals(GetMappingsAction.NAME)) {
                String index = ((GetMappingsRequest) request).indices()[0];
                Map<String, Object> source = mappingSource == null ? Map.of() : mappingSource;
                listener.onResponse((Response) new GetMappingsResponse(Map.of(index, new MappingMetadata("_doc", source))));
                return;
            }
            super.doExecute(action, request, listener);
        }

        int count(String actionName) {
            int count = 0;
            for (String name : actionNames) {
                if (name.equals(actionName)) {
                    count++;
                }
            }
            return count;
        }

        List<ActionRequest> requests(String actionName) {
            List<ActionRequest> matching = new ArrayList<>();
            for (int i = 0; i < actionNames.size(); i++) {
                if (actionNames.get(i).equals(actionName)) {
                    matching.add(requests.get(i));
                }
            }
            return matching;
        }
    }
}
