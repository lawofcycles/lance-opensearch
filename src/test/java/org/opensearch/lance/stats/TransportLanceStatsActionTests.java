/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.namespace.LanceIndexSyncAction;
import org.opensearch.lance.namespace.LanceIndexSyncRequest;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * {@code GET /_plugins/_lance/stats} must not report a figure a reader wrapper (the
 * security plugin's DLS / FLS wrapper) does not filter, nor name columns
 * it may hide. The node runs with
 * {@code plugins.lance.test.hiding_wrapper_index_prefix} set, so the
 * plugin installs its {@code HidingReaderWrapper} on the Lance-backed
 * indexes whose name starts with {@code wrapped}, and one node hosts a
 * wrapped and a plain index side by side so the report of each can be
 * compared: the plain index lists its row counts, its Lance index types
 * and the rename its mapping records, the wrapped index lists none of
 * them. What the wrapper hides plays no part; the stats action only asks
 * whether one is installed.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TransportLanceStatsActionTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 8;
    private static final int ROWS_PER_FRAGMENT = 4;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(LancePlugin.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.getKey(), "wrapped:body:id:4")
            .build();
    }

    /**
     * Attach a table under {@code indexName}, rename its {@code title}
     * column to {@code headline} and run the freshness check so the
     * mapping records the rename.
     */
    private void attachAndRename(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeMultiFragmentTable(dir, indexName, ROWS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
        LanceTableFactory.renameColumn(tableUri, "title", "headline");
        client().execute(LanceIndexSyncAction.INSTANCE, new LanceIndexSyncRequest(indexName)).actionGet();
    }

    private LanceNodeStats.IndexReaderStats statsOf(String indexName) {
        LanceStatsResponse response = client().execute(LanceStatsAction.INSTANCE, new LanceStatsRequest()).actionGet();
        assertEquals(1, response.getNodes().size());
        return response.getNodes()
            .get(0)
            .stats()
            .indices()
            .stream()
            .filter(index -> indexName.equals(index.index()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no stats for " + indexName));
    }

    public void testWrapperWithholdsRowCountsIndexTypesAndRenamedFields() throws Exception {
        String plain = "plain-stats";
        String wrapped = "wrapped-stats";
        attachAndRename(plain);
        attachAndRename(wrapped);

        // The mapping update the freshness check sends is applied by the
        // cluster state applier, so the rename shows up shortly after
        // the sync returns.
        assertBusy(() -> {
            LanceNodeStats.IndexReaderStats stats = statsOf(plain);
            assertFalse("plain index reports its counts", stats.rowsWithheld());
            assertEquals(ROWS, stats.rows());
            assertEquals(ROWS, stats.shardReaderRows());
            assertFalse("plain index must list its Lance index types", stats.indexTypes().isEmpty());
            List<LanceMappingMeta.RenamedField> renamed = stats.renamedFields();
            assertEquals("plain index must report the rename: " + renamed, 1, renamed.size());
            assertEquals("title", renamed.get(0).from());
            assertEquals("headline", renamed.get(0).to());
        }, 30, TimeUnit.SECONDS);

        // The wrapped index's mapping records the same rename; only the
        // report withholds it.
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        assertBusy(() -> {
            List<LanceMappingMeta.RenamedField> renamed = LanceMappingMeta.renamedFields(
                clusterService.state().metadata().index(wrapped).mapping()
            );
            assertEquals("wrapped index mapping must record the rename: " + renamed, 1, renamed.size());
        }, 30, TimeUnit.SECONDS);
        LanceNodeStats.IndexReaderStats stats = statsOf(wrapped);
        assertTrue("wrapped index must withhold its counts", stats.rowsWithheld());
        assertEquals(0L, stats.rows());
        assertEquals(0L, stats.shardReaderRows());
        assertEquals(0L, stats.nestedDocs());
        assertFalse(stats.luceneBoundExceeded());
        assertTrue("wrapped index must not list Lance index types: " + stats.indexTypes(), stats.indexTypes().isEmpty());
        assertTrue("wrapped index must not list renamed fields: " + stats.renamedFields(), stats.renamedFields().isEmpty());

        // The rendered report leaves the fields out for the wrapped
        // index and keeps them for the plain one.
        LanceStatsResponse response = client().execute(LanceStatsAction.INSTANCE, new LanceStatsRequest()).actionGet();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            response.getNodes().get(0).stats().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            Map<String, Object> rendered = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
            @SuppressWarnings("unchecked")
            Map<String, Object> indices = (Map<String, Object>) rendered.get("indices");
            @SuppressWarnings("unchecked")
            Map<String, Object> wrappedEntry = (Map<String, Object>) indices.get(wrapped);
            assertEquals(Map.of("lucene_bound_exceeded", false), wrappedEntry);
            @SuppressWarnings("unchecked")
            Map<String, Object> plainEntry = (Map<String, Object>) indices.get(plain);
            assertEquals(ROWS, plainEntry.get("rows"));
            assertEquals(ROWS, plainEntry.get("shard_reader_rows"));
            assertEquals(0, plainEntry.get("nested_docs"));
            assertTrue(plainEntry.toString(), plainEntry.containsKey("index_types"));
            assertTrue(plainEntry.toString(), plainEntry.containsKey("renamed_fields"));
        }
    }
}
