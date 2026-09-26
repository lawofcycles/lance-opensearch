/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexModule;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.namespace.LanceIndexSyncAction;
import org.opensearch.lance.namespace.LanceIndexSyncRequest;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * {@code GET /_lance/stats} must not name columns a reader wrapper (the
 * security plugin's DLS / FLS wrapper) may hide. {@link WrapperOnPrefixPlugin}
 * installs a wrapper on the Lance-backed indexes whose name starts with
 * {@code wrapped}, so one node hosts a wrapped and a plain index side by
 * side and the report of each can be compared: the plain index lists
 * its Lance index types and the rename its mapping records, the wrapped
 * index lists neither.
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
        return List.of(LancePlugin.class, WrapperOnPrefixPlugin.class);
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

    public void testWrapperHidesIndexTypesAndRenamedFields() throws Exception {
        String plain = "plain-stats";
        String wrapped = "wrapped-stats";
        attachAndRename(plain);
        attachAndRename(wrapped);

        // The mapping update the freshness check sends is applied by the
        // cluster state applier, so the rename shows up shortly after
        // the sync returns.
        assertBusy(() -> {
            LanceNodeStats.IndexReaderStats stats = statsOf(plain);
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
        assertEquals(ROWS, stats.shardReaderRows());
        assertTrue("wrapped index must not list Lance index types: " + stats.indexTypes(), stats.indexTypes().isEmpty());
        assertTrue("wrapped index must not list renamed fields: " + stats.renamedFields(), stats.renamedFields().isEmpty());
    }

    /**
     * Installs a reader wrapper on every Lance-backed index whose name
     * starts with {@code wrapped}. The wrapper itself changes nothing;
     * the stats action only asks whether one is installed.
     */
    public static class WrapperOnPrefixPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null
                && indexModule.getIndex().getName().startsWith("wrapped")) {
                indexModule.setReaderWrapper(indexService -> reader -> reader);
            }
        }
    }
}
