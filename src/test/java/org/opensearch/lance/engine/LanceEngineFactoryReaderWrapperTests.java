/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.opensearch.action.get.GetResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * A GET of a row outside the shard reader (the table is above the Lucene
 * document bound) is answered under an installed reader wrapper: the GET
 * filter reads the row through a reader over its fragment that the
 * index's wrapper is applied to, so the shard reader and its bound do
 * not enter the lookup. The wrapper installed here hands back the reader
 * it is given, the way a security plugin whose rules admit every row
 * might, and the wrapped index answers every row the plain index next
 * to it answers.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceEngineFactoryReaderWrapperTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 12;
    private static final int ROWS_PER_FRAGMENT = 4;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class, IdentityWrapperOnPrefixPlugin.class);
    }

    private void attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeStringPkTable(dir, indexName, ROWS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        assertTrue("the bound of one fragment puts the table above it", attached.luceneBoundExceeded());
        ensureGreen(indexName);
    }

    public void testGetOutsideTheShardReaderAnswersUnderAnInstalledWrapper() throws Exception {
        String plain = "plain-outside";
        String wrapped = "wrapped-outside";
        // One fragment per shard reader: rows 0 to 3 are inside it, the
        // other eight rows are outside.
        updateMaxDocsPerReader(Long.toString(ROWS_PER_FRAGMENT));
        try {
            attach(plain);
            attach(wrapped);

            assertTrue(client().prepareGet(plain, "alpha-1").get().isExists());
            assertTrue(client().prepareGet(wrapped, "alpha-1").get().isExists());

            // Outside the shard reader both indexes resolve the row through
            // a reader over its fragment, the wrapped one with the wrapper
            // applied to that reader.
            GetResponse outside = client().prepareGet(plain, "alpha-5").get();
            assertTrue(outside.isExists());
            assertEquals("alpha-5", outside.getId());
            GetResponse wrappedOutside = client().prepareGet(wrapped, "alpha-9").get();
            assertTrue(wrappedOutside.isExists());
            assertEquals("alpha-9", wrappedOutside.getId());
            assertEquals("col-9", wrappedOutside.getSourceAsMap().get("label"));

            // A row that does not exist is no error on either index.
            assertFalse(client().prepareGet(plain, "alpha-99").get().isExists());
            assertFalse(client().prepareGet(wrapped, "alpha-99").get().isExists());
        } finally {
            updateMaxDocsPerReader(null);
        }
    }

    private void updateMaxDocsPerReader(String value) {
        Settings.Builder transientSettings = Settings.builder();
        if (value == null) {
            transientSettings.putNull("plugins.lance.test.max_docs_per_reader");
        } else {
            transientSettings.put("plugins.lance.test.max_docs_per_reader", value);
        }
        assertTrue(client().admin().cluster().prepareUpdateSettings().setTransientSettings(transientSettings).get().isAcknowledged());
    }

    /**
     * Installs, on every Lance backed index whose name starts with
     * {@code wrapped}, a reader wrapper that returns the reader it is
     * given, the way a security plugin whose rules admit every row of
     * the index for the caller might.
     */
    public static class IdentityWrapperOnPrefixPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null
                && indexModule.getIndex().getName().startsWith("wrapped")) {
                indexModule.setReaderWrapper(indexService -> reader -> reader);
            }
        }
    }
}
