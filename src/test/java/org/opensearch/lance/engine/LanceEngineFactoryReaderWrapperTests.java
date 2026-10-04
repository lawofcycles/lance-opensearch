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
 * document bound) opens a reader the index's reader wrapper never sees,
 * so the engine refuses it whenever a wrapper is installed on the index,
 * asked of the node's {@code IndexService} through
 * {@code ReaderWrapperProbe}. The wrapper installed here hands back the
 * reader it is given, which leaves the engine's own reader on top of the
 * searcher: a check on the type of that reader would let the GET through,
 * the probe does not. The plain index next to it answers the same GET.
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

    public void testGetOutsideTheShardReaderRefusesUnderAnInstalledWrapperWhateverTheWrapperReturns() throws Exception {
        String plain = "plain-outside";
        String wrapped = "wrapped-outside";
        // One fragment per shard reader: rows 0 to 3 are inside it, the
        // other eight rows are outside.
        updateMaxDocsPerReader(Long.toString(ROWS_PER_FRAGMENT));
        try {
            attach(plain);
            attach(wrapped);

            // Inside the shard reader both indexes answer: the wrapper
            // (which changes nothing) is applied to the shard searcher.
            assertTrue(client().prepareGet(plain, "alpha-1").get().isExists());
            assertTrue(client().prepareGet(wrapped, "alpha-1").get().isExists());

            // Outside it the plain index resolves the row through a
            // reader over its fragment.
            GetResponse outside = client().prepareGet(plain, "alpha-5").get();
            assertTrue(outside.isExists());
            assertEquals("alpha-5", outside.getId());

            // The wrapped index refuses, although the wrapper left the
            // engine's own reader on top of the searcher.
            Exception refused = expectThrows(Exception.class, () -> client().prepareGet(wrapped, "alpha-9").get());
            Throwable cause = refused;
            boolean found = false;
            for (int depth = 0; cause != null && depth < 10 && !found; depth++, cause = cause.getCause()) {
                String message = cause.getMessage();
                found = message != null
                    && message.contains(
                        "GET of a row outside the shard reader of ["
                            + wrapped
                            + "] (table above the Lucene document bound) cannot apply the index's reader wrapper (DLS / FLS)"
                    );
            }
            assertTrue("the refusal names the wrapper: " + refused, found);

            // A row that does not exist is not resolved outside either,
            // and is no error on the plain index.
            assertFalse(client().prepareGet(plain, "alpha-99").get().isExists());
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
