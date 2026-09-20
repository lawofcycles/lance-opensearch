/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Bits;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * The fragment executor on a node that holds no shard copy builds its
 * {@code IndexService} through {@code IndicesService.withTempIndexService}.
 * That path has to carry the reader wrapper a plugin installs through
 * {@link IndexModule#setReaderWrapper} (the security plugin's DLS / FLS
 * wrapper in production), otherwise hits and {@code hits.total.value}
 * would bypass it. {@link HidingReaderWrapperPlugin} installs a wrapper
 * that hides every odd document of a Lance-backed index; the tests check
 * the wrapper is on the temporary {@code IndexService} and that the
 * executor honours it once the node's own {@code IndexService} for the
 * index has been removed.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library: its runtime keeps a native thread alive after the last
 * dataset is closed.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TempIndexServiceReaderWrapperTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 12;
    private static final int ROWS_PER_FRAGMENT = 4;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class, HidingReaderWrapperPlugin.class);
    }

    private String attach(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeMultiFragmentTable(dir, indexName, ROWS, ROWS_PER_FRAGMENT);
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        assertEquals(ROWS / ROWS_PER_FRAGMENT, attached.fragments());
        ensureGreen(indexName);
        return tableUri;
    }

    public void testTemporaryIndexServiceCarriesTheInstalledReaderWrapper() throws Exception {
        String indexName = "wrapped-temp-service";
        attach(indexName);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexMetadata attached = getInstanceFromNode(ClusterService.class).state().metadata().index(indexName);
        assertNotNull(attached);
        // withTempIndexService refuses an index the node already has, so
        // hand it a copy of the metadata under a fresh name and UUID.
        // It still carries index.lance.table, which is what the wrapper
        // plugin keys on.
        IndexMetadata detached = IndexMetadata.builder(attached)
            .index(indexName + "-detached")
            .settings(
                Settings.builder().put(attached.getSettings()).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build()
            )
            .build();
        Class<?> wrapperClass = indicesService.withTempIndexService(detached, indexService -> {
            var wrapper = TransportLanceFragmentQueryAction.resolveReaderWrapper(indexService);
            assertNotNull("temporary IndexService carries no reader wrapper", wrapper);
            return wrapper.getClass();
        });
        assertEquals(HidingReaderWrapperPlugin.WRAPPER_FACTORY_CLASS, wrapperClass);
    }

    public void testExecutorHonoursTheWrapperWithoutALocalIndexService() throws Exception {
        String indexName = "wrapped-executor";
        String tableUri = attach(indexName);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        Index index = getInstanceFromNode(ClusterService.class).state().metadata().index(indexName).getIndex();

        LanceFragmentQueryRequest request = new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            /* pinnedVersion */ -1L,
            /* filterSql */ null,
            /* query */ null,
            /* postFilter */ null,
            List.of(),
            /* searchAfter */ null,
            ROWS,
            /* aggregations */ null,
            List.of(),
            false
        );

        // With the node's own IndexService the wrapper is applied the way
        // it always was: two of the four rows of each fragment survive.
        LanceFragmentQueryResponse viaLocal = executor.execute(request);
        assertEquals(ROWS / 2, viaLocal.matched());
        assertEquals(ROWS / 2, viaLocal.hits().size());

        // Remove the node's IndexService while the index stays in cluster
        // state. This is the state of a data node without a shard copy;
        // the executor has to build a temporary IndexService and must
        // see the same wrapper.
        indicesService.removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, "simulate a node without a shard copy");
        assertNull(indicesService.indexService(index));
        LanceFragmentQueryResponse viaTemp = executor.execute(request);
        assertNull("the executor must not have registered an IndexService", indicesService.indexService(index));
        assertEquals(ROWS / 2, viaTemp.matched());
        assertEquals(ROWS / 2, viaTemp.hits().size());
        for (SearchHit hit : viaTemp.hits()) {
            // Ids are "<fragment>-<offset>", and the wrapper hides odd
            // offsets inside every fragment leaf.
            int offset = Integer.parseInt(hit.getId().substring(hit.getId().indexOf('-') + 1));
            assertEquals("hit " + hit.getId() + " should have been hidden", 0, offset % 2);
        }
        assertEquals(ids(viaLocal.hits()), ids(viaTemp.hits()));
    }

    private static List<String> ids(List<SearchHit> hits) {
        return hits.stream().map(SearchHit::getId).toList();
    }

    /**
     * Installs a reader wrapper on every Lance-backed index that hides the
     * odd documents of each leaf, the way a DLS filter hides rows.
     */
    public static class HidingReaderWrapperPlugin extends Plugin {

        static final Class<?> WRAPPER_FACTORY_CLASS = HidingWrapperFactory.class;

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null) {
                indexModule.setReaderWrapper(indexService -> new HidingWrapperFactory());
            }
        }
    }

    static final class HidingWrapperFactory implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
        @Override
        public DirectoryReader apply(DirectoryReader reader) throws IOException {
            return new HidingDirectoryReader(reader);
        }
    }

    static final class HidingDirectoryReader extends FilterDirectoryReader {

        HidingDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new HidingLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new HidingDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // Same contract as the security plugin's wrapper: the
            // directory level key stays that of the wrapped reader so
            // IndexShard#wrapSearcher accepts it.
            return in.getReaderCacheHelper();
        }
    }

    static final class HidingLeafReader extends FilterLeafReader {

        HidingLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public Bits getLiveDocs() {
            Bits inner = in.getLiveDocs();
            int maxDoc = in.maxDoc();
            return new Bits() {
                @Override
                public boolean get(int index) {
                    return index % 2 == 0 && (inner == null || inner.get(index));
                }

                @Override
                public int length() {
                    return maxDoc;
                }
            };
        }

        @Override
        public int numDocs() {
            Bits live = getLiveDocs();
            int count = 0;
            for (int i = 0; i < live.length(); i++) {
                if (live.get(i)) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // Live docs differ from the wrapped reader, so this reader
            // must not share its cache key.
            return null;
        }
    }
}
