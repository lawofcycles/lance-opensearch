/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import org.opensearch.lance.engine.HidingReaderWrapper;
import org.opensearch.lance.engine.LanceWarmCache;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * The fragment executor on a node that holds no shard copy builds its
 * {@code IndexService} through {@code IndicesService.withTempIndexService}.
 * That path has to carry the reader wrapper a plugin installs through
 * {@code IndexModule#setReaderWrapper} (the security plugin's DLS / FLS
 * wrapper in production), otherwise hits and {@code hits.total.value}
 * would bypass it. The node runs with
 * {@code plugins.lance.test.hiding_wrapper_index_prefix} set, so the
 * plugin installs a {@link HidingReaderWrapper} on every Lance backed
 * index whose name starts with {@code wrapped} that hides the rows whose
 * {@code id} is below 6; the tests check the wrapper is on the temporary
 * {@code IndexService} and that the executor honours it once the node's
 * own {@code IndexService} for the index has been removed.
 *
 * <p>Thread leak checks are off as in the other tests that load the Lance
 * native library: its runtime keeps a native thread alive after the last
 * dataset is closed.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TempIndexServiceReaderWrapperTests extends OpenSearchSingleNodeTestCase {

    private static final int ROWS = 12;
    private static final int ROWS_PER_FRAGMENT = 4;
    /** Rows whose {@code id} is below this are hidden, so rows 6 to 11 are visible. */
    private static final int FIRST_VISIBLE_ID = 6;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.getKey(), "wrapped:body:id:" + FIRST_VISIBLE_ID)
            .build();
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
        // It still carries index.plugins.lance.table and the name still
        // starts with the prefix, which is what the plugin keys on.
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
        assertEquals(HidingReaderWrapper.class, wrapperClass);
    }

    public void testExecutorHonoursTheWrapperWithoutALocalIndexService() throws Exception {
        String indexName = "wrapped-executor";
        String tableUri = attach(indexName);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        Index index = getInstanceFromNode(ClusterService.class).state().metadata().index(indexName).getIndex();

        LanceFragmentQueryRequest request = FragmentRequests.planned(
            getInstanceFromNode(ClusterService.class),
            getInstanceFromNode(LanceWarmCache.class),
            tableUri,
            indexName,
            /* query */ null,
            List.of(),
            ROWS,
            /* aggregations */ null,
            List.of()
        );

        // With the node's own IndexService the wrapper is applied the way
        // it always was: the six rows with id 6 to 11 survive.
        LanceFragmentQueryResponse viaLocal = executor.execute(request);
        assertEquals(ROWS - FIRST_VISIBLE_ID, viaLocal.matched());
        assertEquals(ROWS - FIRST_VISIBLE_ID, viaLocal.hits().size());

        // Remove the node's IndexService while the index stays in cluster
        // state. This is the state of a data node without a shard copy;
        // the executor has to build a temporary IndexService and must
        // see the same wrapper.
        indicesService.removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, "simulate a node without a shard copy");
        assertNull(indicesService.indexService(index));
        LanceFragmentQueryResponse viaTemp = executor.execute(request);
        assertNull("the executor must not have registered an IndexService", indicesService.indexService(index));
        assertEquals(ROWS - FIRST_VISIBLE_ID, viaTemp.matched());
        assertEquals(ROWS - FIRST_VISIBLE_ID, viaTemp.hits().size());
        for (SearchHit hit : viaTemp.hits()) {
            // Ids are "<fragment>-<offset>" and row i sits at fragment
            // i / 4, offset i % 4; the wrapper hides the rows below id 6.
            assertTrue("hit " + hit.getId() + " should have been hidden", rowOf(hit) >= FIRST_VISIBLE_ID);
        }
        assertEquals(ids(viaLocal.hits()), ids(viaTemp.hits()));
    }

    private static int rowOf(SearchHit hit) {
        int dash = hit.getId().indexOf('-');
        int fragment = Integer.parseInt(hit.getId().substring(0, dash));
        int offset = Integer.parseInt(hit.getId().substring(dash + 1));
        return fragment * ROWS_PER_FRAGMENT + offset;
    }

    private static List<String> ids(List<SearchHit> hits) {
        return hits.stream().map(SearchHit::getId).toList();
    }
}
