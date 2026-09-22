/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The cache lifetime bridge of a fragment leaf is built on the first
 * {@code getCoreCacheHelper} / {@code getReaderCacheHelper} call (not
 * in the constructor), stays the same for the life of the leaf, is
 * distinct between leaves, and fires its closed listeners when the
 * leaf closes. A leaf whose helpers are never requested never builds
 * a bridge, and a helper requested after close refuses to build one
 * (nothing would ever close it).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentLeafReaderCacheBridgeTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 2;
    private static final int ROWS_PER_FRAGMENT = 100;

    private LanceDirectoryReader openReader() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "bridge-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT);
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        List<Integer> fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        return LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            "",
            LanceEngineFactory.LancePrimaryKeyType.NONE,
            Collections.emptyMap(),
            fragmentIds
        );
    }

    private static LanceFragmentLeafReader raw(LeafReaderContext ctx) {
        LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(ctx.reader());
        assertNotNull(leaf);
        return leaf;
    }

    public void testBridgeIsBuiltOnFirstHelperCallAndIsStablePerLeaf() throws Exception {
        try (LanceDirectoryReader reader = openReader()) {
            List<LeafReaderContext> leaves = reader.leaves();
            assertEquals(FRAGMENTS, leaves.size());

            // Construction is bridge-free; the constructor no longer
            // pays an IndexWriter commit per leaf.
            assertFalse(raw(leaves.get(0)).cacheLifetimeBridgeExists());
            assertFalse(raw(leaves.get(1)).cacheLifetimeBridgeExists());

            IndexReader.CacheHelper firstCore = leaves.get(0).reader().getCoreCacheHelper();
            assertTrue(raw(leaves.get(0)).cacheLifetimeBridgeExists());
            // Asking one leaf does not build the others' bridges.
            assertFalse(raw(leaves.get(1)).cacheLifetimeBridgeExists());

            IndexReader.CacheHelper secondCore = leaves.get(1).reader().getCoreCacheHelper();
            assertTrue(raw(leaves.get(1)).cacheLifetimeBridgeExists());
            assertNotNull(firstCore);
            assertNotNull(secondCore);
            // Stable across calls: cache entries keyed on the first call's
            // key must be found by the second call's key.
            assertSame(firstCore, leaves.get(0).reader().getCoreCacheHelper());
            assertSame(firstCore.getKey(), leaves.get(0).reader().getCoreCacheHelper().getKey());
            assertSame(leaves.get(0).reader().getReaderCacheHelper(), leaves.get(0).reader().getReaderCacheHelper());
            // Distinct leaves must not share a key: OpenSearch's bitset
            // filter cache and fielddata cache key per-leaf state on it.
            assertNotSame(firstCore.getKey(), secondCore.getKey());
        }
    }

    public void testClosedListenerFiresOnLeafClose() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "bridge-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Fragment fragment = dataset.getFragments().get(0);
            AtomicInteger closed = new AtomicInteger();
            LanceFragmentLeafReader leaf = new LanceFragmentLeafReader(
                dataset,
                fragment.getId(),
                fragment.metadata().getPhysicalRows(),
                fragment.metadata().getDeletionFile() != null,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                Collections.emptyMap(),
                Collections.emptySet(),
                null
            );
            assertFalse(leaf.cacheLifetimeBridgeExists());
            IndexReader.CacheHelper core = leaf.getCoreCacheHelper();
            assertTrue(leaf.cacheLifetimeBridgeExists());
            core.addClosedListener(key -> {
                assertSame(core.getKey(), key);
                closed.incrementAndGet();
            });
            assertEquals(0, closed.get());
            leaf.close();
            assertEquals(1, closed.get());
        }
    }

    public void testReaderClosesWithoutEverBuildingABridge() throws Exception {
        LanceDirectoryReader reader = openReader();
        List<LanceFragmentLeafReader> raws = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            raws.add(raw(ctx));
        }
        for (LanceFragmentLeafReader leaf : raws) {
            assertFalse(leaf.cacheLifetimeBridgeExists());
        }
        reader.close();
        // No helper was ever requested, so no bridge was ever built.
        for (LanceFragmentLeafReader leaf : raws) {
            assertFalse(leaf.cacheLifetimeBridgeExists());
        }
    }

    public void testHelperRequestAfterCloseDoesNotBuildABridge() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "bridge-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            Fragment fragment = dataset.getFragments().get(0);
            LanceFragmentLeafReader leaf = new LanceFragmentLeafReader(
                dataset,
                fragment.getId(),
                fragment.metadata().getPhysicalRows(),
                fragment.metadata().getDeletionFile() != null,
                "",
                LanceEngineFactory.LancePrimaryKeyType.NONE,
                Collections.emptyMap(),
                Collections.emptySet(),
                null
            );
            leaf.close();
            // A bridge built now would never be closed (doClose already
            // read the field as absent), so the leaf refuses.
            expectThrows(AlreadyClosedException.class, leaf::getCoreCacheHelper);
            assertFalse(leaf.cacheLifetimeBridgeExists());
        }
    }
}
