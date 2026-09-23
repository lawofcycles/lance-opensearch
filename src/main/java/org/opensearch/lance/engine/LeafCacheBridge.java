/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;

/**
 * Bridge from a {@link LanceFragmentLeafReader} to Lucene's cache
 * lifecycle. IndicesQueryCache, IndicesFieldDataCache and
 * IndicesRequestCache all key entries by {@link IndexReader.CacheKey}
 * and rely on {@link IndexReader.ClosedListener} to invalidate them.
 * {@code IndexReader.CacheKey} has a package-private constructor, so a
 * plugin sitting outside the {@code org.apache.lucene.index} package
 * cannot mint its own key. We instead hold a tiny one-doc Lucene reader
 * whose lifetime is bound to the fragment reader: its CacheHelper is
 * exposed as the fragment reader's, and closing the fragment reader
 * closes the bridge, which fires the listeners registered by the
 * OpenSearch caches.
 *
 * <p>Built on the first {@link #coreCacheHelper} /
 * {@link #readerCacheHelper} call rather than in the constructor: the
 * bridge costs an IndexWriter, a commit and a DirectoryReader.open, and
 * the fragment path opens one leaf per fragment per request, so an
 * aggregation over hundreds of fragments would pay that per leaf
 * although nothing on its path asks for a leaf-level cache key (the
 * query cache is disabled, the request cache keys off the composite
 * reader, and numeric doc values fielddata is built without the cache).
 * Consumers that do ask (the bitset filter cache for nested docs, global
 * ordinals fielddata, a DLS/FLS reader wrapper) get the same bridge for
 * the life of the leaf. Guarded by {@code this} and published through
 * the volatile field; helpers may be requested from any slice thread.
 * {@code closed} (also guarded by {@code this}) keeps a helper request
 * that races with close from building a bridge {@link #close} has
 * already read as absent, which nothing would ever close.
 *
 * <p>Owns the bridge reader and its close race guard only; it knows
 * nothing about the fragment, its columns or its doc ids.
 */
final class LeafCacheBridge {

    private volatile DirectoryReader cacheLifetimeBridge;
    private boolean closed;

    /**
     * The one-doc Lucene reader backing {@link #coreCacheHelper()} and
     * {@link #readerCacheHelper()}, built on first use (see the class
     * comment for why it is not built in the constructor). The instance
     * is stable for the life of the owning leaf, as the cache helper
     * contract requires.
     *
     * @throws AlreadyClosedException when the
     *         leaf was closed before any helper was requested; building
     *         a bridge then would leak it, because {@link #close()}
     *         has already read the field as absent
     */
    private DirectoryReader cacheLifetimeBridge() {
        DirectoryReader bridge = cacheLifetimeBridge;
        if (bridge != null) {
            return bridge;
        }
        synchronized (this) {
            bridge = cacheLifetimeBridge;
            if (bridge != null) {
                return bridge;
            }
            if (closed) {
                throw new AlreadyClosedException("this LanceFragmentLeafReader was closed before a cache helper was requested");
            }
            try {
                ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
                try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
                    writer.addDocument(new Document());
                    writer.commit();
                }
                bridge = DirectoryReader.open(bridgeDir);
            } catch (IOException e) {
                // getCoreCacheHelper / getReaderCacheHelper cannot
                // throw a checked exception; an in-heap one-doc index
                // only fails when the JVM is already in trouble.
                throw new UncheckedIOException("could not build the cache lifetime bridge", e);
            }
            cacheLifetimeBridge = bridge;
            return bridge;
        }
    }

    /** Whether the cache lifetime bridge has been built. Test observability only. */
    boolean exists() {
        return cacheLifetimeBridge != null;
    }

    /** Core cache helper of the bridge's single leaf; the fragment reader exposes it as its own. */
    IndexReader.CacheHelper coreCacheHelper() {
        return cacheLifetimeBridge().leaves().get(0).reader().getCoreCacheHelper();
    }

    /** Reader cache helper of the bridge; the fragment reader exposes it as its own. */
    IndexReader.CacheHelper readerCacheHelper() {
        return cacheLifetimeBridge().getReaderCacheHelper();
    }

    /**
     * Mark the bridge closed and close the one-doc reader when it was
     * built, firing the closed listeners the OpenSearch caches
     * registered. Called from the fragment reader's {@code doClose}.
     */
    void close() throws IOException {
        DirectoryReader bridge;
        synchronized (this) {
            closed = true;
            bridge = cacheLifetimeBridge;
        }
        // Close outside the monitor so the bridge's closed listeners
        // (cache invalidation callbacks) do not run while holding it.
        if (bridge != null) {
            bridge.close();
        }
    }
}
