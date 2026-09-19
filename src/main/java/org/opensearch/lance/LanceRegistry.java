/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.OpenDatasetBuilder;
import org.lance.ReadOptions;
import org.lance.Session;

/**
 * Process-wide Arrow allocator and Lance {@link Session} shared by every
 * plugin-owned code path that needs to open a Lance dataset, plus the
 * single-source helper for constructing that {@link Dataset} instance.
 * Every call site that used to build the {@link OpenDatasetBuilder}
 * inline now goes through {@link #openDataset(String, StorageOptions)} so
 * the storage-options plumbing and the Session sharing both live in one
 * place.
 *
 * <p>Sharing one {@link Session} across every {@code Dataset} on the node
 * keeps Lance's inverted-index and metadata caches native-side and node
 * scoped: with a per-shard {@code Dataset} each shard used to allocate its
 * own 6 GiB / 1 GiB caches (the Lance defaults) on demand, so 200 shards
 * of the same table could drive the resident set above 100 GiB even
 * though JVM heap stayed at its configured maximum. The shared Session
 * caps the two caches to the node-level limits configured by
 * {@code lance.native_memory.limit}, and {@link Session#sizeBytes()}
 * exposes the current usage so a follow-up circuit-breaker layer can
 * feed it back into OpenSearch's memory accounting.
 *
 * <p>Historically this class also cached a per-index {@code Dataset} for
 * two REST endpoints ({@code _scan} / {@code _query}); the endpoints were
 * undocumented PoC leftovers and both they and the caching layer have
 * been removed.
 */
public final class LanceRegistry {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    /**
     * Node-wide Lance {@link Session}. Initialised by
     * {@link #initSession(long, long)} from the plugin's
     * {@code createComponents} hook and released by
     * {@link #closeSession()} from {@code Plugin.close}. Kept as
     * {@code volatile} so the reader in {@link #openDataset(String, StorageOptions)}
     * observes the write from the initialising thread without a lock on
     * the fast path; writes go through the synchronised init / close
     * methods below.
     */
    private static volatile Session SESSION;

    /**
     * Cache sizes captured at {@link #initSession(long, long)} time so
     * that a runtime rebuild through {@link #reinstallSession()} can
     * recreate the {@link Session} with the exact same limits. Storing
     * the values here keeps the caller from having to plumb them
     * through every code path that might need to reinstall the
     * session (the {@code IndexEventListener} that fires on index
     * DELETE, most importantly).
     */
    private static volatile long indexCacheBytes;
    private static volatile long metadataCacheBytes;

    private LanceRegistry() {}

    public static BufferAllocator allocator() {
        return ALLOCATOR;
    }

    /**
     * Install a node-scoped {@link Session} with the given cache sizes.
     * Called once from {@code LancePlugin.createComponents}. If a Session
     * is already installed (e.g. an integration test framework restart
     * within the same JVM) the existing Session is closed first so its
     * native cache is released before the new one is built.
     *
     * @param indexCacheBytes    upper bound of the shared index cache
     * @param metadataCacheBytes upper bound of the shared metadata cache
     */
    public static synchronized void initSession(long indexCacheBytes, long metadataCacheBytes) {
        if (SESSION != null && !SESSION.isClosed()) {
            SESSION.close();
        }
        LanceRegistry.indexCacheBytes = indexCacheBytes;
        LanceRegistry.metadataCacheBytes = metadataCacheBytes;
        SESSION = Session.builder().indexCacheSizeBytes(indexCacheBytes).metadataCacheSizeBytes(metadataCacheBytes).build();
    }

    /**
     * Rebuild the node-scoped {@link Session} preserving the cache
     * sizes captured at {@link #initSession(long, long)} time. Used
     * when the plugin needs to force the Lance cache to drop stale
     * entries; the concrete trigger today is
     * {@code IndexEventListener.beforeIndexRemoved} with reason
     * {@code DELETED} on a Lance-backed index, because the cache
     * would otherwise return pages that reference the just-removed
     * {@code _indices/<uuid>/*.lance} files if the same path is
     * re-attached (see issue #46 for the reproducer). The reproducer
     * would keep returning 500 {@code Not found: ... _indices/<old-uuid>/}
     * on GET until the node restarted; this call is what closes that
     * window without a restart.
     *
     * <p>Safety w.r.t. in-flight datasets: Lance's Rust {@code Session}
     * lives inside an {@code Arc<Session>} that every {@code Dataset}
     * captures through {@code ReadOptions.setSession}. Datasets that
     * already captured the old session keep their own {@code Arc}, so
     * the native session stays alive for their cache reads and is
     * dropped only once the last dataset closes. The new session takes
     * over for any {@code openDataset} that happens after this call
     * returns.
     *
     * <p>Concurrency detail: we deliberately do NOT call
     * {@link Session#close()} on the previous session, because
     * {@code Session.close()} in {@code lance-jni} is
     * {@code Box::from_raw} of the {@code Box<Arc<LanceSession>>}
     * whose raw pointer is what {@link #openDataset} reads from
     * {@code Session.nativeSessionHandle} and passes to
     * {@code Dataset.openNative}. The Rust side dereferences that
     * pointer through {@code session_from_handle} to clone the
     * {@code Arc}, and if a concurrent {@code close()} calls
     * {@code Box::from_raw} between the pointer read and the clone,
     * the dereference is use-after-free and crashes the JVM with
     * {@code SIGBUS} in {@code Java_org_lance_Dataset_openNative}.
     * Under repeated DELETE events during the integration test suite
     * this race was reproducible; the {@code openDataset} reader
     * cannot be synchronised against {@code close()} without holding
     * a lock across the whole native call. Leaving {@code previous}
     * unclosed leaks the Java wrapper and one {@code Arc}
     * reference, so the underlying native {@code Session} object is
     * not reclaimed until JVM exit. The cache pages inside that
     * session are still dropped as their owning datasets close, so
     * the leak is a bounded per-reinstall constant rather than
     * per-cache-entry.
     *
     * <p>Coarseness: this is a whole-cache invalidation. Every path
     * pays the cost of a cold cache on its next query, not just the
     * path that triggered the rebuild, because Lance 11's Java SDK
     * does not expose a per-path invalidation entry point. A tighter
     * invalidation would need an upstream {@code Session#invalidatePath}
     * or equivalent (tracked separately as a Lance feature request);
     * once that lands we can replace this coarse rebuild with a
     * targeted invalidation. Until then the trade-off is: the same
     * node running many Lance-backed indexes takes a warm-up hit on
     * every {@code DELETE} of one Lance-backed index, in exchange for
     * correctness on the re-attach path.
     *
     * <p>No-op when {@link #initSession(long, long)} has not been
     * called, so unit tests that never install a Session are
     * unaffected.
     */
    public static synchronized void reinstallSession() {
        Session previous = SESSION;
        if (previous == null) {
            return;
        }
        Session next = Session.builder().indexCacheSizeBytes(indexCacheBytes).metadataCacheSizeBytes(metadataCacheBytes).build();
        SESSION = next;
        // Deliberately do NOT call previous.close(). See javadoc above.
        // The old Session leaks its Java wrapper and one Arc reference,
        // but that is required to avoid a use-after-free crash in
        // Java_org_lance_Dataset_openNative when a concurrent shard is
        // still in the process of building a Dataset against the old
        // session handle.
    }

    /**
     * Release the node-scoped {@link Session}. Called from
     * {@code LancePlugin.close}. Existing {@code Dataset} handles keep
     * their own reference to the underlying native session, so this call
     * is safe even if some shards are still open at the moment of
     * shutdown.
     */
    public static synchronized void closeSession() {
        if (SESSION != null) {
            if (!SESSION.isClosed()) {
                SESSION.close();
            }
            SESSION = null;
        }
    }

    /**
     * Return the current node-scoped Session, or {@code null} if none
     * has been installed. Package-private so tests can assert on the
     * identity of the underlying native session via
     * {@link Session#isSameAs(Session)}.
     */
    static Session currentSession() {
        return SESSION;
    }

    /**
     * Open a Lance dataset against {@code uri} using {@code storageOptions}
     * for object-store credentials, endpoints, and timeouts. Callers pass
     * {@link StorageOptions#empty()} when the URI is a local filesystem
     * path; Lance's Rust {@code object_store} then relies on its own
     * environment-variable fallback (AWS_*, GCS_*, AZURE_*) for remote
     * URIs when the map is empty.
     *
     * <p>If a node-scoped {@link Session} has been installed via
     * {@link #initSession(long, long)}, the dataset is opened against
     * that Session so its index and metadata caches are shared with
     * every other {@code Dataset} on this node. Otherwise Lance falls
     * back to a per-{@code Dataset} internal session with its own
     * default cache sizes; this branch exists so unit tests that never
     * call {@code initSession} keep working, but production code paths
     * always go through the plugin's {@code createComponents} and hit
     * the shared Session.
     */
    public static Dataset openDataset(String uri, StorageOptions storageOptions) {
        return openDataset(uri, storageOptions, java.util.Optional.empty());
    }

    /**
     * Open a Lance dataset at a specific manifest version. When
     * {@code pinnedVersion} is non-empty, the returned dataset is
     * pinned to that Lance version and will not follow subsequent
     * appends. Callers that pin should also opt the resulting index
     * out of the namespace poll cycle (see {@code
     * LanceNamespaceService.registerAttachedIndex}) so refresh does
     * not race with a manifest advance.
     *
     * <p>Storage options and version pinning both go through
     * {@link ReadOptions}, so this method combines them into a single
     * {@code ReadOptions} rather than round-tripping through
     * {@link StorageOptions#toReadOptionsOrNull} (which would drop
     * the version silently).
     */
    public static Dataset openDataset(String uri, StorageOptions storageOptions, java.util.Optional<Long> pinnedVersion) {
        OpenDatasetBuilder builder = Dataset.open().allocator(ALLOCATOR).uri(uri);
        Session session = SESSION;
        if (session != null && !session.isClosed()) {
            builder = builder.session(session);
        }
        java.util.Map<String, String> storageMap = storageOptions == null ? null : storageOptions.asMap();
        boolean hasStorage = storageMap != null && !storageMap.isEmpty();
        if (hasStorage || pinnedVersion.isPresent()) {
            ReadOptions.Builder roBuilder = new ReadOptions.Builder();
            if (hasStorage) {
                roBuilder.setStorageOptions(storageMap);
            }
            pinnedVersion.ifPresent(roBuilder::setVersion);
            builder = builder.readOptions(roBuilder.build());
        }
        return builder.build();
    }
}
