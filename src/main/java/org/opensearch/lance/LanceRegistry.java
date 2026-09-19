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
        SESSION = Session.builder().indexCacheSizeBytes(indexCacheBytes).metadataCacheSizeBytes(metadataCacheBytes).build();
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
