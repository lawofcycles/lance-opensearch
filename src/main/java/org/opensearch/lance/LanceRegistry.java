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
 * Every call site goes through {@link #openDataset(String, StorageOptions)}
 * so the storage-options plumbing and the Session sharing both live in
 * one place.
 *
 * <p>Sharing one {@link Session} across every {@code Dataset} on the node
 * keeps Lance's inverted-index and metadata caches native-side and node
 * scoped: a per-shard {@code Dataset} would allocate its own 6 GiB /
 * 1 GiB caches (the Lance defaults) on demand, so 200 shards of the
 * same table could drive the resident set above 100 GiB even though
 * JVM heap stayed at its configured maximum. The shared Session caps
 * the two caches to the node-level limits configured by
 * {@code lance.native_memory.limit}, and {@link Session#sizeBytes()}
 * exposes the current usage to the {@code lance_native} circuit breaker.
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
     * How the installed Session's index cache is sized: the capacity
     * handed to Lance, the shard count Lance derives from it and the
     * per-shard share that bounds the heaviest admissible entry. Set
     * together with {@link #SESSION} and cleared with it; read by the
     * stats endpoint and by attach, which warns when a table's inverted
     * index is heavier than the share.
     */
    private static volatile NativeMemoryLimit.IndexCacheSizing INDEX_CACHE_SIZING;

    private LanceRegistry() {}

    public static BufferAllocator allocator() {
        return ALLOCATOR;
    }

    /**
     * Install a node-scoped {@link Session} with the given cache sizes.
     * If a Session is already installed (e.g. an integration test
     * framework restart within the same JVM) the existing Session is
     * closed first so its native cache is released before the new one is
     * built. The index cache capacity is handed to Lance as is; the shard
     * count and share recorded for it describe what Lance does with that
     * capacity on this node's CPU count.
     *
     * @param indexCacheBytes    upper bound of the shared index cache
     * @param metadataCacheBytes upper bound of the shared metadata cache
     */
    public static synchronized void initSession(long indexCacheBytes, long metadataCacheBytes) {
        initSession(NativeMemoryLimit.IndexCacheSizing.ofCapacity(indexCacheBytes, NativeMemoryLimit.availableCpus()), metadataCacheBytes);
    }

    /**
     * Install a node-scoped {@link Session} whose index cache capacity
     * is {@code sizing.capacityBytes()}. Called once from
     * {@code LancePlugin.createComponents} with the sizing chosen by
     * {@link NativeMemoryLimit#sizeIndexCache}.
     *
     * @param sizing             index cache capacity and its shard layout
     * @param metadataCacheBytes upper bound of the shared metadata cache
     */
    public static synchronized void initSession(NativeMemoryLimit.IndexCacheSizing sizing, long metadataCacheBytes) {
        if (SESSION != null && !SESSION.isClosed()) {
            SESSION.close();
        }
        SESSION = Session.builder().indexCacheSizeBytes(sizing.capacityBytes()).metadataCacheSizeBytes(metadataCacheBytes).build();
        INDEX_CACHE_SIZING = sizing;
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
        INDEX_CACHE_SIZING = null;
    }

    /**
     * Index cache sizing of the installed Session, or {@code null} when
     * no Session is installed.
     */
    public static NativeMemoryLimit.IndexCacheSizing indexCacheSizing() {
        return INDEX_CACHE_SIZING;
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
     * appends. An index pinned this way carries {@code index.lance.version},
     * which keeps it out of the freshness checks (see
     * {@code LanceIndexFreshnessService}) so refresh does not race with a
     * manifest advance.
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

    /**
     * Resolve a Lance tag to the manifest version it currently points at.
     * Tags live in the table's {@code _refs} directory rather than in any
     * one manifest, so the table is opened at its latest version first and
     * {@code Dataset.tags().getVersion(tag)} is read from there. Callers
     * then pass the returned version through
     * {@link #openDataset(String, StorageOptions, java.util.Optional)} to
     * read the tagged snapshot; this keeps tag following a two step
     * "resolve, then open at version" so the version-pinned open path is
     * the only place that checks out a specific manifest.
     *
     * <p>Propagates whatever Lance throws for an unknown tag so the caller
     * can surface the Lance message to the operator.
     */
    public static long resolveTagVersion(String uri, StorageOptions storageOptions, String tag) {
        try (Dataset latest = openDataset(uri, storageOptions)) {
            return latest.tags().getVersion(tag);
        }
    }
}
