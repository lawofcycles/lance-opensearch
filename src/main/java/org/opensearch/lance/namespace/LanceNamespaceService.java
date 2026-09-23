/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * The RFC's namespace registration.
 *
 * <p>A registered catalog is polled at a configurable cadence via the Lance
 * Namespace API. Tables in the catalog surface as OpenSearch indexes
 * automatically, with mappings and shard counts derived from the Lance
 * schema. Version changes propagate through a shard-level refresh that
 * swaps the reader without closing the index. The runtime handle behind
 * each registration is a {@link LanceNamespace}: {@code DirectoryNamespace}
 * for filesystem catalogs, {@code RestNamespace} for REST catalogs, and
 * the Glue, Iceberg REST, Polaris and Unity implementations for those
 * catalog servers, all built by {@link LanceNamespaceFactory}.
 */
public final class LanceNamespaceService {

    private static final Logger LOG = LogManager.getLogger(LanceNamespaceService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final TimeValue cadence;
    private final long builderMaxRows;
    private final AllowedTableRoots allowedRoots;
    /**
     * Per-node cache of the runtime {@link LanceNamespace} handles
     * keyed by registration name, each wrapped in a
     * {@link LanceNamespaceHandle} so a removed registration's native
     * release waits for the poll or preview call still using it.
     * Rebuilt from cluster state on every
     * {@link #onClusterStateChanged} callback so a fresh node that
     * joins mid-life still sees the registrations that were already
     * in the cluster's {@link LanceNamespaceMetadata}.
     */
    private final Map<String, LanceNamespaceHandle> namespaceCache = new ConcurrentHashMap<>();
    /**
     * Registration names whose catalog is currently unusable, mapped
     * to the initialise or poll error. An entry here surfaces as
     * {@code "status": "unavailable"} in the GET listing and is
     * retried on every poll; success removes it.
     */
    private final Map<String, String> unavailable = new ConcurrentHashMap<>();
    /** Names whose initialise failure has been warned about, so the retry loop logs once. */
    private final Set<String> warnedInitFailure = ConcurrentHashMap.newKeySet();
    /** "name:index" pairs whose catalog location fell outside the allowlist, warned once. */
    private final Set<String> warnedDisallowedLocation = ConcurrentHashMap.newKeySet();
    /** Serialises handle construction so a poll and an applier tick do not double-initialise. */
    private final Object initLock = new Object();
    private final Map<String, Long> servedVersions = new ConcurrentHashMap<>();
    // Index names created via /_lance/attach along with the absolute Lance
    // table path and storage_options they point at. Tracked here so poll()
    // can extend append coverage to attach-only indexes and not just
    // namespace-registered tables; otherwise an append to a table whose
    // index came from attach would not surface through _search until
    // the operator refreshed manually.
    private final Map<String, AttachedIndex> attachedIndexes = new ConcurrentHashMap<>();
    // Index names we've already flagged as unowned, so the poll doesn't shout
    // the same warning every ten seconds. Cleared if the collision resolves.
    private final Set<String> warnedUnowned = ConcurrentHashMap.newKeySet();
    // Lance-backed indexes found in cluster state whose table could not be
    // opened when the poll tried to adopt them. Warned once per index; the
    // entry is dropped as soon as a later poll adopts the index or the
    // index leaves cluster state.
    private final Set<String> warnedUnreachableAdopt = ConcurrentHashMap.newKeySet();
    // Track which indexes we've already told the operator that the
    // `wait` uncovered-fragment policy is a no-op today. Set once per
    // index for the lifetime of the plugin instance; a poll every few
    // seconds would otherwise flood the log.
    private final Set<String> warnedWaitPolicy = ConcurrentHashMap.newKeySet();
    /** Tombstone bookkeeping and grace check for the re-surface guard. */
    private final LanceResurfaceGuard resurfaceGuard;
    /** Schema drift detection and mapping override rewriting. */
    private final LanceSchemaDriftDetector driftDetector;
    /** Adoption of Lance-backed indexes the tracking maps do not know yet. */
    private final LanceIndexAdopter indexAdopter;
    private final ThreadPool threadPool;
    /** Fragment path snapshot cache to retire from, or {@code null}. */
    private final LanceWarmCache warmCache;

    public LanceNamespaceService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        TimeValue cadence,
        long builderMaxRows
    ) {
        this(client, clusterService, threadPool, cadence, builderMaxRows, TimeValue.timeValueHours(1));
    }

    public LanceNamespaceService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        TimeValue cadence,
        long builderMaxRows,
        TimeValue resurfaceGrace
    ) {
        this(client, clusterService, threadPool, cadence, builderMaxRows, resurfaceGrace, null, new AllowedTableRoots(List.of()));
    }

    public LanceNamespaceService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        TimeValue cadence,
        long builderMaxRows,
        TimeValue resurfaceGrace,
        LanceWarmCache warmCache
    ) {
        this(client, clusterService, threadPool, cadence, builderMaxRows, resurfaceGrace, warmCache, new AllowedTableRoots(List.of()));
    }

    /**
     * @param warmCache    fragment path snapshot cache to retire entries
     *                     from when a table moves to a new version or an
     *                     index is deleted; {@code null} when there is none
     *                     (tests)
     * @param allowedRoots the {@code lance.allowed_table_roots} allowlist,
     *                     applied to the table locations rest / glue
     *                     catalogs return before their tables surface
     */
    public LanceNamespaceService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        TimeValue cadence,
        long builderMaxRows,
        TimeValue resurfaceGrace,
        LanceWarmCache warmCache,
        AllowedTableRoots allowedRoots
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.cadence = cadence;
        this.builderMaxRows = builderMaxRows;
        this.warmCache = warmCache;
        this.allowedRoots = allowedRoots;
        this.resurfaceGuard = new LanceResurfaceGuard(resurfaceGrace);
        this.driftDetector = new LanceSchemaDriftDetector(client);
        this.indexAdopter = new LanceIndexAdopter(
            servedVersions,
            attachedIndexes,
            warnedUnreachableAdopt,
            warnedUnowned,
            this.resurfaceGuard
        );
        // The applier listener keeps the tombstone bookkeeping current
        // and releases handles of removed registrations; the poll
        // builds handles itself on the generic pool. addListener
        // returns immediately; the listener body reads whatever state
        // is current when the applier fires.
        clusterService.addListener(this::onClusterStateChanged);
        threadPool.scheduleWithFixedDelay(this::poll, cadence, ThreadPool.Names.GENERIC);
    }

    /**
     * Reactive setter for the dynamic
     * {@code lance.namespace.resurface_guard_grace} node setting. Zero
     * or negative disables the guard (poll re-surfaces immediately).
     */
    public void setResurfaceGrace(TimeValue newGrace) {
        resurfaceGuard.setGrace(newGrace);
    }

    /**
     * Reconcile the per-node bookkeeping against the cluster's
     * {@link LanceNamespaceMetadata}. Called from the cluster state
     * applier on every state that touches metadata. The applier thread
     * must never block on I/O — a stalled applier delays every cluster
     * state update on the node — so this method only records tombstones
     * and drops bookkeeping for removed registrations; the runtime
     * handles are built lazily off this thread (see
     * {@link #ensureHandle}), and a removed handle's native release is
     * handed to the generic pool.
     */
    private void onClusterStateChanged(ClusterChangedEvent event) {
        if (!event.metadataChanged()) {
            return;
        }
        // Tombstone bookkeeping for the re-surface guard: any index that
        // disappeared from cluster state since the previous applier tick
        // gets a tombstone entry if it was a Lance-backed index. Ordinary
        // OpenSearch indexes are skipped. Additions and mutations do not
        // touch the map here; the poll cycle removes an entry once the
        // grace period elapses or the operator sets the grace to zero.
        if (event.previousState() != null && event.previousState().metadata() != null) {
            org.opensearch.cluster.metadata.Metadata previous = event.previousState().metadata();
            org.opensearch.cluster.metadata.Metadata current = event.state().metadata();
            long now = System.currentTimeMillis();
            for (String prevIndex : previous.indices().keySet()) {
                if (current.hasIndex(prevIndex)) {
                    continue;
                }
                org.opensearch.cluster.metadata.IndexMetadata prevMeta = previous.index(prevIndex);
                if (prevMeta == null) {
                    continue;
                }
                String table = prevMeta.getSettings().get(LanceEngineFactory.TABLE_SETTING);
                if (table == null || table.isEmpty()) {
                    continue;
                }
                resurfaceGuard.recordTombstone(prevIndex, table, now);
                // Also stop tracking the served version and the
                // attached-index bookkeeping so the delete really looks
                // "gone" to the poll cycle and to attach retries.
                servedVersions.remove(prevIndex);
                attachedIndexes.remove(prevIndex);
                warnedUnowned.remove(prevIndex);
                warnedUnreachableAdopt.remove(prevIndex);
                driftDetector.forgetIndex(prevIndex);
                warnedWaitPolicy.remove(prevIndex);
                // The fragment path keys its snapshots on the index uuid,
                // so nothing will ask for them again; close them as soon
                // as the requests that hold them finish. Closing a
                // snapshot releases a Lance dataset and Arrow vectors,
                // which does not belong on the cluster state applier
                // thread, so hand it to the generic pool.
                if (warmCache != null) {
                    String deletedUuid = prevMeta.getIndexUUID();
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> warmCache.retireAll(deletedUuid));
                }
            }
        }
        LanceNamespaceMetadata metadata = currentMetadata(event.state());
        Set<String> desired = new java.util.HashSet<>(metadata.entries().size());
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            desired.add(entry.name());
        }
        // Drop cache entries for namespaces the cluster removed. The
        // handles are not built here: DirectoryNamespace.initialize reads
        // the manifest table from storage (a network round trip for
        // object-store roots), which must not run on the applier thread;
        // the poll and the tables preview build handles on the generic
        // pool through ensureHandle instead. Closing a handle releases
        // native resources and waits for the poll or preview call that
        // may still be using it, so that leaves the applier thread too.
        for (String name : namespaceCache.keySet()) {
            if (desired.contains(name)) {
                continue;
            }
            LanceNamespaceHandle removed = namespaceCache.remove(name);
            if (removed != null) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> closeQuietly(name, removed));
            }
        }
        unavailable.keySet().removeIf(name -> !desired.contains(name));
        warnedInitFailure.removeIf(name -> !desired.contains(name));
    }

    /**
     * Return the runtime handle for {@code entry}, building and
     * initialising it on first use. An {@code initialize} failure (bad
     * credentials, unreachable endpoint, missing implementation) is
     * recorded in {@link #unavailable}, warned once, and retried on
     * the next call — every poll cycle goes through here, so a
     * registration that failed to initialise keeps being retried at
     * the poll cadence.
     *
     * <p>Callers run on the generic pool (the poll's schedule and the
     * tables preview's fork), never on the cluster state applier
     * thread, because {@code initialize} is not free of I/O for every
     * implementation. Per implementation: DirectoryNamespace builds its
     * object store and opens the manifest table from storage when
     * manifest support is on (its default) — a network round trip for
     * object-store roots; RestNamespace, IcebergNamespace,
     * PolarisNamespace and UnityNamespace only construct their HTTP
     * client without sending a request; GlueNamespace only builds the
     * AWS SDK client, whose credential providers resolve lazily on the
     * first call.
     */
    private LanceNamespaceHandle ensureHandle(LanceNamespaceMetadata.Entry entry) {
        LanceNamespaceHandle cached = namespaceCache.get(entry.name());
        if (cached != null) {
            return cached;
        }
        synchronized (initLock) {
            cached = namespaceCache.get(entry.name());
            if (cached != null) {
                return cached;
            }
            try {
                LanceNamespaceHandle created = new LanceNamespaceHandle(LanceNamespaceFactory.create(entry, LanceRegistry.allocator()));
                namespaceCache.put(entry.name(), created);
                unavailable.remove(entry.name());
                warnedInitFailure.remove(entry.name());
                return created;
            } catch (Exception e) {
                String message = e.getMessage() == null ? e.toString() : e.getMessage();
                unavailable.put(entry.name(), message);
                if (warnedInitFailure.add(entry.name())) {
                    LOG.warn("failed to initialise namespace {} (type {}); retrying on every poll", entry.name(), entry.type(), e);
                }
                return null;
            }
        }
    }

    private static void closeQuietly(String name, LanceNamespaceHandle handle) {
        try {
            handle.close();
        } catch (Exception e) {
            LOG.debug("closing namespace handle {} failed: {}", name, e.getMessage());
        }
    }

    static LanceNamespaceMetadata currentMetadata(ClusterState state) {
        LanceNamespaceMetadata metadata = state.metadata().custom(LanceNamespaceMetadata.TYPE);
        return metadata == null ? LanceNamespaceMetadata.EMPTY : metadata;
    }

    /** Namespace poll cadence in effect for this service. */
    public TimeValue cadence() {
        return cadence;
    }

    public List<String> namespaces() {
        LanceNamespaceMetadata metadata = currentMetadata(clusterService.state());
        List<String> names = new ArrayList<>(metadata.entries().size());
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            names.add(entry.name());
        }
        return List.copyOf(names);
    }

    /**
     * List the tables the poll cycle would surface from the namespace
     * registered under {@code identifier} (a registration name, or a
     * directory registration's path). Returns an empty {@link Optional}
     * when nothing is registered under the identifier (or the handle
     * failed to initialise on this node), a populated
     * set otherwise. Table names come from the catalog's
     * {@code listTables} without any {@code .lance} suffix or scheme
     * prefix — matching the form the surface path uses.
     *
     * <p>Read-only: does not create, delete, or advance anything. The
     * caller can use this to preview what the next poll would do (for
     * example after a fresh namespace registration on a large directory),
     * or to spot tables the poller failed to surface due to a name
     * clash with an existing OpenSearch index.
     */
    public Optional<Set<String>> listTables(String identifier) throws Exception {
        LanceNamespaceMetadata.Entry entry = currentMetadata(clusterService.state()).findByIdentifier(identifier);
        if (entry == null) {
            return Optional.empty();
        }
        // Build the handle on demand: the transport action forks this
        // call to the generic pool, and only the poll (cluster manager
        // only) builds handles otherwise, so a preview served by a
        // follower node cannot rely on a pre-built cache entry.
        LanceNamespaceHandle handle = ensureHandle(entry);
        if (handle == null) {
            return Optional.empty();
        }
        Set<String> names = new TreeSet<>();
        for (LanceCatalogEnumerator.CatalogTable table : handle.call(
            namespace -> LanceCatalogEnumerator.enumerateTables(namespace, entry)
        )) {
            names.add(table.name());
        }
        return Optional.of(names);
    }

    /**
     * The GET listing: one row per registration with its redacted
     * config and this node's view of the catalog's availability.
     */
    public List<LanceNamespaceListResponse.NamespaceInfo> namespaceInfos() {
        LanceNamespaceMetadata metadata = currentMetadata(clusterService.state());
        List<LanceNamespaceListResponse.NamespaceInfo> infos = new ArrayList<>(metadata.entries().size());
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            String path = LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type()) ? entry.rootUri() : null;
            infos.add(
                new LanceNamespaceListResponse.NamespaceInfo(
                    entry.name(),
                    entry.type(),
                    path,
                    entry.redactedConfig(),
                    unavailable.get(entry.name())
                )
            );
        }
        return List.copyOf(infos);
    }

    // Package-private so tests can drive a poll cycle synchronously
    // instead of waiting for the scheduled cadence.
    void poll() {
        // Skip poll cycles that fire before the applier delivers a
        // cluster state, which happens once at startup. Reading state()
        // then would trip the AssertionError inside
        // ClusterApplierService instead of returning gracefully.
        if (clusterService.lifecycleState() != org.opensearch.common.lifecycle.Lifecycle.State.STARTED) {
            return;
        }
        // Poll only on the cluster manager. In a multi-node cluster
        // every node would otherwise scan the shared namespaces and
        // race to CreateIndex the same table, which spikes state
        // update pressure and starves other traffic (unregister
        // ack timeouts, follower state application). Skipping the
        // poll on followers is safe because the manager surfaces
        // every discovered index into cluster state anyway.
        if (!clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
            return;
        }
        ClusterState state = clusterService.state();
        // The tracking maps live only in this node's memory, so any index
        // that carries index.lance.table in cluster state but is missing
        // from servedVersions has to be picked up again before the two
        // sync loops below run; otherwise those loops would treat it as a
        // name collision and skip it for good.
        indexAdopter.adoptUntrackedIndexes(state);
        LanceNamespaceMetadata metadata = currentMetadata(state);
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            LanceNamespaceHandle handle = ensureHandle(entry);
            if (handle == null) {
                // initialise failed; ensureHandle recorded the error and
                // will retry on the next poll.
                continue;
            }
            try {
                for (LanceCatalogEnumerator.CatalogTable table : handle.call(
                    namespace -> LanceCatalogEnumerator.enumerateTables(namespace, entry)
                )) {
                    syncCatalogTable(entry, handle, table);
                }
                unavailable.remove(entry.name());
            } catch (Exception e) {
                // A listing failure (unreachable endpoint, revoked
                // credentials after a successful initialise) marks the
                // registration unavailable until a poll succeeds again.
                String message = e.getMessage() == null ? e.toString() : e.getMessage();
                unavailable.put(entry.name(), message);
                LOG.warn("namespace poll failed for {}", entry.name(), e);
            }
        }
        // Sync attach-created indexes so append fragments surface on the
        // same schedule as namespace-registered tables. attach records the
        // (indexName -> tablePath) pair; the sync path is the same, just
        // without the rootUri / tableName join namespace tables use. The
        // tag is a Dynamic setting the operator can rewrite on a running
        // index, so it is re-read from cluster state each cycle rather
        // than taken from the value captured at attach time; the cached
        // entry is refreshed too so a later restart replays the current
        // tag through adoption.
        for (Map.Entry<String, AttachedIndex> entry : attachedIndexes.entrySet()) {
            AttachedIndex attached = entry.getValue();
            String indexName = entry.getKey();
            String currentTag = attached.tag;
            IndexMetadata attachedMetadata = state.metadata().index(indexName);
            if (attachedMetadata != null) {
                String tagSetting = attachedMetadata.getSettings().get(LanceEngineFactory.TAG_SETTING, "");
                currentTag = tagSetting.isEmpty() ? null : tagSetting;
                if (!java.util.Objects.equals(currentTag, attached.tag)) {
                    attachedIndexes.put(indexName, new AttachedIndex(attached.tablePath, attached.storageOptions, currentTag));
                }
            }
            try {
                syncAttachedTable(indexName, attached.tablePath, attached.storageOptions, currentTag);
            } catch (Exception e) {
                LOG.warn("attach poll failed for index {} at {}", indexName, attached.tablePath, e);
            }
        }
    }

    /**
     * Sync one catalog table into the poll's index bookkeeping.
     *
     * <p>A directory registration builds the table path from its root
     * and the table name, exactly the shape adoption classification
     * and the persisted {@code index.lance.table} setting rely on. A
     * rest or glue registration asks the catalog itself through
     * {@code describeTable}; the returned location is validated
     * against {@code lance.allowed_table_roots} before anything
     * surfaces, because for these types the register call had no root
     * the allowlist could gate.
     */
    private void syncCatalogTable(
        LanceNamespaceMetadata.Entry entry,
        LanceNamespaceHandle handle,
        LanceCatalogEnumerator.CatalogTable table
    ) {
        String indexName = table.name();
        if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type())) {
            runSyncCycle(entry.rootUri() + "/" + indexName + ".lance", indexName, entry.storageOptions(), null, entry.overridesJson());
            return;
        }
        DescribeTableResponse described;
        try {
            described = handle.call(namespace -> namespace.describeTable(new DescribeTableRequest().id(table.id())));
        } catch (Exception e) {
            LOG.warn("describe_table failed for {} in namespace {}: {}", indexName, entry.name(), e.getMessage());
            return;
        }
        String location = LanceCatalogEnumerator.tableLocation(described);
        if (location == null) {
            LOG.warn("catalog {} returned no location for table {}; skipping", entry.name(), indexName);
            return;
        }
        if (!allowedRoots.allows(location)) {
            if (warnedDisallowedLocation.add(entry.name() + ":" + indexName)) {
                LOG.warn(
                    "table {} from namespace {} resolves to {}, outside the configured lance.allowed_table_roots; skipping",
                    indexName,
                    entry.name(),
                    location
                );
            }
            return;
        }
        warnedDisallowedLocation.remove(entry.name() + ":" + indexName);
        runSyncCycle(
            location,
            indexName,
            LanceCatalogEnumerator.mergeStorageOptions(described.getStorageOptions(), entry.storageOptions()),
            null,
            entry.overridesJson()
        );
    }

    /** Visible for tests: whether the disallowed-location warning for this namespace and index has fired. */
    boolean hasWarnedDisallowedLocation(String namespaceName, String indexName) {
        return warnedDisallowedLocation.contains(namespaceName + ":" + indexName);
    }

    /** Visible for tests: how many distinct disallowed-location warnings have fired. */
    int disallowedLocationWarningCount() {
        return warnedDisallowedLocation.size();
    }

    // Attach-created indexes carry the fully-qualified table path already,
    // so the rootUri / tableName join namespace tables use doesn't apply.
    // Everything downstream of the path resolution is identical, except
    // that a tag-following index compares against the version its tag
    // resolves to instead of the latest manifest.
    private void syncAttachedTable(String indexName, String tablePath, StorageOptions storageOptions, String tag) {
        // An attach-created index that got deleted and resurfaces after
        // the grace period carries no overrides: they lived in the
        // deleted index's settings, and attach is where the operator
        // declares them again.
        runSyncCycle(tablePath, indexName, storageOptions, tag, "");
    }

    private void runSyncCycle(String table, String indexName, StorageOptions storageOptions, String tag, String surfaceOverridesJson) {
        try {
            boolean exists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
            if (!exists) {
                // Re-surface guard: if this index was recently deleted
                // through OpenSearch, honour the operator's intent and
                // skip the surface until the grace period expires.
                // Grace <= 0 disables the guard and every poll recreates
                // the index unconditionally.
                if (resurfaceGuard.shouldSkipSurface(indexName, table)) {
                    return;
                }
                surface(indexName, table, storageOptions, surfaceOverridesJson);
                return;
            }
            Long served = servedVersions.get(indexName);
            if (served == null) {
                // The index name already existed before we saw the table: a
                // classic OpenSearch index, or another namespace beat us to
                // the name. Recoverable with operator action, so log once
                // per index instead of silently skipping every poll.
                if (warnedUnowned.add(indexName)) {
                    LOG.warn("skipping table {}: index {} exists but is not tracked by this namespace (name collision)", table, indexName);
                }
                return;
            }
            // In case the collision has resolved (index deleted and re-created by
            // us), allow future warnings again.
            warnedUnowned.remove(indexName);
            String policy = readUncoveredFragmentPolicy(indexName);
            // `target` is the version the index should serve after this
            // cycle: the latest manifest for a latest-following index, or
            // the version the tag points at for a tag-following one. The
            // latest dataset is opened in both cases because tags are read
            // from the table's refs, not from a particular manifest.
            long target;
            boolean moved;
            String rederivedMappingJson = null;
            LanceOverrides storedOverrides = LanceOverrides.EMPTY;
            try (Dataset latestDataset = LanceRegistry.openDataset(table, storageOptions)) {
                long latest = latestDataset.version();
                // An index adopted from cluster state serves -1 until this
                // point, so its first cycle always counts as a move and
                // refreshes the reader once.
                if (tag == null) {
                    target = latest;
                    moved = target > served;
                } else {
                    target = latestDataset.tags().getVersion(tag);
                    // A tag can move backwards as well as forwards, so any
                    // difference from the served version is a move.
                    moved = target != served;
                }
                if (moved) {
                    // The RFC's Mapping interface states the mapping is re-derived at
                    // every checkout. We derive first so the builder only touches
                    // columns that derived to lance_text; keyword columns stay untouched.
                    // Re-apply any attach-body overrides captured on shard creation
                    // so the re-derived mapping preserves type overrides and
                    // multi-field declarations across manifest version advance;
                    // without this the mapping would drop back to the default
                    // derivation and a caller querying body.raw would suddenly
                    // see 400 no-such-field errors. Lenient: a column an
                    // override names may have been dropped or retyped by the
                    // writer; its override is skipped this cycle but stays in
                    // the setting, so it applies again if a later manifest
                    // restores the column.
                    IndexMetadata rederivationMetadata = clusterService.state().metadata().index(indexName);
                    storedOverrides = rederivationMetadata == null
                        ? LanceOverrides.EMPTY
                        : LanceOverrides.of(rederivationMetadata.getSettings());
                    boolean nodeLocal = rederivationMetadata != null && LanceLocalClones.isNodeLocal(rederivationMetadata.getSettings());
                    if (nodeLocal) {
                        // node_local: the search structures live in per-node
                        // clones the source never carries, so a derivation
                        // from the source would flip every clone-built
                        // lance_text column back to keyword (and trigger the
                        // rebuild loop below on every cycle). The build
                        // action maintains the mapping from the clones; the
                        // poll only advances the reader.
                        rederivedMappingJson = null;
                    } else if (target == latest) {
                        storedOverrides = driftDetector.rewriteOverridesForSchemaDrift(
                            indexName,
                            storedOverrides,
                            latestDataset.getLanceSchema()
                        );
                        RestAttachAction.Derivation derivation = RestAttachAction.derive(latestDataset, storedOverrides, true);
                        rederivedMappingJson = derivation.mappingJson();
                        driftDetector.warnOnLanceFieldRename(indexName, latestDataset.getLanceSchema());
                    } else {
                        // The tag points at an older manifest: derive from
                        // that snapshot so the mapping matches the schema
                        // the shard is about to read.
                        try (Dataset tagged = LanceRegistry.openDataset(table, storageOptions, Optional.of(target))) {
                            storedOverrides = driftDetector.rewriteOverridesForSchemaDrift(
                                indexName,
                                storedOverrides,
                                tagged.getLanceSchema()
                            );
                            RestAttachAction.Derivation derivation = RestAttachAction.derive(tagged, storedOverrides, true);
                            rederivedMappingJson = derivation.mappingJson();
                            driftDetector.warnOnLanceFieldRename(indexName, tagged.getLanceSchema());
                        }
                    }
                    if ("wait".equals(policy)) {
                        // `wait` is accepted but converges with the
                        // immediate branch: the plugin never writes to a
                        // user table, so folding appended fragments into
                        // the existing indexes is left to the table's
                        // writer or to POST /_lance/build_indexes/{index}.
                        // Log once per index so an operator who set `wait`
                        // on purpose sees why nothing is happening.
                        warnDeprecatedWaitPolicyOnce(indexName);
                    }
                    // Either branch exposes the new version at once and
                    // lets Lance fall back to scan evaluation on any
                    // fragment the existing indexes have not caught up
                    // to. Lance's scanner produces a mixed plan for FTS
                    // and knn (index for covered fragments, flat scan for
                    // uncovered, unioned) so an incremental append does
                    // not slow down queries on covered fragments.
                }
            }
            if (moved) {
                if (tag == null) {
                    LOG.info("table {} moved to version {} (serving {}), refreshing {}", table, target, served, indexName);
                } else {
                    LOG.info(
                        "tag {} on table {} now points at version {} (serving {}), refreshing {}",
                        tag,
                        table,
                        target,
                        served,
                        indexName
                    );
                }
                if (rederivedMappingJson != null) {
                    try {
                        client.admin()
                            .indices()
                            .preparePutMapping(indexName)
                            .setSource(rederivedMappingJson, MediaTypeRegistry.JSON)
                            .execute()
                            .actionGet();
                    } catch (Exception e) {
                        String message = e.getMessage() == null ? "" : e.getMessage();
                        if (message.contains("cannot be changed from type")) {
                            // A Utf8 column has flipped between keyword and
                            // lance_text after the user created / dropped an
                            // FTS index on the Lance side. PutMapping refuses
                            // the type change, but leaving the stale mapping
                            // in place makes the column silently unsearchable.
                            // Rebuild the index (delete + recreate with the
                            // new mapping) so the reader sees the correct
                            // field type. The underlying Lance table keeps
                            // its data intact, so nothing is lost.
                            LOG.warn(
                                "mapping re-derivation for {} at version {} hit a keyword <-> lance_text type change ({}); "
                                    + "rebuilding the OpenSearch index (Lance data is untouched)",
                                indexName,
                                target,
                                message
                            );
                            try {
                                client.admin()
                                    .indices()
                                    .delete(new org.opensearch.action.admin.indices.delete.DeleteIndexRequest(indexName))
                                    .actionGet();
                                servedVersions.remove(indexName);
                                // The recreate must keep the index's own
                                // overrides; without them the rebuilt
                                // mapping would drop the operator's type
                                // and sub-field declarations.
                                surface(indexName, table, storageOptions, storedOverrides.toJson());
                                return;
                            } catch (Exception rebuild) {
                                LOG.warn("rebuild after type change failed for {}: {}", indexName, rebuild.getMessage());
                            }
                        } else {
                            LOG.warn("mapping re-derivation failed for {} at version {}: {}", indexName, target, message);
                        }
                    }
                }
                client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
                servedVersions.put(indexName, target);
                // Fragment path requests key on the new version from now
                // on; let the snapshots of the version left behind close
                // as soon as no request holds them instead of waiting for
                // the cache's size bound.
                if (warmCache != null) {
                    IndexMetadata movedMetadata = clusterService.state().metadata().index(indexName);
                    if (movedMetadata != null) {
                        warmCache.retire(movedMetadata.getIndexUUID(), target);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("sync failed for table {}", table, e);
        }
    }

    private void surface(String indexName, String table, StorageOptions storageOptions, String overridesJson) throws Exception {
        // Overrides apply leniently: the register call's override list
        // covers every table under the root, so a column this table
        // lacks is skipped (visible once at debug) while the full list
        // is persisted in the index settings, ready for a manifest that
        // adds the column.
        LanceOverrides overrides = LanceOverrides.parse(overridesJson);
        RestAttachAction.Derivation derivation;
        try (Dataset dataset = LanceRegistry.openDataset(table, storageOptions)) {
            // Derive first so the CreateIndex settings and mapping reflect
            // the current Lance schema. Automatic index creation is off by
            // default; operators build indexes explicitly through
            // POST /_lance/build_indexes.
            derivation = RestAttachAction.derive(dataset, overrides, true);
        }
        if (!derivation.notes().isEmpty() && LOG.isDebugEnabled()) {
            for (String note : derivation.notes()) {
                LOG.debug("surface of {} at {}: {}", indexName, table, note);
            }
        }
        // Fire the CreateIndex asynchronously so a red shard on this table
        // does not block the poll thread for 30 seconds waiting for ack.
        // Every other table in the same namespace would otherwise wait
        // behind that block.
        final long version = derivation.version();
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, derivation.keyFieldType());
        if (!derivation.overridesJson().isEmpty()) {
            settings.put(LanceEngineFactory.OVERRIDES_SETTING, derivation.overridesJson());
        }
        storageOptions.writeToSettings(settings);
        // LanceCreateIndexActionFilter blocks user PUT /{index} with
        // index.lance.table in settings. This surface call is
        // plugin-internal so stamp the marker header before dispatch;
        // stashContext preserves the caller's headers for whatever
        // scheduled the poll.
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson()),
                    new ActionListener<org.opensearch.action.admin.indices.create.CreateIndexResponse>() {
                        @Override
                        public void onResponse(org.opensearch.action.admin.indices.create.CreateIndexResponse response) {
                            servedVersions.put(indexName, version);
                            LOG.info("surfaced table {} as index {} (version {})", table, indexName, version);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // ResourceAlreadyExistsException means another node
                            // (or an earlier poll) already surfaced the table;
                            // the syncTable path will pick it up next cycle.
                            Throwable cursor = e;
                            while (cursor != null) {
                                if (cursor instanceof org.opensearch.ResourceAlreadyExistsException) {
                                    LOG.debug("surface for {} raced with an existing index", indexName);
                                    return;
                                }
                                cursor = cursor.getCause();
                            }
                            LOG.warn("surface failed for {} at version {}: {}", indexName, version, e.getMessage());
                        }
                    }
                );
        }
    }

    void recordServedVersion(String indexName, long version) {
        servedVersions.put(indexName, version);
    }

    /**
     * Records an attach-created index and its Lance table path so poll()
     * can pick up appends for it, just as it would for a namespace-registered
     * table. Idempotent: repeated calls with the same (indexName, tablePath)
     * are a no-op beyond overwriting the served version.
     */
    public void registerAttachedIndex(String indexName, String tablePath, long version) {
        registerAttachedIndex(indexName, tablePath, version, StorageOptions.empty());
    }

    public void registerAttachedIndex(String indexName, String tablePath, long version, StorageOptions storageOptions) {
        registerAttachedIndex(indexName, tablePath, version, storageOptions, null);
    }

    /**
     * Variant for indexes that follow a Lance tag. {@code tag} is the tag
     * name the poll re-resolves on every cycle, or {@code null} for an
     * index that follows the latest manifest. {@code version} is the
     * version currently served (the tag's version at attach time).
     */
    public void registerAttachedIndex(String indexName, String tablePath, long version, StorageOptions storageOptions, String tag) {
        attachedIndexes.put(indexName, new AttachedIndex(tablePath, storageOptions, tag));
        servedVersions.put(indexName, version);
    }

    private String readUncoveredFragmentPolicy(String indexName) {
        try {
            var state = client.admin().cluster().prepareState().execute().actionGet().getState();
            var metadata = state.metadata().index(indexName);
            if (metadata == null) {
                return "immediate";
            }
            return metadata.getSettings().get("index.lance.uncovered_fragment_policy", "immediate");
        } catch (Exception e) {
            LOG.warn("failed to read uncovered_fragment_policy for {}: {}", indexName, e.getMessage());
            return "immediate";
        }
    }

    private void warnDeprecatedWaitPolicyOnce(String indexName) {
        if (warnedWaitPolicy.add(indexName)) {
            LOG.info(
                "index [{}] has index.lance.uncovered_fragment_policy=wait, but the plugin no longer runs auto-optimize on the user's Lance table. "
                    + "Index maintenance is expected to happen outside OpenSearch (Python, Ray, Spark, or the Lance Java SDK) or via "
                    + "an explicit POST /_lance/build_indexes/{{index}} call. The wait value is accepted for a future async-optimize implementation.",
                indexName
            );
        }
    }

    /**
     * Poll bookkeeping for an attach-created index. {@code tag} is the
     * Lance tag the index follows, or {@code null} when it follows the
     * latest manifest.
     */
    record AttachedIndex(String tablePath, StorageOptions storageOptions, String tag) {
    }
}
