/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
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
     * keyed by registration name. Rebuilt from cluster state on every
     * {@link #onClusterStateChanged} callback so a fresh node that
     * joins mid-life still sees the registrations that were already
     * in the cluster's {@link LanceNamespaceMetadata}.
     */
    private final Map<String, LanceNamespace> namespaceCache = new ConcurrentHashMap<>();
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
    // Track rename warnings so a table that renamed the same field is not
    // logged on every poll. Keyed by "indexName:fieldId:oldName->newName" so
    // the same rename fires once, but a later re-rename still warns.
    private final Set<String> warnedRenamed = ConcurrentHashMap.newKeySet();
    // Track which indexes we've already told the operator that the
    // `wait` uncovered-fragment policy is a no-op today. Set once per
    // index for the lifetime of the plugin instance; a poll every few
    // seconds would otherwise flood the log.
    private final Set<String> warnedWaitPolicy = ConcurrentHashMap.newKeySet();
    /**
     * Deleted-index tombstones for the re-surface guard. Maps a
     * Lance-backed index name to the millisecond timestamp at which
     * the {@code DELETE /{index}} was observed on the cluster state.
     * The poll cycle consults this map before creating a new index
     * for a table it would otherwise surface; if the tombstone is
     * still within {@link #resurfaceGrace} the surface is skipped,
     * otherwise the entry is dropped and surfacing proceeds. Entries
     * only get added for indexes carrying {@code index.lance.table}
     * so plain OpenSearch indexes never accumulate here.
     */
    private final Map<String, Long> tombstones = new ConcurrentHashMap<>();
    /**
     * Current grace period. Held in an {@link AtomicReference} so a
     * dynamic setting update from {@link org.opensearch.lance.LancePlugin}
     * can atomically swap it in without racing against the poll cycle.
     */
    private final java.util.concurrent.atomic.AtomicReference<TimeValue> resurfaceGrace;
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
        this.resurfaceGrace = new java.util.concurrent.atomic.AtomicReference<>(resurfaceGrace);
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
        resurfaceGrace.set(newGrace);
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
                tombstones.put(prevIndex, now);
                // Also stop tracking the served version and the
                // attached-index bookkeeping so the delete really looks
                // "gone" to the poll cycle and to attach retries.
                servedVersions.remove(prevIndex);
                attachedIndexes.remove(prevIndex);
                warnedUnowned.remove(prevIndex);
                warnedUnreachableAdopt.remove(prevIndex);
                warnedRenamed.remove(prevIndex);
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
                LOG.info("recording resurface tombstone for deleted Lance-backed index {} (table {})", prevIndex, table);
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
        // native resources, so that leaves the applier thread too.
        for (String name : namespaceCache.keySet()) {
            if (desired.contains(name)) {
                continue;
            }
            LanceNamespace removed = namespaceCache.remove(name);
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
    private LanceNamespace ensureHandle(LanceNamespaceMetadata.Entry entry) {
        LanceNamespace cached = namespaceCache.get(entry.name());
        if (cached != null) {
            return cached;
        }
        synchronized (initLock) {
            cached = namespaceCache.get(entry.name());
            if (cached != null) {
                return cached;
            }
            try {
                LanceNamespace created = LanceNamespaceFactory.create(entry, LanceRegistry.allocator());
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

    private static void closeQuietly(String name, LanceNamespace handle) {
        if (handle instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.debug("closing namespace handle {} failed: {}", name, e.getMessage());
            }
        }
    }

    private static LanceNamespaceMetadata currentMetadata(ClusterState state) {
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
        LanceNamespace handle = ensureHandle(entry);
        if (handle == null) {
            return Optional.empty();
        }
        Set<String> names = new TreeSet<>();
        for (CatalogTable table : enumerateTables(handle, entry)) {
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

    /** One table the catalog names: its identifier path and its plain name (the last segment). */
    private record CatalogTable(List<String> id, String name) {
    }

    /**
     * Config key naming the warehouse (Iceberg REST) or catalog
     * (Polaris) the table walk starts from. Its dot-separated segments
     * become the leading levels of every namespace and table id the
     * poll sends, because those clients address everything under a
     * warehouse and reject listings without one. Read by the plugin,
     * passed through to {@code initialize} like any other config key.
     */
    static final String WAREHOUSE_CONFIG_KEY = "warehouse";

    /**
     * Config key bounding how many namespace levels below the walk's
     * root the poll descends when the root itself does not answer a
     * table listing. The default reaches Glue databases, single-level
     * Iceberg namespaces under a warehouse, and Unity's fixed
     * catalog.schema shape.
     */
    static final String MAX_NAMESPACE_DEPTH_CONFIG_KEY = "max_namespace_depth";

    static final int DEFAULT_MAX_NAMESPACE_DEPTH = 2;

    /**
     * Enumerate the catalog's tables through the {@link LanceNamespace}
     * interface. Implementations disagree on how the root namespace is
     * addressed: DirectoryNamespace accepts a request without an id,
     * RestNamespace requires an explicit (empty) id for the root, Glue
     * rejects a root table listing outright because tables live inside
     * databases, Iceberg REST and Polaris root every id at the
     * registration's {@code warehouse} config and hold tables in
     * multi-level namespaces below it, and Unity holds tables at the
     * fixed two-level {@code catalog.schema}. The chain below tries the
     * root shapes in turn and finally walks the namespace tree
     * depth-first, bounded by {@code max_namespace_depth}.
     */
    private static List<CatalogTable> enumerateTables(LanceNamespace handle, LanceNamespaceMetadata.Entry entry) throws Exception {
        List<String> root = walkRoot(entry);
        int maxDepth = maxNamespaceDepth(entry);
        Exception rootListingFailure = null;
        if (root.isEmpty()) {
            try {
                return tablesAt(handle.listTables(new ListTablesRequest()), root);
            } catch (Exception noIdFailure) {
                rootListingFailure = noIdFailure;
            }
        }
        try {
            return tablesAt(handle.listTables(new ListTablesRequest().id(root)), root);
        } catch (Exception explicitIdFailure) {
            if (rootListingFailure == null) {
                rootListingFailure = explicitIdFailure;
            }
        }
        List<CatalogTable> tables = new ArrayList<>();
        Set<List<String>> visited = new HashSet<>();
        visited.add(root);
        Exception[] firstTablesFailure = new Exception[1];
        try {
            walkNamespaces(handle, root, maxDepth, visited, tables, firstTablesFailure);
        } catch (Exception walkFailure) {
            // Neither root shape works and the walk cannot start; report
            // the root failure, which names the catalog's own error
            // rather than the fallback's.
            throw rootListingFailure;
        }
        if (tables.isEmpty() && firstTablesFailure[0] != null) {
            // Every namespace the walk reached refused its table listing.
            // An empty catalog answers empty listings instead, so this is
            // a real failure (revoked table permissions, wrong warehouse)
            // and the registration should show as unavailable rather than
            // silently surfacing nothing.
            throw firstTablesFailure[0];
        }
        return tables;
    }

    /**
     * Depth-first walk over the namespaces below {@code parent},
     * collecting the tables of every namespace that answers a table
     * listing. A child that refuses its table listing is not a failure
     * on its own: Unity's first level (the catalog) and Iceberg's
     * warehouse level hold no tables and reject the request shape, and
     * the tables live one level further down. The first such refusal is
     * recorded so the caller can tell an empty catalog from one that
     * refused everything. A child that cannot list its own namespaces
     * is treated as a leaf.
     */
    private static void walkNamespaces(
        LanceNamespace handle,
        List<String> parent,
        int remainingDepth,
        Set<List<String>> visited,
        List<CatalogTable> tables,
        Exception[] firstTablesFailure
    ) throws Exception {
        if (remainingDepth <= 0) {
            return;
        }
        ListNamespacesResponse children = handle.listNamespaces(new ListNamespacesRequest().id(parent));
        if (children.getNamespaces() == null) {
            return;
        }
        for (String child : children.getNamespaces()) {
            List<String> childId = childId(parent, child);
            if (visited.add(childId) == false) {
                // Glue answers every listNamespaces with the full database
                // list regardless of the parent id; the visited set keeps
                // that from looping.
                continue;
            }
            try {
                tables.addAll(tablesAt(handle.listTables(new ListTablesRequest().id(childId)), childId));
            } catch (Exception tablesFailure) {
                if (firstTablesFailure[0] == null) {
                    firstTablesFailure[0] = tablesFailure;
                }
                LOG.debug("table listing at {} failed; descending: {}", childId, tablesFailure.getMessage());
            }
            if (remainingDepth > 1) {
                try {
                    walkNamespaces(handle, childId, remainingDepth - 1, visited, tables, firstTablesFailure);
                } catch (Exception childWalkFailure) {
                    LOG.debug("namespace listing below {} failed; treating it as a leaf: {}", childId, childWalkFailure.getMessage());
                }
            }
        }
    }

    /**
     * Build a child's full id from the parent id and the name the
     * catalog listed. Iceberg REST and Polaris return dot-joined full
     * paths rooted at the warehouse ({@code wh.ns1}); Unity and Glue
     * return the bare child name.
     */
    private static List<String> childId(List<String> parent, String child) {
        List<String> segments = List.of(child.split("\\."));
        if (segments.size() > parent.size() && segments.subList(0, parent.size()).equals(parent)) {
            return segments;
        }
        List<String> id = new ArrayList<>(parent.size() + 1);
        id.addAll(parent);
        id.add(child);
        return List.copyOf(id);
    }

    private static List<String> walkRoot(LanceNamespaceMetadata.Entry entry) {
        String warehouse = entry.config().get(WAREHOUSE_CONFIG_KEY);
        if (warehouse == null || warehouse.isEmpty()) {
            return List.of();
        }
        return List.of(warehouse.split("\\."));
    }

    private static int maxNamespaceDepth(LanceNamespaceMetadata.Entry entry) {
        String raw = entry.config().get(MAX_NAMESPACE_DEPTH_CONFIG_KEY);
        if (raw == null || raw.isEmpty()) {
            return DEFAULT_MAX_NAMESPACE_DEPTH;
        }
        int depth;
        try {
            depth = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + MAX_NAMESPACE_DEPTH_CONFIG_KEY + "] must be a positive integer, got [" + raw + "]");
        }
        if (depth < 1) {
            throw new IllegalArgumentException("[" + MAX_NAMESPACE_DEPTH_CONFIG_KEY + "] must be a positive integer, got [" + raw + "]");
        }
        return depth;
    }

    private static List<CatalogTable> tablesAt(ListTablesResponse response, List<String> namespaceId) {
        List<CatalogTable> tables = new ArrayList<>();
        if (response.getTables() != null) {
            for (String table : response.getTables()) {
                List<String> id = new ArrayList<>(namespaceId.size() + 1);
                id.addAll(namespaceId);
                id.add(table);
                tables.add(new CatalogTable(List.copyOf(id), table));
            }
        }
        return tables;
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
        adoptUntrackedIndexes(state);
        LanceNamespaceMetadata metadata = currentMetadata(state);
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            LanceNamespace handle = ensureHandle(entry);
            if (handle == null) {
                // initialise failed; ensureHandle recorded the error and
                // will retry on the next poll.
                continue;
            }
            try {
                for (CatalogTable table : enumerateTables(handle, entry)) {
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
    private void syncCatalogTable(LanceNamespaceMetadata.Entry entry, LanceNamespace handle, CatalogTable table) {
        String indexName = table.name();
        if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type())) {
            runSyncCycle(entry.rootUri() + "/" + indexName + ".lance", indexName, entry.storageOptions(), null, entry.overridesJson());
            return;
        }
        DescribeTableResponse described;
        try {
            described = handle.describeTable(new DescribeTableRequest().id(table.id()));
        } catch (Exception e) {
            LOG.warn("describe_table failed for {} in namespace {}: {}", indexName, entry.name(), e.getMessage());
            return;
        }
        String location = tableLocation(described);
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
            mergeStorageOptions(described.getStorageOptions(), entry.storageOptions()),
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

    /**
     * The single place a table location is read out of a
     * {@code DescribeTableResponse}, whichever implementation produced
     * it. Every catalog implementation maps its own location field into
     * the response's {@code location} before returning: directory and
     * REST set it natively, Glue reads its storage descriptor, Iceberg
     * REST copies the table metadata's location, Polaris the generic
     * table's base location, and Unity the table's storage location. A
     * trailing slash (Glue storage descriptors sometimes carry one) is
     * stripped so the value matches the path shape
     * {@code index.lance.table} persists.
     */
    static String tableLocation(DescribeTableResponse response) {
        if (response == null) {
            return null;
        }
        String location = response.getLocation();
        if (location == null || location.isEmpty()) {
            return null;
        }
        return location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    }

    /**
     * Overlay the registration's storage options on whatever the
     * catalog returned with the table (Glue passes its
     * {@code storage.*} config through, REST catalogs may vend
     * credentials). The registration's values win so an operator can
     * override what the catalog hands out.
     */
    static StorageOptions mergeStorageOptions(Map<String, String> fromCatalog, StorageOptions fromEntry) {
        if (fromCatalog == null || fromCatalog.isEmpty()) {
            return fromEntry;
        }
        Map<String, String> merged = new LinkedHashMap<>(fromCatalog);
        merged.putAll(fromEntry.asMap());
        return StorageOptions.of(merged);
    }

    /**
     * How an index found in cluster state relates to the poll's tracking.
     * {@link #NOT_LANCE} and {@link #PINNED} are left alone; the other two
     * name the bookkeeping the index has to be restored into.
     */
    enum Adoption {
        /** No {@code index.lance.table}: an ordinary OpenSearch index. */
        NOT_LANCE,
        /** {@code index.lance.version} is set: a readonly snapshot that never advances. */
        PINNED,
        /** The table sits directly under a registered namespace root and is named after the index. */
        NAMESPACE,
        /** Any other Lance-backed index: created through attach, or its namespace is no longer registered. */
        ATTACH
    }

    /**
     * Classify an index from its settings alone. A namespace-surfaced
     * index has {@code index.lance.table} equal to
     * {@code <root>/<indexName>.lance} for one of the registered
     * {@code namespaceRoots}, because that is the path the surface
     * step builds; everything else Lance-backed and unpinned is treated
     * the way an attached index is.
     */
    static Adoption classifyForAdoption(String indexName, Settings settings, Set<String> namespaceRoots) {
        String table = settings.get(LanceEngineFactory.TABLE_SETTING, "");
        if (table.isEmpty()) {
            return Adoption.NOT_LANCE;
        }
        if (settings.getAsLong(LanceEngineFactory.VERSION_SETTING, -1L) >= 0) {
            return Adoption.PINNED;
        }
        for (String root : namespaceRoots) {
            if (table.equals(root + "/" + indexName + ".lance")) {
                return Adoption.NAMESPACE;
            }
        }
        return Adoption.ATTACH;
    }

    /**
     * Put every Lance-backed, unpinned index that cluster state knows
     * about but {@link #servedVersions} does not back into the poll's
     * bookkeeping. Cluster state keeps {@code index.lance.table},
     * {@code index.lance.tag} and {@code index.lance.storage_options.*}
     * across a snapshot restore, a full cluster restart and a manager
     * failover, while the tracking maps are per node and start empty, so
     * they are rebuilt from those settings here.
     *
     * <p>The table is opened once before adopting so an index whose table
     * is unreachable is not handed to the sync loops, which would fail on
     * it every cycle; it is warned about once and retried on the next
     * poll. The served version is recorded as {@code -1}, below any real
     * manifest version, so the first {@link #runSyncCycle} after adoption
     * sees a move and re-derives the mapping and refreshes the reader
     * exactly once. That refresh is wanted: the engine may have opened an
     * older manifest than the one the table is at now.
     */
    private void adoptUntrackedIndexes(ClusterState state) {
        Set<String> roots = new HashSet<>();
        for (LanceNamespaceMetadata.Entry entry : currentMetadata(state).entries()) {
            if (entry.rootUri() != null) {
                roots.add(entry.rootUri());
            }
        }
        for (IndexMetadata indexMetadata : state.metadata().indices().values()) {
            String indexName = indexMetadata.getIndex().getName();
            if (servedVersions.containsKey(indexName)) {
                continue;
            }
            // One index with unparseable settings must not keep the rest
            // of the scan from running this cycle.
            try {
                adoptIfLanceBacked(indexName, indexMetadata.getSettings(), roots);
            } catch (Exception e) {
                LOG.warn("adoption scan failed for index {}", indexName, e);
            }
        }
    }

    private void adoptIfLanceBacked(String indexName, Settings settings, Set<String> roots) {
        Adoption adoption = classifyForAdoption(indexName, settings, roots);
        if (adoption == Adoption.NOT_LANCE || adoption == Adoption.PINNED) {
            return;
        }
        String table = settings.get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        try (Dataset ignored = LanceRegistry.openDataset(table, storageOptions)) {
            // Reachability probe only; the version is read by the
            // sync cycle that follows.
        } catch (Exception e) {
            if (warnedUnreachableAdopt.add(indexName)) {
                LOG.warn(
                    "cannot adopt Lance-backed index {} into the poll: table {} is unreachable ({}); retrying on the next poll",
                    indexName,
                    table,
                    e.getMessage()
                );
            }
            return;
        }
        if (adoption == Adoption.ATTACH) {
            String tag = settings.get(LanceEngineFactory.TAG_SETTING, "");
            attachedIndexes.put(indexName, new AttachedIndex(table, storageOptions, tag.isEmpty() ? null : tag));
        }
        servedVersions.put(indexName, -1L);
        // A restore can bring back an index under a name that was
        // deleted within the resurface grace; the index is present
        // again, so the tombstone no longer describes anything.
        tombstones.remove(indexName);
        warnedUnreachableAdopt.remove(indexName);
        warnedUnowned.remove(indexName);
        LOG.info(
            "adopting Lance-backed index {} (table {}, source {}) into the poll",
            indexName,
            table,
            adoption == Adoption.NAMESPACE ? "namespace" : "attach"
        );
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
                Long tombstonedAt = tombstones.get(indexName);
                if (tombstonedAt != null) {
                    long graceMs = resurfaceGrace.get().millis();
                    if (graceMs > 0 && System.currentTimeMillis() - tombstonedAt < graceMs) {
                        LOG.debug("skipping surface of {} at {}: index was deleted within the resurface guard window", indexName, table);
                        return;
                    }
                    // Grace expired or the guard was turned off; clear
                    // the tombstone so the map does not grow without
                    // bound and let surfacing proceed.
                    tombstones.remove(indexName);
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
                        storedOverrides = rewriteOverridesForSchemaDrift(indexName, storedOverrides, latestDataset.getLanceSchema());
                        RestAttachAction.Derivation derivation = RestAttachAction.derive(latestDataset, storedOverrides, true);
                        rederivedMappingJson = derivation.mappingJson();
                        warnOnLanceFieldRename(indexName, latestDataset.getLanceSchema());
                    } else {
                        // The tag points at an older manifest: derive from
                        // that snapshot so the mapping matches the schema
                        // the shard is about to read.
                        try (Dataset tagged = LanceRegistry.openDataset(table, storageOptions, Optional.of(target))) {
                            storedOverrides = rewriteOverridesForSchemaDrift(indexName, storedOverrides, tagged.getLanceSchema());
                            RestAttachAction.Derivation derivation = RestAttachAction.derive(tagged, storedOverrides, true);
                            rederivedMappingJson = derivation.mappingJson();
                            warnOnLanceFieldRename(indexName, tagged.getLanceSchema());
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

    /**
     * Follow the operator's mapping overrides across schema drift before
     * the mapping is re-derived. Two moves:
     * <ul>
     *   <li>Rename (a mapping field id now carries a different name in
     *       the Lance schema): every override keyed by the old column
     *       name is re-keyed to the new name, so the operator's
     *       {@code type} / {@code format} / {@code fields} rules follow
     *       the column and the re-derivation applies them to the new
     *       name.</li>
     *   <li>Reset (a column's Arrow type changed): the override is
     *       checked against the new type. A still-valid override stays
     *       ({@code type: date} on a column recast from Int64 to
     *       Timestamp); an override the new type does not admit is
     *       dropped from the setting with one warning naming the
     *       column, the override and the new type, so the setting does
     *       not carry a rule that can never apply again.</li>
     * </ul>
     * An override whose column is absent from the schema entirely is
     * left in the setting, as ever: it waits for a manifest that
     * restores the column.
     *
     * <p>The rewritten JSON is persisted with an update-settings call on
     * {@code index.lance.overrides} (Dynamic for exactly this purpose).
     * When persisting fails the stored overrides are returned unchanged
     * and the rewrite retries on the next poll cycle.
     */
    private LanceOverrides rewriteOverridesForSchemaDrift(String indexName, LanceOverrides stored, LanceSchema lanceSchema) {
        if (stored.isEmpty()) {
            return stored;
        }
        Map<Integer, MappingFieldInfo> mappingFieldIds;
        try {
            mappingFieldIds = readMappingFieldIds(indexName);
        } catch (Exception e) {
            LOG.debug("could not inspect mapping meta for {}: {}", indexName, e.getMessage());
            return stored;
        }
        Map<String, LanceField> lanceFieldsByName = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            lanceFieldsByName.put(field.getName(), field);
        }
        // Old name -> new name for every field id whose name moved.
        // A reset (different Arrow type under the same id) re-keys too:
        // the compatibility check below decides whether the override
        // survives on the new name.
        Map<String, String> renames = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            MappingFieldInfo mapped = mappingFieldIds.get(field.getId());
            if (mapped != null && !mapped.name.equals(field.getName())) {
                renames.put(mapped.name, field.getName());
            }
        }
        LanceOverrides rewritten = stored.withRenamedColumns(renames);
        // Compatibility: a column present in the schema must still admit
        // its override. Covers in-place type changes (same name, new
        // Arrow type) and renamed-plus-reset ids alike.
        for (Map.Entry<String, LanceOverrides.Column> entry : new LinkedHashMap<>(rewritten.columns()).entrySet()) {
            LanceField field = lanceFieldsByName.get(entry.getKey());
            if (field == null) {
                continue;
            }
            try {
                RestAttachAction.validateColumnOverride(entry.getKey(), entry.getValue(), field, lanceFieldsByName.keySet());
            } catch (IllegalArgumentException e) {
                rewritten = rewritten.withoutColumn(entry.getKey());
                String key = indexName + ":override-drop:" + entry.getKey() + ":" + field.getType();
                if (warnedRenamed.add(key)) {
                    LOG.warn(
                        "dropping mapping override on column '{}' of {}: the Lance schema reset the column to {} and the "
                            + "override no longer applies ({})",
                        entry.getKey(),
                        indexName,
                        field.getType(),
                        e.getMessage()
                    );
                }
            }
        }
        if (rewritten.equals(stored)) {
            return stored;
        }
        try {
            client.admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(LanceEngineFactory.OVERRIDES_SETTING, rewritten.toJson()).build())
                .execute()
                .actionGet();
            LOG.info("rewrote index.lance.overrides of {} after a Lance schema change: {}", indexName, rewritten.toJson());
            return rewritten;
        } catch (Exception e) {
            LOG.warn("could not persist rewritten overrides for {}: {}", indexName, e.getMessage());
            return stored;
        }
    }

    /**
     * Compare the Lance table's current schema against the OpenSearch mapping
     * to detect column renames, schema resets (a field id reused with a
     * different Arrow type), and drops. Reactions:
     * <ul>
     *   <li>Rename (same id, same Arrow type, different name): log a warning
     *       once per session. The old name lingers because PutMapping cannot
     *       remove properties.</li>
     *   <li>Schema reset (same id, different Arrow type): log a drop + add
     *       pair, mark the stale name as {@code lance_dropped}, and let the
     *       mapping re-derivation add the new column.</li>
     *   <li>Drop (id gone from Lance): log once and mark the stale name as
     *       {@code lance_dropped}.</li>
     * </ul>
     * {@code lance_dropped} is stored in the field's {@code meta} so the
     * read paths can hide the stale name: {@link LanceTextFieldMapper},
     * {@link LanceVectorFieldMapper} and the Lance query builders reject
     * queries against it up front, the coordinator's field type lookup
     * treats it as unmapped (no Lance SQL ever names the stale column,
     * scalar queries fold to 0 hits), and the explain endpoint's field
     * resolution names the rename. {@code GET _mapping} still lists the
     * stale name because PutMapping cannot remove properties.
     */
    private void warnOnLanceFieldRename(String indexName, LanceSchema lanceSchema) {
        Map<Integer, MappingFieldInfo> mappingFieldIds;
        try {
            mappingFieldIds = readMappingFieldIds(indexName);
        } catch (Exception e) {
            LOG.debug("could not inspect mapping meta for {}: {}", indexName, e.getMessage());
            return;
        }
        if (mappingFieldIds.isEmpty()) {
            return;
        }
        // Collect field names to mark as dropped after the loop so we can
        // issue a single PutMapping call. Empty when no drift is observed.
        Set<String> droppedFieldNames = new java.util.LinkedHashSet<>();

        Map<Integer, LanceField> lanceFieldsById = new HashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            lanceFieldsById.put(field.getId(), field);
        }

        for (LanceField field : lanceSchema.fields()) {
            MappingFieldInfo mapped = mappingFieldIds.get(field.getId());
            if (mapped == null || mapped.name.equals(field.getName())) {
                continue;
            }
            String currentArrowType = renameArrowType(field);
            boolean typeChanged = mapped.arrowType != null && currentArrowType != null && !mapped.arrowType.equals(currentArrowType);
            if (typeChanged) {
                // Same id, different Arrow type: not a rename, the writer
                // dropped the old column and reused the id for a new one.
                String key = indexName
                    + ":reset:"
                    + field.getId()
                    + ":"
                    + mapped.name
                    + "("
                    + mapped.arrowType
                    + ")->"
                    + field.getName()
                    + "("
                    + currentArrowType
                    + ")";
                if (warnedRenamed.add(key)) {
                    LOG.warn(
                        "Lance table for {} reset field id {}: dropped '{}' ({}), added '{}' ({}). "
                            + "The mapping still exposes '{}' marked lance_dropped: queries against it return no hits and "
                            + "lance_text / lance_vector queries fail. Recreate the index to drop it from the mapping.",
                        indexName,
                        field.getId(),
                        mapped.name,
                        mapped.arrowType,
                        field.getName(),
                        currentArrowType,
                        mapped.name
                    );
                }
                droppedFieldNames.add(mapped.name);
                continue;
            }
            String key = indexName + ":rename:" + field.getId() + ":" + mapped.name + "->" + field.getName();
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} renamed field id {} from '{}' to '{}'. "
                        + "The mapping keeps the old name marked lance_dropped (PutMapping cannot remove properties): queries "
                        + "against '{}' return no hits, and GET /_lance/stats lists the rename under renamed_fields. "
                        + "Mapping overrides keyed by the old name follow the column to '{}'.",
                    indexName,
                    field.getId(),
                    mapped.name,
                    field.getName(),
                    mapped.name,
                    field.getName()
                );
            }
            droppedFieldNames.add(mapped.name);
        }
        // drop_columns / overwrite on the Lance side removes a field id
        // entirely. OpenSearch's PutMapping cannot remove properties, so
        // the stale name lingers and queries against it fail silently with
        // 0 hits (numeric doc values just return their default, term
        // queries never match). Surface the drift so operators know to
        // recreate the index.
        for (Map.Entry<Integer, MappingFieldInfo> mapped : mappingFieldIds.entrySet()) {
            int id = mapped.getKey();
            if (lanceFieldsById.containsKey(id)) {
                continue;
            }
            String staleName = mapped.getValue().name;
            String key = indexName + ":dropped:" + id + ":" + staleName;
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} no longer contains field id {} ('{}'). "
                        + "The mapping keeps the name marked lance_dropped (PutMapping cannot remove properties): queries "
                        + "against '{}' return no hits. Recreate the index to drop it from the mapping.",
                    indexName,
                    id,
                    staleName,
                    staleName
                );
            }
            droppedFieldNames.add(staleName);
        }
        if (!droppedFieldNames.isEmpty()) {
            markFieldsDropped(indexName, droppedFieldNames, mappingFieldIds);
        }
    }

    /**
     * Best-effort encoding of a Lance field's Arrow type that matches the
     * strings stored under {@code meta.lance_arrow_type} at derivation
     * time. Returns {@code null} when the type is not one the deriver
     * fingerprints (unmapped columns).
     */
    private static String renameArrowType(LanceField field) {
        ArrowType type = field.getType();
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList fsl) {
            org.apache.arrow.vector.types.pojo.Field arrow = field.asArrowField();
            ArrowType child = arrow.getChildren().isEmpty() ? null : arrow.getChildren().get(0).getType();
            if (child instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fp
                && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                return "fixed_size_list<float32>[" + fsl.getListSize() + "]";
            }
            return null;
        }
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.List) {
            if (field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
                return "list<utf8>";
            }
            return null;
        }
        return type.toString();
    }

    /**
     * Persist {@code meta.lance_dropped = "true"} on each supplied field
     * name via PutMapping. Silently no-ops when the underlying mapping
     * update fails — the WARN log entries in the caller are the source of
     * truth, this is a best-effort assist so custom Lance mappers can
     * reject queries at the query builder layer.
     */
    private void markFieldsDropped(String indexName, Set<String> droppedNames, Map<Integer, MappingFieldInfo> mappingFieldIds) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
            // Extra map from name → info so we can preserve type / meta
            // when re-emitting each field.
            Map<String, MappingFieldInfo> byName = new HashMap<>(mappingFieldIds.size());
            for (MappingFieldInfo info : mappingFieldIds.values()) {
                byName.put(info.name, info);
            }
            for (String name : droppedNames) {
                MappingFieldInfo info = byName.get(name);
                if (info == null) {
                    continue;
                }
                // PutMapping requires "type" to be present when updating an
                // existing field's meta, otherwise the whole field is
                // rejected. Emit the existing type and the merged meta.
                builder.startObject(name);
                if (info.osType != null) {
                    builder.field("type", info.osType);
                }
                if (info.opts != null) {
                    for (Map.Entry<String, Object> opt : info.opts.entrySet()) {
                        builder.field(opt.getKey(), opt.getValue());
                    }
                }
                builder.startObject("meta");
                builder.field("lance_field_id", Integer.toString(info.fieldId));
                if (info.arrowType != null) {
                    builder.field("lance_arrow_type", info.arrowType);
                }
                builder.field("lance_dropped", "true");
                builder.endObject();
                builder.endObject();
            }
            builder.endObject().endObject();
            client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setSource(builder.toString(), MediaTypeRegistry.JSON)
                .execute()
                .actionGet();
        } catch (Exception e) {
            LOG.debug("could not update lance_dropped meta on {}: {}", indexName, e.getMessage());
        }
    }

    /**
     * Read Lance-related meta off every top-level field in the current
     * mapping. Fields without {@code meta.lance_field_id} (older indexes,
     * non-Lance mappings) are skipped, and so are fields already marked
     * {@code lance_dropped}: after a rename both the stale and the live
     * name carry the same field id, and drift detection must see the
     * live one only, or every later poll would re-detect the rename the
     * mapping already recorded. Returns a map from Lance field id
     * to the field's OpenSearch name, type, Arrow type identifier, and
     * remaining top-level options (so a subsequent update can round-trip
     * the field unchanged).
     */
    private Map<Integer, MappingFieldInfo> readMappingFieldIds(String indexName) {
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
        MappingMetadata mapping = response.mappings().get(indexName);
        if (mapping == null) {
            return Map.of();
        }
        Map<String, Object> source = mapping.sourceAsMap();
        Object properties = source.get("properties");
        if (!(properties instanceof Map<?, ?> propsMap)) {
            return Map.of();
        }
        Map<Integer, MappingFieldInfo> result = new HashMap<>();
        for (Map.Entry<?, ?> entry : propsMap.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> field)) {
                continue;
            }
            Object meta = field.get("meta");
            if (!(meta instanceof Map<?, ?> metaMap)) {
                continue;
            }
            if ("true".equals(metaMap.get("lance_dropped"))) {
                continue;
            }
            Object rawId = metaMap.get("lance_field_id");
            if (!(rawId instanceof String s)) {
                continue;
            }
            int fieldId;
            try {
                fieldId = Integer.parseInt(s);
            } catch (NumberFormatException e) {
                // A hand-crafted mapping may put anything here; skip malformed entries.
                continue;
            }
            String osType = field.get("type") instanceof String t ? t : null;
            String arrowType = metaMap.get("lance_arrow_type") instanceof String at ? at : null;
            // Preserve any per-type options (e.g. lance_vector's dimension /
            // element_type) so a subsequent PutMapping to update meta round
            // trips the field unchanged.
            Map<String, Object> opts = new HashMap<>();
            for (Map.Entry<?, ?> optEntry : field.entrySet()) {
                if (!(optEntry.getKey() instanceof String optName)) {
                    continue;
                }
                if ("type".equals(optName) || "meta".equals(optName)) {
                    continue;
                }
                opts.put(optName, optEntry.getValue());
            }
            result.put(fieldId, new MappingFieldInfo(fieldId, name, osType, arrowType, opts));
        }
        return result;
    }

    /**
     * Snapshot of one top-level mapping field, kept around so drift
     * handling and lance_dropped updates can round-trip the field without
     * losing type-specific options.
     */
    private record MappingFieldInfo(int fieldId, String name, String osType, String arrowType, Map<String, Object> opts) {
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
    private record AttachedIndex(String tablePath, StorageOptions storageOptions, String tag) {
    }
}
