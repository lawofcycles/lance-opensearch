/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * The RFC's namespace registration.
 *
 * <p>A registered catalog is polled at a configurable cadence via the Lance
 * Namespace API, on the elected cluster manager only. Tables in the
 * catalog surface as OpenSearch indexes automatically, with mappings and
 * shard counts derived from the Lance schema. Keeping a surfaced or
 * attached index in step with its table afterwards (a new manifest
 * version, a moved tag, a schema change) is the job of
 * {@link LanceIndexFreshnessService} on the node that holds the index's
 * shard; the manager opens no table except the one it is about to
 * surface. The runtime handle behind each registration is a
 * {@link LanceNamespace}: {@code DirectoryNamespace} for filesystem
 * catalogs, {@code RestNamespace} for REST catalogs, and the Glue,
 * Iceberg REST, Polaris and Unity implementations for those catalog
 * servers, all built by {@link LanceNamespaceFactory}.
 */
public final class LanceNamespaceService {

    private static final Logger LOG = LogManager.getLogger(LanceNamespaceService.class);

    /** How long a manual poll waits for the CreateIndex calls it issued before answering. */
    private static final TimeValue MANUAL_POLL_CREATE_TIMEOUT = TimeValue.timeValueSeconds(60);

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
    /**
     * Registration names whose last poll listed the catalog but could
     * not list one of its subnamespaces, mapped to that failure. The
     * tables below the refused subnamespace are not surfaced. An entry
     * here surfaces as {@code "status": "partial"} in the GET listing
     * and is retried on every poll; a poll that walks every namespace
     * removes it.
     */
    private final Map<String, String> partial = new ConcurrentHashMap<>();
    /** Names whose initialise failure has been warned about, so the retry loop logs once. */
    private final Set<String> warnedInitFailure = ConcurrentHashMap.newKeySet();
    /** "name:index" pairs whose catalog location fell outside the allowlist, warned once. */
    private final Set<String> warnedDisallowedLocation = ConcurrentHashMap.newKeySet();
    /** Serialises handle construction so a poll and an applier tick do not double-initialise. */
    private final Object initLock = new Object();
    /** Serialises poll cycles so a manual poll and the scheduled one do not race to create the same index. */
    private final Object pollLock = new Object();
    // Index names we've already flagged as unowned, so the poll doesn't shout
    // the same warning every ten seconds. Cleared if the collision resolves.
    private final Set<String> warnedUnowned = ConcurrentHashMap.newKeySet();
    /** Tombstone bookkeeping and grace check for the re-surface guard. */
    private final LanceResurfaceGuard resurfaceGuard;
    private final ThreadPool threadPool;
    /** Fragment path snapshot cache to retire from on index deletion, or {@code null}. */
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
     *                     from when an index is deleted; {@code null}
     *                     when there is none (tests)
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
            Metadata previous = event.previousState().metadata();
            Metadata current = event.state().metadata();
            long now = System.currentTimeMillis();
            for (String prevIndex : previous.indices().keySet()) {
                if (current.hasIndex(prevIndex)) {
                    continue;
                }
                IndexMetadata prevMeta = previous.index(prevIndex);
                if (prevMeta == null) {
                    continue;
                }
                String table = prevMeta.getSettings().get(LanceEngineFactory.TABLE_SETTING);
                if (table == null || table.isEmpty()) {
                    continue;
                }
                resurfaceGuard.recordTombstone(prevIndex, table, now);
                warnedUnowned.remove(prevIndex);
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
        Set<String> desired = new HashSet<>(metadata.entries().size());
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
        partial.keySet().removeIf(name -> !desired.contains(name));
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
     * first call. Both the build (in {@link LanceNamespaceFactory#create})
     * and every later call (in {@link LanceNamespaceHandle#call}) run
     * inside {@code doPrivileged}, so the SDK's reads of the process
     * user's {@code ~/.aws} files are judged against the plugin's own
     * policy and not the server frames on this thread's stack.
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
        try {
            for (LanceCatalogEnumerator.CatalogTable table : handle.call(
                namespace -> LanceCatalogEnumerator.enumerateTables(namespace, entry)
            ).tables()) {
                names.add(table.name());
            }
        } catch (LanceNamespaceHandle.ReleasedException e) {
            // The registration was removed between the metadata read
            // above and the call: answer as for an unregistered path.
            return Optional.empty();
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
                    unavailable.get(entry.name()),
                    partial.get(entry.name())
                )
            );
        }
        return List.copyOf(infos);
    }

    /**
     * What one catalog listing cycle did: the indexes whose CreateIndex
     * was issued (and, for a manual poll, acknowledged), the tables that
     * were left alone with the reason, the registrations whose listing
     * failed with the error, and the registrations whose listing
     * succeeded but could not descend into one of their subnamespaces,
     * with that failure.
     */
    public record PollReport(List<String> surfaced, List<SkippedTable> skipped, Map<String, String> unavailable, Map<
        String,
        String> partial) {

        /** A report without partial listings, the shape of the previous wire version. */
        public PollReport(List<String> surfaced, List<SkippedTable> skipped, Map<String, String> unavailable) {
            this(surfaced, skipped, unavailable, Map.of());
        }

        /** A catalog table the cycle did not surface: the index name it would have taken and why. */
        public record SkippedTable(String namespace, String table, String index, String reason) {
        }
    }

    /** One cycle's bookkeeping; the scheduled poll drops it, the manual poll answers with it. */
    private static final class CycleReport {
        final List<String> surfaced = new ArrayList<>();
        final List<PollReport.SkippedTable> skipped = new ArrayList<>();
        final Map<String, String> unavailable = new LinkedHashMap<>();
        final Map<String, String> partial = new LinkedHashMap<>();
        final List<PendingCreate> creates = new ArrayList<>();

        record PendingCreate(String namespace, String table, String index, PlainActionFuture<CreateIndexResponse> future) {
        }
    }

    // Package-private so tests can drive a poll cycle synchronously
    // instead of waiting for the scheduled cadence.
    void poll() {
        // Skip poll cycles that fire before the applier delivers a
        // cluster state, which happens once at startup. Reading state()
        // then would trip the AssertionError inside
        // ClusterApplierService instead of returning gracefully.
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
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
        pollCycle(null, false);
    }

    /**
     * Run one catalog listing cycle now, on the calling thread (the
     * generic pool: listing and the derivation open storage), and
     * answer what it did. {@code name} limits the cycle to one
     * registration; {@code null} lists every registration. The
     * CreateIndex calls the cycle issues are awaited so the answer
     * names the indexes that exist when it returns. Callers route this
     * to the elected cluster manager, where the scheduled poll runs.
     */
    public PollReport pollNow(String name) {
        return pollCycle(name, true);
    }

    private PollReport pollCycle(String onlyName, boolean awaitCreates) {
        synchronized (pollLock) {
            ClusterState state = clusterService.state();
            CycleReport report = new CycleReport();
            LanceNamespaceMetadata metadata = currentMetadata(state);
            for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
                if (onlyName != null && !onlyName.equals(entry.name()) && !onlyName.equals(entry.rootUri())) {
                    continue;
                }
                LanceNamespaceHandle handle = ensureHandle(entry);
                if (handle == null) {
                    // initialise failed; ensureHandle recorded the error and
                    // will retry on the next poll.
                    report.unavailable.put(entry.name(), unavailable.getOrDefault(entry.name(), "initialise failed"));
                    continue;
                }
                try {
                    LanceCatalogEnumerator.Enumeration listed = handle.call(
                        namespace -> LanceCatalogEnumerator.enumerateTables(namespace, entry)
                    );
                    for (LanceCatalogEnumerator.CatalogTable table : listed.tables()) {
                        syncCatalogTable(state, entry, handle, table, report);
                    }
                    unavailable.remove(entry.name());
                    if (listed.firstNamespacesFailure() == null) {
                        partial.remove(entry.name());
                    } else {
                        // The catalog answered, but a subnamespace refused
                        // its listing (credentials that do not cover it,
                        // a database the endpoint cannot reach): the
                        // tables below it stay hidden until a poll walks
                        // it, so the registration shows as partial.
                        String message = listed.firstNamespacesFailure().getMessage();
                        partial.put(entry.name(), message);
                        report.partial.put(entry.name(), message);
                        LOG.warn("namespace poll of {} could not list every subnamespace: {}", entry.name(), message);
                    }
                } catch (LanceNamespaceHandle.ReleasedException e) {
                    // The registration was removed after this cycle read the
                    // metadata; there is nothing to report against it.
                    LOG.debug("namespace {} was unregistered during the poll cycle", entry.name());
                } catch (Exception e) {
                    // A listing failure (unreachable endpoint, revoked
                    // credentials after a successful initialise) marks the
                    // registration unavailable until a poll succeeds again.
                    String message = e.getMessage() == null ? e.toString() : e.getMessage();
                    unavailable.put(entry.name(), message);
                    partial.remove(entry.name());
                    report.unavailable.put(entry.name(), message);
                    LOG.warn("namespace poll failed for {}", entry.name(), e);
                }
            }
            if (awaitCreates) {
                awaitCreates(report);
            } else {
                for (CycleReport.PendingCreate pending : report.creates) {
                    report.surfaced.add(pending.index());
                }
            }
            return new PollReport(
                List.copyOf(report.surfaced),
                List.copyOf(report.skipped),
                Map.copyOf(report.unavailable),
                Map.copyOf(report.partial)
            );
        }
    }

    /**
     * Wait for the creates a manual cycle issued. A create that failed
     * (other than because the index already exists) moves its table to
     * the skipped list with the failure; one that does not answer in
     * time is reported as issued but not acknowledged.
     */
    private void awaitCreates(CycleReport report) {
        long deadline = System.nanoTime() + MANUAL_POLL_CREATE_TIMEOUT.nanos();
        for (CycleReport.PendingCreate pending : report.creates) {
            long remaining = Math.max(1L, deadline - System.nanoTime());
            try {
                pending.future().actionGet(remaining, TimeUnit.NANOSECONDS);
                report.surfaced.add(pending.index());
            } catch (Exception e) {
                if (isAlreadyExists(e)) {
                    report.surfaced.add(pending.index());
                } else {
                    report.skipped.add(
                        new PollReport.SkippedTable(
                            pending.namespace(),
                            pending.table(),
                            pending.index(),
                            "create index failed: " + e.getMessage()
                        )
                    );
                }
            }
        }
    }

    /**
     * Sync one catalog table into the poll's index bookkeeping.
     *
     * <p>A directory registration builds the table path from its root
     * and the table name, exactly the shape the persisted
     * {@code index.lance.table} setting relies on. A rest or glue
     * registration asks the catalog itself through
     * {@code describeTable}; the returned location is validated
     * against {@code lance.allowed_table_roots} before anything
     * surfaces, because for these types the register call had no root
     * the allowlist could gate.
     */
    private void syncCatalogTable(
        ClusterState state,
        LanceNamespaceMetadata.Entry entry,
        LanceNamespaceHandle handle,
        LanceCatalogEnumerator.CatalogTable table,
        CycleReport report
    ) {
        String indexName = table.name();
        if (LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type())) {
            surfaceIfMissing(
                state,
                entry.name(),
                entry.rootUri() + "/" + indexName + ".lance",
                indexName,
                entry.storageOptions(),
                entry.overridesJson(),
                report
            );
            return;
        }
        DescribeTableResponse described;
        try {
            described = handle.call(namespace -> namespace.describeTable(new DescribeTableRequest().id(table.id())));
        } catch (LanceNamespaceHandle.ReleasedException e) {
            // The registration was removed mid-cycle; nothing to surface.
            return;
        } catch (Exception e) {
            LOG.warn("describe_table failed for {} in namespace {}: {}", indexName, entry.name(), e.getMessage());
            report.skipped.add(new PollReport.SkippedTable(entry.name(), indexName, indexName, "describe_table failed: " + e.getMessage()));
            return;
        }
        String location = LanceCatalogEnumerator.tableLocation(described);
        if (location == null) {
            LOG.warn("catalog {} returned no location for table {}; skipping", entry.name(), indexName);
            report.skipped.add(new PollReport.SkippedTable(entry.name(), indexName, indexName, "the catalog returned no location"));
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
            report.skipped.add(
                new PollReport.SkippedTable(
                    entry.name(),
                    indexName,
                    indexName,
                    "location " + location + " is outside lance.allowed_table_roots"
                )
            );
            return;
        }
        warnedDisallowedLocation.remove(entry.name() + ":" + indexName);
        surfaceIfMissing(
            state,
            entry.name(),
            location,
            indexName,
            LanceCatalogEnumerator.mergeStorageOptions(described.getStorageOptions(), entry.storageOptions()),
            entry.overridesJson(),
            report
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
     * Create the index for {@code table} unless one exists. An existing
     * index that carries {@code index.lance.table} equal to the table is
     * the one an earlier cycle (or attach) created for it, and the node
     * holding its shard keeps it fresh; any other index under the name
     * is a name collision, warned once and left alone. A name deleted
     * within the resurface grace is left alone too.
     */
    private void surfaceIfMissing(
        ClusterState state,
        String namespaceName,
        String table,
        String indexName,
        StorageOptions storageOptions,
        String surfaceOverridesJson,
        CycleReport report
    ) {
        IndexMetadata existing = state.metadata().index(indexName);
        if (existing != null) {
            String existingTable = existing.getSettings().get(LanceEngineFactory.TABLE_SETTING, "");
            if (existingTable.equals(table)) {
                // Present again after a restore or a rebuild: a tombstone
                // recorded for the name no longer describes anything.
                resurfaceGuard.clearTombstone(indexName);
                warnedUnowned.remove(indexName);
                return;
            }
            // The index name already existed before we saw the table: a
            // classic OpenSearch index, or another namespace beat us to
            // the name. Recoverable with operator action, so log once
            // per index instead of silently skipping every poll.
            if (warnedUnowned.add(indexName)) {
                LOG.warn("skipping table {}: index {} exists but is not backed by it (name collision)", table, indexName);
            }
            report.skipped.add(
                new PollReport.SkippedTable(
                    namespaceName,
                    indexName,
                    indexName,
                    existingTable.isEmpty()
                        ? "name collision: an index of that name exists and is not Lance backed"
                        : "name collision: index is backed by another table " + existingTable
                )
            );
            return;
        }
        // Re-surface guard: if this index was recently deleted through
        // OpenSearch, honour the operator's intent and skip the surface
        // until the grace period expires. Grace <= 0 disables the guard
        // and every poll recreates the index unconditionally.
        if (resurfaceGuard.shouldSkipSurface(indexName, table)) {
            report.skipped.add(
                new PollReport.SkippedTable(namespaceName, indexName, indexName, "deleted within lance.namespace.resurface_guard_grace")
            );
            return;
        }
        try {
            report.creates.add(
                new CycleReport.PendingCreate(
                    namespaceName,
                    table,
                    indexName,
                    surface(indexName, table, storageOptions, surfaceOverridesJson)
                )
            );
        } catch (Exception e) {
            LOG.warn("surface failed for table {} as index {}", table, indexName, e);
            report.skipped.add(new PollReport.SkippedTable(namespaceName, indexName, indexName, "surface failed: " + e.getMessage()));
        }
    }

    /**
     * Derive the mapping from the table's current schema and issue the
     * CreateIndex. The create is asynchronous so a red shard on this
     * table does not block the poll thread for 30 seconds waiting for
     * the ack while every other table in the namespace waits behind it;
     * the returned future completes with the ack (a manual poll waits
     * on it, the scheduled one does not).
     */
    private PlainActionFuture<CreateIndexResponse> surface(
        String indexName,
        String table,
        StorageOptions storageOptions,
        String overridesJson
    ) throws Exception {
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
        final long version = derivation.version();
        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        LanceIndexCreation.create(
            client,
            threadPool,
            LanceIndexCreation.request(indexName, table, storageOptions, derivation, Settings.EMPTY),
            new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    LOG.info("surfaced table {} as index {} (version {})", table, indexName, version);
                    future.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    // ResourceAlreadyExistsException means another node
                    // (or an earlier poll) already surfaced the table;
                    // the next cycle finds the index in cluster state.
                    if (isAlreadyExists(e)) {
                        LOG.debug("surface for {} raced with an existing index", indexName);
                    } else {
                        LOG.warn("surface failed for {} at version {}: {}", indexName, version, e.getMessage());
                    }
                    future.onFailure(e);
                }
            }
        );
        return future;
    }

    private static boolean isAlreadyExists(Exception e) {
        Throwable cursor = e;
        while (cursor != null) {
            if (cursor instanceof ResourceAlreadyExistsException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }
}
