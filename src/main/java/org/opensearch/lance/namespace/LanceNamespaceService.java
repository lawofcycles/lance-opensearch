/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.namespace.DirectoryNamespace;
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
 * Namespace API. Tables in the catalog surface as OpenSearch indexes
 * automatically, with mappings and shard counts derived from the Lance
 * schema. Version changes propagate through a shard-level refresh that
 * swaps the reader without closing the index. Today the implementation
 * uses {@link DirectoryNamespace} for filesystem catalogs; the same
 * abstraction will accept REST catalogs (Glue, Unity, Iceberg REST) as
 * the Lance Namespace ecosystem matures.
 */
public final class LanceNamespaceService {

    private static final Logger LOG = LogManager.getLogger(LanceNamespaceService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final TimeValue cadence;
    private final long builderMaxRows;
    /**
     * Per-node cache of the runtime {@link DirectoryNamespace} handles
     * keyed by root URI. Rebuilt from cluster state on every
     * {@link #onClusterStateChanged} callback so a fresh node that
     * joins mid-life still sees the registrations that were already
     * in the cluster's {@link LanceNamespaceMetadata}.
     */
    private final Map<String, DirectoryNamespace> directoryCache = new ConcurrentHashMap<>();
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
        this(client, clusterService, threadPool, cadence, builderMaxRows, resurfaceGrace, null);
    }

    /**
     * @param warmCache fragment path snapshot cache to retire entries
     *                  from when a table moves to a new version or an
     *                  index is deleted; {@code null} when there is none
     *                  (tests)
     */
    public LanceNamespaceService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        TimeValue cadence,
        long builderMaxRows,
        TimeValue resurfaceGrace,
        LanceWarmCache warmCache
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.cadence = cadence;
        this.builderMaxRows = builderMaxRows;
        this.warmCache = warmCache;
        this.resurfaceGrace = new java.util.concurrent.atomic.AtomicReference<>(resurfaceGrace);
        // Subscribe before the first schedule fires so the poller
        // never runs against a stale cache. addListener returns
        // immediately; the listener body reads whatever state is
        // current when the applier fires.
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
     * Reconcile the per-node {@link #directoryCache} against the
     * cluster's {@link LanceNamespaceMetadata}. Called from the
     * cluster state applier on every state that touches metadata, so
     * new registrations propagated from another node reach the
     * cache in time for the next poll cycle. Failures to initialise
     * a DirectoryNamespace surface as warnings.
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
            desired.add(entry.rootUri());
            directoryCache.computeIfAbsent(entry.rootUri(), uri -> {
                try {
                    DirectoryNamespace namespace = new DirectoryNamespace();
                    Map<String, String> config = new HashMap<>();
                    config.put("root", uri);
                    namespace.initialize(config, LanceRegistry.allocator());
                    return namespace;
                } catch (Exception e) {
                    LOG.warn("failed to initialise namespace {} through DirectoryNamespace", uri, e);
                    return null;
                }
            });
        }
        // Drop cache entries for namespaces the cluster removed.
        directoryCache.keySet().removeIf(uri -> !desired.contains(uri));
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
        List<String> uris = new ArrayList<>(metadata.entries().size());
        for (LanceNamespaceMetadata.Entry entry : metadata.entries()) {
            uris.add(entry.rootUri());
        }
        return List.copyOf(uris);
    }

    /**
     * List the tables the poll cycle would surface from the namespace
     * registered at {@code rootUri}. Returns an empty {@link Optional} when
     * the namespace is not registered (or the local applier has not yet
     * built the runtime handle for it), a populated set otherwise. The
     * value comes straight from {@link DirectoryNamespace#listTables}, so
     * table names are without the {@code .lance} suffix and without any
     * scheme prefix — matching the form the surface path uses.
     *
     * <p>Read-only: does not create, delete, or advance anything. The
     * caller can use this to preview what the next poll would do (for
     * example after a fresh namespace registration on a large directory),
     * or to spot tables the poller failed to surface due to a name
     * clash with an existing OpenSearch index.
     */
    public Optional<java.util.Set<String>> listTables(String rootUri) throws Exception {
        DirectoryNamespace directory = directoryCache.get(rootUri);
        if (directory == null) {
            return Optional.empty();
        }
        ListTablesResponse response = directory.listTables(new ListTablesRequest());
        java.util.Set<String> tables = response.getTables();
        return Optional.of(tables == null ? java.util.Collections.emptySet() : tables);
    }

    private void poll() {
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
            DirectoryNamespace directory = directoryCache.get(entry.rootUri());
            if (directory == null) {
                // Cluster state carries the registration but the
                // applier has not yet built a runtime handle on this
                // node. Skip this cycle; the next poll after
                // onClusterStateChanged finishes will pick it up.
                continue;
            }
            try {
                ListTablesResponse response = directory.listTables(new ListTablesRequest());
                Set<String> tables = response.getTables();
                if (tables == null) {
                    continue;
                }
                for (String tableName : tables) {
                    syncTable(entry.rootUri(), tableName, entry.storageOptions());
                }
            } catch (Exception e) {
                LOG.warn("namespace poll failed for {}", entry.rootUri(), e);
            }
        }
        // Sync attach-created indexes so append fragments surface on the
        // same schedule as namespace-registered tables. attach records the
        // (indexName -> tablePath) pair; the sync path is the same, just
        // without the rootUri / tableName join namespace tables use.
        for (Map.Entry<String, AttachedIndex> entry : attachedIndexes.entrySet()) {
            AttachedIndex attached = entry.getValue();
            try {
                syncAttachedTable(entry.getKey(), attached.tablePath, attached.storageOptions, attached.tag);
            } catch (Exception e) {
                LOG.warn("attach poll failed for index {} at {}", entry.getKey(), attached.tablePath, e);
            }
        }
    }

    private void syncTable(String rootUri, String tableName, StorageOptions storageOptions) {
        String table = rootUri + "/" + tableName + ".lance";
        runSyncCycle(table, tableName, storageOptions, null);
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
            roots.add(entry.rootUri());
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
        runSyncCycle(tablePath, indexName, storageOptions, tag);
    }

    private void runSyncCycle(String table, String indexName, StorageOptions storageOptions, String tag) {
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
                surface(indexName, table, storageOptions);
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
                    // so the re-derived mapping preserves multi-field
                    // declarations across manifest version advance; without this the
                    // mapping would drop back to the default derivation and a caller
                    // querying body.raw would suddenly see 400 no-such-field errors.
                    IndexMetadata rederivationMetadata = clusterService.state().metadata().index(indexName);
                    String storedMultiFieldsJson = rederivationMetadata == null
                        ? ""
                        : rederivationMetadata.getSettings().get(LanceEngineFactory.MULTI_FIELDS_SETTING, "");
                    java.util.Map<String, java.util.LinkedHashMap<String, String>> storedMultiFields = RestAttachAction
                        .deserialiseMultiFields(storedMultiFieldsJson);
                    if (target == latest) {
                        RestAttachAction.Derivation derivation = RestAttachAction.derive(latestDataset, storedMultiFields);
                        rederivedMappingJson = derivation.mappingJson();
                        warnOnLanceFieldRename(indexName, latestDataset.getLanceSchema());
                    } else {
                        // The tag points at an older manifest: derive from
                        // that snapshot so the mapping matches the schema
                        // the shard is about to read.
                        try (Dataset tagged = LanceRegistry.openDataset(table, storageOptions, Optional.of(target))) {
                            RestAttachAction.Derivation derivation = RestAttachAction.derive(tagged, storedMultiFields);
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
                                surface(indexName, table, storageOptions);
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

    private void surface(String indexName, String table, StorageOptions storageOptions) throws Exception {
        RestAttachAction.Derivation derivation;
        try (Dataset dataset = LanceRegistry.openDataset(table, storageOptions)) {
            // Derive first so the CreateIndex settings and mapping reflect
            // the current Lance schema. Automatic index creation is off by
            // default; operators build indexes explicitly through
            // POST /_lance/build_indexes.
            derivation = RestAttachAction.derive(dataset);
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
     * {@code lance_dropped} is stored in the field's {@code meta} so
     * {@link LanceTextFieldMapper}, {@link LanceVectorFieldMapper}, and any
     * future custom type can reject queries against it up front. Standard
     * scalar mappers (integer / keyword / date / boolean) do not honour it
     * yet — they still accept queries silently.
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
                            + "The mapping still exposes '{}'; marking it lance_dropped so lance_text / lance_vector queries fail. "
                            + "Standard scalar queries against '{}' still succeed silently — recreate the index to drop it.",
                        indexName,
                        field.getId(),
                        mapped.name,
                        mapped.arrowType,
                        field.getName(),
                        currentArrowType,
                        mapped.name,
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
                        + "The mapping still exposes both names; marking the old name lance_dropped so lance_text / lance_vector "
                        + "queries against it fail. Standard scalar queries against '{}' still succeed silently. Recreate the "
                        + "index to drop the stale mapping.",
                    indexName,
                    field.getId(),
                    mapped.name,
                    field.getName(),
                    mapped.name
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
                        + "Marking the mapping field lance_dropped so lance_text / lance_vector queries against it fail. "
                        + "Standard scalar queries against '{}' still succeed silently; recreate the index to drop it.",
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
     * non-Lance mappings) are skipped. Returns a map from Lance field id
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
