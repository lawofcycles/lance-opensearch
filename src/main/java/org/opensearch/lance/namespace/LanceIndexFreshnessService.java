/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.engine.LanceServedVersions;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.lance.stats.LanceNodeStats;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Keeps the Lance backed indexes whose shard this node holds in step with
 * their tables. One instance per node. The shard lifecycle hooks register
 * every started Lance backed shard (a Lance backed index has one shard and
 * no replica, so exactly one node checks each index) and unregister it
 * when the shard closes; each tracked index gets its own task at the
 * namespace poll cadence on the generic pool.
 *
 * <p>A check opens the table's latest manifest and compares the version
 * the index should serve (the latest, or the version the followed tag
 * points at) with the version the shard's engine serves, read from the
 * node's {@link LanceServedVersions} (the engine publishes its reader
 * manager's counter there, so no searcher is acquired and no reader
 * wrapper is looked past). When they differ,
 * the mapping is derived again from the schema of the version about to be
 * served, with the stored overrides followed across renames and resets,
 * and applied only when it differs from the mapping the index has: the
 * derived mapping is merged into the shard's own {@link MapperService} as
 * a preflight, and a {@code PutMapping} is sent only when the merged
 * mapping is not the current one. Then the shard is refreshed, which is
 * where the engine swaps its reader to the new version, and the snapshots
 * of the version left behind are retired. A pinned index
 * ({@code index.lance.version}) never advances and is not tracked. A
 * {@code node_local} index only has its reader advanced; its mapping is
 * maintained by the build action from the clones.
 *
 * <p>The first check after a shard starts derives the mapping even when
 * the version did not move: the table may have changed while no node held
 * the shard (a node restart, a shard relocation), and the comparison keeps
 * that first check from sending a mapping update the index does not need.
 *
 * <p>A {@code keyword} to {@code lance_text} flip (the table gained or lost
 * an inverted index on a Utf8 column) cannot be applied by
 * {@code PutMapping}. The index is rebuilt: deleted, then created again
 * with the same table, storage options, overrides and tag and the new
 * mapping. The service outlives its own index's deletion; the closing
 * shard unregisters itself and the new shard registers when it starts.
 * Any other refusal of the {@code PutMapping} leaves the index on the
 * mapping it has; the message is kept per index and reported by
 * {@link #stats()} and by the outcome of the check, until a mapping
 * update for the index is acknowledged (or the index is rebuilt) or a
 * check at another version than the refused one finds its mapping in
 * place. A check that has nothing to derive (the version stood still)
 * leaves the entry alone: the refused mapping is still the one the
 * index should have.
 */
public final class LanceIndexFreshnessService implements IndexEventListener, Closeable {

    private static final Logger LOG = LogManager.getLogger(LanceIndexFreshnessService.class);

    static final String REFRESH_SOURCE = "lance freshness";
    private static final String UNCOVERED_FRAGMENT_POLICY_SETTING = "index.lance.uncovered_fragment_policy";

    /**
     * What one check found and did; the answer of {@code POST /{index}/_lance/sync}.
     * {@code mappingChanged} is true once the cluster manager acknowledged
     * the mapping update the check sent (or the index was rebuilt);
     * {@code mappingError} is the message of the {@code PutMapping} the
     * check sent and the cluster manager refused, {@code null} when the
     * mapping was not touched or the update was applied. The two are
     * never both set.
     */
    public record Outcome(String index, boolean checked, String reason, boolean moved, long servedVersion, long targetVersion,
        boolean mappingChanged, boolean rebuilt, String mappingError) {

        public Outcome(
            String index,
            boolean checked,
            String reason,
            boolean moved,
            long servedVersion,
            long targetVersion,
            boolean mappingChanged,
            boolean rebuilt
        ) {
            this(index, checked, reason, moved, servedVersion, targetVersion, mappingChanged, rebuilt, null);
        }

        static Outcome notChecked(String index, String reason) {
            return new Outcome(index, false, reason, false, -1L, -1L, false, false, null);
        }
    }

    /** How the derived mapping relates to the mapping the index has. */
    enum MappingComparison {
        /** Merging the derived mapping into the current one changes nothing. */
        UNCHANGED,
        /** The merge adds or changes a field; a {@code PutMapping} is needed. */
        CHANGED,
        /** A field changes type between {@code keyword} and {@code lance_text}; the index has to be rebuilt. */
        TYPE_CONFLICT
    }

    /**
     * The started shard of one Lance backed index as the check sees it.
     * The production implementation wraps an {@link IndexShard}; tests
     * substitute their own.
     */
    interface TrackedShard {
        String indexName();

        String indexUuid();

        /** The index settings as they are now (the tag is dynamic). */
        Settings settings();

        /** Manifest version the shard reader serves, or {@code -1} when it cannot be read. */
        long servedVersion();

        MappingComparison compareMapping(String mappingJson);

        void refresh(String source);
    }

    /** One tracked index: its shard and the scheduled check. */
    static final class Tracked {
        final TrackedShard shard;
        volatile Scheduler.Cancellable task;
        /** Whether a check has derived the mapping since the shard started; guarded by the entry's monitor. */
        boolean derivedOnce;

        Tracked(TrackedShard shard) {
            this.shard = shard;
        }

        void cancel() {
            Scheduler.Cancellable current = task;
            if (current != null) {
                current.cancel();
            }
        }

        boolean isCancelled() {
            Scheduler.Cancellable current = task;
            return current == null || current.isCancelled();
        }
    }

    private final Client client;
    private final ThreadPool threadPool;
    private final TimeValue cadence;
    /** Snapshot cache to retire the left behind version from, or {@code null} when there is none. */
    private final LanceWarmCache warmCache;
    /** The served version of every open Lance engine on this node. */
    private final LanceServedVersions servedVersions;
    private final LanceSchemaDriftDetector driftDetector;
    private final Map<String, Tracked> tracked = new ConcurrentHashMap<>();
    /** Indexes whose `wait` uncovered fragment policy has been explained once. */
    private final Set<String> warnedWaitPolicy = ConcurrentHashMap.newKeySet();
    /**
     * The last mapping update the cluster manager refused, per index:
     * the version the refused derivation targeted and the refusal
     * message. Kept until a mapping update for the index is
     * acknowledged (or the index is rebuilt), until a check at another
     * version finds the derived mapping already in place, or until the
     * shard leaves this node. A check that derives nothing (the version
     * stood still) keeps the entry: the refused mapping is still the one
     * the index should have. Reported under
     * {@code freshness.mapping_errors} of {@code GET /_lance/stats},
     * because a refused update otherwise leaves the index serving the
     * stale mapping with nothing but a WARN line to say so.
     */
    private final Map<String, RefusedMapping> mappingErrors = new ConcurrentHashMap<>();

    /** A refused mapping update: the version its derivation targeted and the cluster manager's message. */
    private record RefusedMapping(long version, String message) {
    }

    private final LongAdder checks = new LongAdder();
    private final LongAdder moves = new LongAdder();
    private final LongAdder mappingUpdates = new LongAdder();
    private final LongAdder mappingUnchanged = new LongAdder();
    private final LongAdder rebuilds = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private volatile long lastCheckMillis;

    public LanceIndexFreshnessService(
        Client client,
        ThreadPool threadPool,
        TimeValue cadence,
        LanceWarmCache warmCache,
        LanceServedVersions servedVersions
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.cadence = cadence;
        this.warmCache = warmCache;
        this.servedVersions = servedVersions;
        // A drift detection step that could not read or update the
        // mapping leaves the check's outcome intact but the drift
        // unrecorded; it counts as a failure of the check in the stats.
        this.driftDetector = new LanceSchemaDriftDetector(client, failures::increment);
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        // Called on the cluster state applier thread: only bookkeeping
        // and a schedule here, the check itself runs on the generic pool.
        track(new IndexShardHandle(indexShard, servedVersions));
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        untrack(shardId.getIndexName());
    }

    /**
     * Start checking the index behind {@code shard}. Returns the entry,
     * or {@code null} when the index is not Lance backed or is pinned to
     * a version and never advances.
     */
    Tracked track(TrackedShard shard) {
        Settings settings = shard.settings();
        if (settings.get(LanceEngineFactory.TABLE_SETTING, "").isEmpty()) {
            return null;
        }
        if (settings.getAsLong(LanceEngineFactory.VERSION_SETTING, -1L) >= 0) {
            return null;
        }
        Tracked entry = new Tracked(shard);
        Tracked previous = tracked.put(shard.indexName(), entry);
        if (previous != null) {
            previous.cancel();
        }
        entry.task = threadPool.scheduleWithFixedDelay(() -> scheduledCheck(entry), cadence, ThreadPool.Names.GENERIC);
        LOG.debug("tracking freshness of {} on this node", shard.indexName());
        return entry;
    }

    void untrack(String indexName) {
        Tracked removed = tracked.remove(indexName);
        if (removed != null) {
            removed.cancel();
            LOG.debug("stopped tracking freshness of {} on this node", indexName);
        }
        driftDetector.forgetIndex(indexName);
        warnedWaitPolicy.remove(indexName);
        mappingErrors.remove(indexName);
    }

    /** Whether this node checks {@code indexName}. */
    public boolean isTracked(String indexName) {
        return tracked.containsKey(indexName);
    }

    /** Visible for tests: the entry of {@code indexName}, or {@code null}. */
    Tracked trackedEntry(String indexName) {
        return tracked.get(indexName);
    }

    private void scheduledCheck(Tracked entry) {
        if (entry.isCancelled() || tracked.get(entry.shard.indexName()) != entry) {
            return;
        }
        try {
            check(entry);
        } catch (Exception e) {
            // check() has counted and logged the failure; the schedule
            // must go on regardless.
        }
    }

    /**
     * Run the freshness check of {@code indexShard} now, on the calling
     * thread. The shard is the one this node holds, so the manual trigger
     * arrives here after the single shard routing. A shard whose start
     * hook has not fired yet is registered here, so the answer does not
     * depend on the applier's timing.
     */
    public Outcome syncNow(IndexShard indexShard) {
        String indexName = indexShard.shardId().getIndexName();
        Tracked entry = tracked.get(indexName);
        if (entry == null) {
            Settings settings = indexShard.indexSettings().getIndexMetadata().getSettings();
            if (settings.getAsLong(LanceEngineFactory.VERSION_SETTING, -1L) >= 0) {
                return Outcome.notChecked(indexName, "the index is pinned to index.lance.version and never advances");
            }
            entry = track(new IndexShardHandle(indexShard, servedVersions));
            if (entry == null) {
                return Outcome.notChecked(indexName, "the index is not Lance backed");
            }
        }
        return check(entry);
    }

    /**
     * One check of a tracked index. Serialised per index, so the
     * scheduled check and a manual trigger never run the same index at
     * once. Counts and logs a failure, then rethrows it.
     */
    Outcome check(Tracked entry) {
        synchronized (entry) {
            checks.increment();
            lastCheckMillis = System.currentTimeMillis();
            try {
                return checkUnguarded(entry);
            } catch (Exception e) {
                failures.increment();
                LOG.warn("freshness check failed for {}", entry.shard.indexName(), e);
                throw e;
            }
        }
    }

    private Outcome checkUnguarded(Tracked entry) {
        TrackedShard shard = entry.shard;
        String indexName = shard.indexName();
        Settings settings = shard.settings();
        String table = settings.get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        String tagSetting = settings.get(LanceEngineFactory.TAG_SETTING, "");
        String tag = tagSetting.isEmpty() ? null : tagSetting;
        boolean nodeLocal = LanceLocalClones.isNodeLocal(settings);
        long served = nodeLocal ? nodeLocalServedVersion(indexName, shard) : shard.servedVersion();
        long target;
        boolean moved;
        RestAttachAction.Derivation derivation = null;
        // One open of the latest manifest answers both questions: the
        // latest version for a latest following index, and the version
        // the tag points at now for a tag following one (tags live in the
        // table's refs, readable from any checkout).
        try (Dataset latest = LanceRegistry.openDataset(table, storageOptions)) {
            long latestVersion = latest.version();
            if (tag == null) {
                target = latestVersion;
                moved = target > served;
            } else {
                target = latest.tags().getVersion(tag);
                // A tag moves backwards as well as forwards, so any
                // difference from the served version is a move.
                moved = target != served;
            }
            // node_local: the search structures live in per node clones
            // the source never carries, so a derivation from the source
            // would flip every clone built lance_text column back to
            // keyword. The build action maintains that mapping; the check
            // only advances the reader.
            boolean derive = !nodeLocal && (moved || !entry.derivedOnce);
            if (derive) {
                // Re-apply the overrides captured at attach or register
                // so the re-derived mapping keeps the operator's type and
                // sub-field declarations. The overrides are rewritten
                // first so a rule keyed by a renamed column follows it,
                // and a rule a reset column no longer admits is dropped.
                LanceOverrides stored = LanceOverrides.of(settings);
                if (target == latestVersion) {
                    stored = driftDetector.rewriteOverridesForSchemaDrift(indexName, stored, latest.getLanceSchema());
                    derivation = RestAttachAction.derive(latest, stored, true);
                    driftDetector.warnOnLanceFieldRename(indexName, latest.getLanceSchema());
                } else {
                    // The tag points at another manifest: derive from that
                    // snapshot so the mapping matches the schema the shard
                    // is about to read.
                    try (Dataset tagged = LanceRegistry.openDataset(table, storageOptions, Optional.of(target))) {
                        stored = driftDetector.rewriteOverridesForSchemaDrift(indexName, stored, tagged.getLanceSchema());
                        derivation = RestAttachAction.derive(tagged, stored, true);
                        driftDetector.warnOnLanceFieldRename(indexName, tagged.getLanceSchema());
                    }
                }
            }
        } catch (Exception e) {
            throw e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
        }
        if (moved) {
            moves.increment();
            if (tag == null) {
                LOG.info("table {} moved to version {} (serving {}), refreshing {}", table, target, served, indexName);
            } else {
                LOG.info("tag {} on table {} now points at version {} (serving {}), refreshing {}", tag, table, target, served, indexName);
            }
        }
        boolean mappingChanged = false;
        String mappingError = null;
        // Nothing to derive this check (the version did not move and the
        // mapping was derived once already): a refusal recorded earlier
        // still stands, because the mapping the index should have has
        // not changed and the update that failed has not been retried.
        if (derivation != null) {
            if ("wait".equals(settings.get(UNCOVERED_FRAGMENT_POLICY_SETTING, "immediate"))) {
                // `wait` is accepted but converges with the immediate
                // branch: the plugin never writes to a user table, so
                // folding appended fragments into the existing indexes is
                // left to the table's writer or to
                // POST /_lance/build_indexes/{index}.
                warnWaitPolicyOnce(indexName);
            }
            entry.derivedOnce = true;
            MappingComparison comparison = shard.compareMapping(derivation.mappingJson());
            switch (comparison) {
                case UNCHANGED -> {
                    mappingUnchanged.increment();
                    // The mapping derived at this version is the one the
                    // index has. That clears a refusal only when it was
                    // recorded at another version: the index no longer
                    // wants the mapping the refused update carried. At
                    // the refused version itself the refused update was
                    // not applied, so the refusal stands.
                    RefusedMapping refused = mappingErrors.get(indexName);
                    if (refused != null && refused.version() != target) {
                        mappingErrors.remove(indexName);
                    }
                }
                case TYPE_CONFLICT -> {
                    // The rebuild creates the index with the derived
                    // mapping: the mapping is applied, the way an
                    // acknowledged update applies it.
                    mappingErrors.remove(indexName);
                    rebuild(indexName, table, storageOptions, derivation, settings, target, "preflight merge refused the type change");
                    return new Outcome(indexName, true, null, moved, served, target, true, true, null);
                }
                case CHANGED -> {
                    try {
                        client.admin()
                            .indices()
                            .preparePutMapping(indexName)
                            .setSource(derivation.mappingJson(), MediaTypeRegistry.JSON)
                            .execute()
                            .actionGet();
                        // The mapping changed only once the cluster
                        // manager acknowledged the update.
                        mappingChanged = true;
                        mappingUpdates.increment();
                        mappingErrors.remove(indexName);
                    } catch (Exception e) {
                        String message = e.getMessage() == null ? "" : e.getMessage();
                        if (message.contains("cannot be changed from type")) {
                            mappingErrors.remove(indexName);
                            rebuild(indexName, table, storageOptions, derivation, settings, target, message);
                            return new Outcome(indexName, true, null, moved, served, target, true, true, null);
                        }
                        failures.increment();
                        // The index keeps serving the mapping it has; the
                        // refusal stays visible in the stats until an
                        // update for the index is acknowledged or a
                        // later version's mapping is found in place.
                        mappingError = message;
                        mappingErrors.put(indexName, new RefusedMapping(target, message));
                        LOG.warn("mapping re-derivation failed for {} at version {}: {}", indexName, target, message);
                    }
                }
            }
        }
        if (moved) {
            // The mapping is in place before the reader advances, so a
            // request that arrives between the two sees no column the
            // mapping does not know. Refreshing is where the engine
            // opens the new version and swaps its reader.
            shard.refresh(REFRESH_SOURCE);
            // Requests key on the new version from now on; let the
            // snapshots of the version left behind close as soon as no
            // request holds them instead of waiting for the size bound.
            if (warmCache != null) {
                warmCache.retire(shard.indexUuid(), target);
            }
        }
        return new Outcome(indexName, true, null, moved, served, target, mappingChanged, false, mappingError);
    }

    /**
     * The source version a {@code node_local} shard serves: the base
     * version its clone was created at. The reader itself is over the
     * clone, whose own manifest chain advances with every index build,
     * so its version says nothing about the source.
     */
    private static long nodeLocalServedVersion(String indexName, TrackedShard shard) {
        LanceLocalClones clones = LanceLocalClones.instance();
        if (clones != null) {
            Optional<LanceLocalClones.Marker> marker = clones.current(indexName);
            if (marker.isPresent()) {
                return marker.get().sourceVersion();
            }
        }
        return shard.servedVersion();
    }

    /**
     * A Utf8 column flipped between keyword and lance_text after an FTS
     * index was created or dropped on the Lance side. PutMapping refuses
     * the type change, and leaving the stale mapping in place makes the
     * column silently unsearchable, so the index is deleted and created
     * again with the new mapping. The Lance table keeps its data. The
     * closing shard unregisters itself from this service and the new
     * shard registers when it starts.
     *
     * <p>A failed delete or create is thrown to the caller: the check
     * counts it as a failure and the manual sync answers with the error
     * instead of reporting a rebuild that did not happen. The rebuild
     * counter counts attempts.
     */
    private void rebuild(
        String indexName,
        String table,
        StorageOptions storageOptions,
        RestAttachAction.Derivation derivation,
        Settings previousSettings,
        long target,
        String cause
    ) {
        LOG.warn(
            "mapping re-derivation for {} at version {} hit a keyword <-> lance_text type change ({}); "
                + "rebuilding the OpenSearch index (Lance data is untouched)",
            indexName,
            target,
            cause
        );
        rebuilds.increment();
        try {
            client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            CreateIndexRequest create = LanceIndexCreation.request(indexName, table, storageOptions, derivation, previousSettings);
            PlainActionFuture<CreateIndexResponse> created = PlainActionFuture.newFuture();
            LanceIndexCreation.create(client, threadPool, create, created);
            created.actionGet();
            LOG.info("rebuilt index {} for table {} at version {}", indexName, table, target);
        } catch (Exception e) {
            throw new IllegalStateException(
                "rebuild of " + indexName + " after a keyword <-> lance_text type change failed: " + e.getMessage(),
                e
            );
        }
    }

    private void warnWaitPolicyOnce(String indexName) {
        if (warnedWaitPolicy.add(indexName)) {
            LOG.info(
                "index [{}] has index.lance.uncovered_fragment_policy=wait, but the plugin no longer runs auto-optimize on the user's Lance table. "
                    + "Index maintenance is expected to happen outside OpenSearch (Python, Ray, Spark, or the Lance Java SDK) or via "
                    + "an explicit POST /_lance/build_indexes/{{index}} call. The wait value is accepted for a future async-optimize implementation.",
                indexName
            );
        }
    }

    /** This node's counters for {@code GET /_lance/stats}, with the mapping updates still refused per index. */
    public LanceNodeStats.FreshnessStats stats() {
        Map<String, String> errors = new TreeMap<>();
        for (Map.Entry<String, RefusedMapping> entry : mappingErrors.entrySet()) {
            errors.put(entry.getKey(), entry.getValue().message());
        }
        return new LanceNodeStats.FreshnessStats(
            tracked.size(),
            checks.sum(),
            moves.sum(),
            mappingUpdates.sum(),
            mappingUnchanged.sum(),
            rebuilds.sum(),
            failures.sum(),
            lastCheckMillis,
            errors
        );
    }

    @Override
    public void close() {
        for (Tracked entry : tracked.values()) {
            entry.cancel();
        }
        tracked.clear();
    }

    /** The production {@link TrackedShard}: reads and drives an {@link IndexShard}. */
    static final class IndexShardHandle implements TrackedShard {

        private final IndexShard shard;
        private final LanceServedVersions servedVersions;

        IndexShardHandle(IndexShard shard, LanceServedVersions servedVersions) {
            this.shard = shard;
            this.servedVersions = servedVersions;
        }

        @Override
        public String indexName() {
            return shard.shardId().getIndexName();
        }

        @Override
        public String indexUuid() {
            return shard.shardId().getIndex().getUUID();
        }

        @Override
        public Settings settings() {
            // The index's own settings (current after a dynamic update),
            // not the merged node and index view the shard also holds.
            return shard.indexSettings().getIndexMetadata().getSettings();
        }

        /**
         * The version the shard's engine serves, from the registry the
         * engine publishes its reader manager's counter to. {@code -1}
         * when the engine is not open (below any real version, so the
         * first check after it opens counts as a move).
         */
        @Override
        public long servedVersion() {
            return servedVersions.servedVersion(shard.shardId());
        }

        /**
         * Merge the derived mapping into the shard's mapper service as a
         * preflight (nothing is applied) and compare the result with the
         * mapping the index has, the same comparison the cluster manager
         * makes before it publishes a mapping update. Equal means the
         * update would be a no-op and is not sent.
         */
        @Override
        public MappingComparison compareMapping(String mappingJson) {
            MapperService mapperService = shard.mapperService();
            try {
                DocumentMapper merged = mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    new CompressedXContent(mappingJson),
                    MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT
                );
                DocumentMapper current = mapperService.documentMapper();
                if (current != null && merged.mappingSource().equals(current.mappingSource())) {
                    return MappingComparison.UNCHANGED;
                }
                return MappingComparison.CHANGED;
            } catch (Exception e) {
                String message = e.getMessage() == null ? "" : e.getMessage();
                if (message.contains("cannot be changed from type")) {
                    return MappingComparison.TYPE_CONFLICT;
                }
                // Let the real PutMapping report whatever else is wrong.
                LOG.debug("mapping preflight for {} failed: {}", shard.shardId(), message);
                return MappingComparison.CHANGED;
            }
        }

        @Override
        public void refresh(String source) {
            shard.refresh(source);
        }
    }
}
