/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;

/**
 * Engine factory producing a read-only engine backed by a Lance table.
 *
 * <p>{@code _search} is intercepted by {@link
 * org.opensearch.lance.dispatch.LanceDispatchActionFilter} and served by the
 * fragment path (see {@link
 * org.opensearch.lance.dispatch.TransportLanceCoordinatorAction}), so the
 * engine returned here rarely runs a query end-to-end. Its remaining
 * responsibilities are:
 * <ul>
 *   <li>{@code GET /_doc/{id}} — {@link #newReadWriteEngine} returns an
 *       engine whose {@link Engine#get(Engine.Get, java.util.function.BiFunction)}
 *       resolves the primary key through a Lance scalar-index-backed
 *       point lookup.</li>
 *   <li>Shard-level stats: {@link Engine#docStats()} reports the Lance row
 *       count, deletion count, and manifest-recorded data file total;
 *       {@link Engine#segmentsStats(boolean, boolean)} stays empty because
 *       there are no Lucene segments to describe.</li>
 *   <li>Refresh lifecycle: {@link Engine#refresh(String)} advances the
 *       shared reader when the Lance manifest version advances, so the
 *       fragment executors that open per-fragment leaves see the latest
 *       version. The reader is also the safety net for the handful of
 *       {@code _search} shapes that {@code LanceDispatchActionFilter}
 *       falls through to the shard path ({@code suggest},
 *       {@code highlighter}, {@code search_after} without {@code sort}).</li>
 * </ul>
 *
 * <p>Under the hood, the empty Lucene commit created at shard bootstrap
 * supplies sequence number metadata to the {@link ReadOnlyEngine}
 * superclass; the plugin layers a separate {@link ReferenceManager} on top
 * whose reference is a {@link LanceDirectoryReader}. The manager swaps in
 * a new reader when the Lance manifest advances so refresh does not close
 * the previous reader until in-flight readers release.
 *
 * <p>The reader is built over the node's {@link LanceWarmCache} snapshot of
 * {@code (index uuid, version)}, the same snapshot the fragment path reads,
 * so one node holds one open dataset, one fragment list and one set of
 * store columns per table version. Each reader owns a lease on its
 * snapshot for as long as it is open. Without a cache (the plugin has not
 * created one, or a test builds the factory directly) the engine opens its
 * own dataset per reader as it always did.
 */
public final class LanceEngineFactory implements EngineFactory {

    private final LanceWarmCache warmCache;
    private final LongSupplier maxDocsPerReader;
    /** Where the engines publish the version they serve, or {@code null} (tests). */
    private final LanceServedVersions servedVersions;

    /** Factory whose engines open their own dataset per reader. */
    public LanceEngineFactory() {
        this(null);
    }

    /**
     * @param warmCache snapshot cache the engines take their readers
     *                  from, or {@code null} to open a dataset per reader
     */
    public LanceEngineFactory(LanceWarmCache warmCache) {
        this(warmCache, () -> IndexWriter.MAX_DOCS);
    }

    /**
     * @param warmCache        snapshot cache the engines take their readers
     *                         from, or {@code null} to open a dataset per
     *                         reader
     * @param maxDocsPerReader read at every reader open: the most physical
     *                         rows a shard reader may hold, the current
     *                         value of {@code lance.test.max_docs_per_reader}
     *                         ({@code IndexWriter.MAX_DOCS} unless a test
     *                         lowered it)
     */
    public LanceEngineFactory(LanceWarmCache warmCache, LongSupplier maxDocsPerReader) {
        this(warmCache, maxDocsPerReader, null);
    }

    /**
     * @param servedVersions node scoped registry the engines publish their
     *                       served manifest version to while open, so the
     *                       freshness check reads it without acquiring a
     *                       searcher; {@code null} publishes nothing
     */
    public LanceEngineFactory(LanceWarmCache warmCache, LongSupplier maxDocsPerReader, LanceServedVersions servedVersions) {
        this.warmCache = warmCache;
        this.maxDocsPerReader = maxDocsPerReader;
        this.servedVersions = servedVersions;
    }

    public static final String TABLE_SETTING = "index.lance.table";
    public static final String PRIMARY_KEY_FIELD_SETTING = "index.lance.primary_key_field";
    /**
     * Arrow type of the declared primary key column. {@code "long"} covers
     * signed integer PKs (default when the setting is missing);
     * {@code "keyword"} covers Utf8 PKs, which take a
     * separate lookup path that quotes the {@code _id} inside the Lance
     * filter and holds {@code BytesRef} values in the reader instead of
     * numeric ones. Ignored when {@link #PRIMARY_KEY_FIELD_SETTING} is empty
     * because there is no PK to type in that case.
     */
    public static final String PRIMARY_KEY_TYPE_SETTING = "index.lance.primary_key_type";
    /**
     * Pin the Lance manifest version an index reads. Non-negative values pin
     * the dataset to that version; the default {@code -1L} means "follow the
     * latest version" and lets the freshness check of
     * {@link org.opensearch.lance.namespace.LanceIndexFreshnessService}
     * advance the reader as new fragments land.
     */
    public static final String VERSION_SETTING = "index.lance.version";
    /**
     * Follow a Lance tag instead of the latest manifest version. Unlike
     * {@link #VERSION_SETTING}, which is an immutable pin, a tag can be
     * moved to another version on the Lance side; the engine resolves the
     * tag to a version every time it opens or refreshes the reader, and
     * the freshness check of
     * {@link org.opensearch.lance.namespace.LanceIndexFreshnessService}
     * triggers that refresh when the resolved version differs from the
     * served one. Empty (the default) means the index does not follow a
     * tag. Attach refuses a body that sets both this and
     * {@link #VERSION_SETTING}.
     */
    public static final String TAG_SETTING = "index.lance.tag";

    /**
     * Compact JSON stringified form of the multi-fields spec captured at
     * attach time. Empty when the attach body did not carry a
     * {@code multi_fields} clause. The engine parses the string back into
     * a base → (sub → type) map on shard open and forwards it to the
     * reader so keyword sub-fields ({@code body.raw} on a {@code body}
     * Utf8 column) become queryable through doc values. See
     * {@link org.opensearch.lance.rest.RestAttachAction#serialiseMultiFields}
     * / {@link org.opensearch.lance.rest.RestAttachAction#deserialiseMultiFields}.
     */
    public static final String MULTI_FIELDS_SETTING = "index.lance.multi_fields";

    /**
     * Compact JSON stringified form of the per-column mapping overrides
     * captured at attach or namespace-register time (base type
     * overrides, date formats and keyword sub-fields; see
     * {@link org.opensearch.lance.LanceOverrides}). Empty when the
     * operator declared none. New attaches write this setting only;
     * {@link org.opensearch.lance.LanceOverrides#of} falls back to
     * {@link #MULTI_FIELDS_SETTING} for indexes created before this
     * setting existed.
     */
    public static final String OVERRIDES_SETTING = "index.lance.overrides";

    /**
     * Where the search structures of the index live: {@code in_table}
     * (default) commits index builds into the source table's own manifest
     * chain; {@code node_local} keeps the source untouched and gives every
     * data node a shallow clone under its data path (see
     * {@link LanceLocalClones}) that receives the builds and serves the
     * node's reads. Registered and validated in {@code LancePlugin}.
     */
    public static final String INDEX_PLACEMENT_SETTING = "index.lance.index_placement";

    /**
     * Arrow type kinds a Lance primary key column can take. Kept small on
     * purpose: {@link #LONG} covers signed integer PKs (any bit width up to
     * 64), {@link #KEYWORD} covers Utf8 PKs, and {@link #NONE} is the
     * sentinel used at runtime when the primary key column name is empty
     * (either because the table did not declare a PK, or because derivation
     * refused to surface a PK of an unsupported type). Integer PKs wider
     * than 64 bits are not supported.
     */
    public enum LancePrimaryKeyType {
        NONE("none"),
        LONG("long"),
        UNSIGNED_LONG("unsigned_long"),
        KEYWORD("keyword");

        private final String settingValue;

        LancePrimaryKeyType(String settingValue) {
            this.settingValue = settingValue;
        }

        public String settingValue() {
            return settingValue;
        }

        /**
         * Resolve the setting-string form back into an enum. Unknown or
         * empty values map to {@link #LONG} so indices created before the
         * setting existed (signed integer was the only kind) stay readable.
         */
        public static LancePrimaryKeyType fromSetting(String value) {
            if (value == null || value.isEmpty()) {
                return LONG;
            }
            switch (value) {
                case "keyword":
                    return KEYWORD;
                case "long":
                    return LONG;
                case "unsigned_long":
                    return UNSIGNED_LONG;
                case "none":
                    return NONE;
                default:
                    return LONG;
            }
        }
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        String table = config.getIndexSettings().getSettings().get(TABLE_SETTING);
        String field = config.getIndexSettings().getSettings().get(PRIMARY_KEY_FIELD_SETTING, "");
        // Empty field name overrides whatever the type setting says: no PK
        // means no lookup, no _id materialisation from a column, and the
        // reader will synthesise "<fragment>-<offset>" instead. Callers that
        // set field="" but leave the type setting alone (or vice versa) get
        // consistent behaviour rather than one accessor reading the field
        // and another the type.
        LancePrimaryKeyType pkType = field.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(config.getIndexSettings().getSettings().get(PRIMARY_KEY_TYPE_SETTING, "long"));
        int shardId = config.getShardId().id();
        long versionSetting = config.getIndexSettings().getSettings().getAsLong(VERSION_SETTING, -1L);
        Optional<Long> pinnedVersion = versionSetting >= 0 ? Optional.of(versionSetting) : Optional.empty();
        String tagSetting = config.getIndexSettings().getSettings().get(TAG_SETTING, "");
        String tag = tagSetting.isEmpty() ? null : tagSetting;
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(config.getIndexSettings().getSettings());
        String indexUuid = config.getIndexSettings().getIndex().getUUID();
        boolean nodeLocal = LanceLocalClones.isNodeLocal(config.getIndexSettings().getSettings());
        return new LanceReadOnlyEngine(
            config,
            table,
            field,
            pkType,
            shardId,
            pinnedVersion,
            tag,
            storageOptions,
            warmCache,
            indexUuid,
            maxDocsPerReader,
            nodeLocal,
            servedVersions
        );
    }

    static final class LanceReadOnlyEngine extends ReadOnlyEngine {

        private static final Logger LOG = LogManager.getLogger(LanceReadOnlyEngine.class);

        final String tablePath;
        final String field;
        final LancePrimaryKeyType pkType;
        final int shardId;
        /**
         * Pinned Lance manifest version. When present, {@link #resolveVersion()}
         * returns it unchanged, so every reader open and every refresh probe
         * reads the same fixed snapshot and {@code refreshIfNeeded}
         * short-circuits.
         */
        final Optional<Long> pinnedVersion;
        /**
         * Lance tag the shard follows, or {@code null}. When set (and no
         * version is pinned), {@link #resolveVersion()} asks Lance which
         * version the tag currently points at, so a tag moved on the Lance
         * side shows up on the next refresh. Captured at engine open so
         * the initial reader resolves the right version; the refresh path
         * re-reads {@link LanceEngineFactory#TAG_SETTING} through
         * {@link #currentTag()} because the tag is a Dynamic setting the
         * operator can rewrite on a running index.
         */
        final String tag;
        final StorageOptions storageOptions;
        /**
         * Snapshot cache the readers are built over, or {@code null} to open
         * a dataset per reader.
         */
        final LanceWarmCache warmCache;
        /** Key the snapshots of this index are filed under, with the version. */
        final String indexUuid;
        /** Bound on the physical rows a reader of this shard may hold; see {@link LanceEngineFactory#LanceEngineFactory(LanceWarmCache, LongSupplier)}. */
        final LongSupplier maxDocsPerReader;
        /**
         * True when {@code index.lance.index_placement} is
         * {@code node_local}: reads open this node's shallow clone (see
         * {@link LanceLocalClones}) and the refresh probe compares the
         * source's version against the clone's recorded base version.
         */
        final boolean nodeLocal;
        private final LanceReaderManager lanceReaderManager;
        /** Registry the served version is published to while the engine is open, or {@code null}. */
        private final LanceServedVersions servedVersions;
        /** The entry published to {@link #servedVersions}, removed again in {@link #closeNoLock}. */
        private final LongSupplier servedVersionEntry;

        LanceReadOnlyEngine(
            EngineConfig config,
            String table,
            String field,
            LancePrimaryKeyType pkType,
            int shardId,
            Optional<Long> pinnedVersion,
            String tag,
            StorageOptions storageOptions,
            LanceWarmCache warmCache,
            String indexUuid,
            LongSupplier maxDocsPerReader
        ) {
            this(
                config,
                table,
                field,
                pkType,
                shardId,
                pinnedVersion,
                tag,
                storageOptions,
                warmCache,
                indexUuid,
                maxDocsPerReader,
                false,
                null
            );
        }

        LanceReadOnlyEngine(
            EngineConfig config,
            String table,
            String field,
            LancePrimaryKeyType pkType,
            int shardId,
            Optional<Long> pinnedVersion,
            String tag,
            StorageOptions storageOptions,
            LanceWarmCache warmCache,
            String indexUuid,
            LongSupplier maxDocsPerReader,
            boolean nodeLocal,
            LanceServedVersions servedVersions
        ) {
            super(config, null, null, true, Function.identity(), true);
            this.tablePath = table;
            this.field = field;
            this.pkType = pkType;
            this.shardId = shardId;
            this.pinnedVersion = pinnedVersion;
            this.tag = tag;
            this.storageOptions = storageOptions;
            this.warmCache = warmCache;
            this.indexUuid = indexUuid;
            this.maxDocsPerReader = maxDocsPerReader;
            this.nodeLocal = nodeLocal;
            // The super constructor has already taken store.incRef(), the
            // IndexWriter write lock, and a DirectoryReader on the empty
            // commit. If the Lance side fails to open (table missing,
            // storage unreachable), those must be given back before the
            // exception leaves this constructor: otherwise the failed shard
            // never releases its store, the node-level ShardLock stays
            // held, and every allocation retry of the same shard fails on
            // ShardLockObtainFailedException instead of surfacing the Lance
            // error again.
            OpenSearchDirectoryReader initial = null;
            LanceReaderManager manager = null;
            try {
                Optional<Long> target = resolveVersion();
                // node_local: make sure this node's shallow clone exists at
                // the source version the shard is about to serve, before any
                // reader open resolves to it.
                Long nodeLocalBase = nodeLocal ? ensureLocalClone(target) : null;
                initial = openLanceReader(target);
                long initialVersion;
                if (target.isPresent()) {
                    // Pinned or tag-resolved: the version is known, no
                    // probe open needed.
                    initialVersion = target.get();
                } else if (nodeLocalBase != null) {
                    // The reader serves the clone, whose own manifest chain
                    // advances with every index build; the version the
                    // refresh probe compares against the source is the
                    // clone's recorded base version.
                    initialVersion = nodeLocalBase;
                } else if (LanceDirectoryReader.snapshotVersionOf(initial) >= 0) {
                    // The cache resolved the latest version when it built
                    // or found the snapshot; the reader serves exactly that.
                    initialVersion = LanceDirectoryReader.snapshotVersionOf(initial);
                } else {
                    try (Dataset probe = LanceRegistry.openDataset(tablePath, storageOptions)) {
                        initialVersion = probe.version();
                    }
                }
                manager = new LanceReaderManager(initial, this, initialVersion);
            } catch (Throwable t) {
                // openLanceReader() cleans up after itself, so `initial` is
                // only left unowned when the version probe failed after the
                // reader was already open.
                if (initial != null) {
                    try {
                        initial.close();
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                // close() is the same path IndexShard uses when it tears an
                // engine down: it marks the engine closed, runs
                // closeNoLock(), and waits for the closed latch. closeNoLock()
                // below skips the Lance manager (still null here) and then
                // ReadOnlyEngine.closeNoLock() releases the reader manager,
                // the IndexWriter lock, and the store reference in that
                // order, which is exactly what the super constructor took.
                // Calling closeNoLock() directly would release the same
                // resources but leave the engine's public close state
                // (write lock, latch) half handled.
                try {
                    close();
                } catch (Throwable suppressed) {
                    t.addSuppressed(suppressed);
                }
                if (t instanceof IOException io) {
                    throw new EngineException(config.getShardId(), "Failed to open initial Lance reader", io);
                }
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                if (t instanceof Error err) {
                    throw err;
                }
                throw new EngineException(config.getShardId(), "Failed to open initial Lance reader", t);
            }
            this.lanceReaderManager = manager;
            this.servedVersions = servedVersions;
            // Published only once the manager exists, so a reader of the
            // registry never sees a half built engine; the entry is the
            // manager's own counter, which refreshIfNeeded advances.
            this.servedVersionEntry = manager::servedVersion;
            if (servedVersions != null) {
                servedVersions.register(config.getShardId(), servedVersionEntry);
            }
        }

        /** Manifest version the shard's reader serves right now. */
        long servedVersion() {
            return lanceReaderManager.servedVersion();
        }

        /**
         * Manifest version the shard should read right now: the pinned
         * version when one is set, the version the followed tag resolves to
         * when the index follows a tag, otherwise empty (latest). Used on
         * the initial open; {@link LanceReaderManager#refreshIfNeeded} reads
         * the tag from the latest dataset it opens anyway instead of paying
         * for the separate open this method does.
         */
        Optional<Long> resolveVersion() {
            if (pinnedVersion.isPresent()) {
                return pinnedVersion;
            }
            if (tag != null) {
                return Optional.of(LanceRegistry.resolveTagVersion(tablePath, storageOptions, tag));
            }
            return Optional.empty();
        }

        /**
         * Read the currently configured {@link LanceEngineFactory#TAG_SETTING}
         * for this shard. The tag is a Dynamic setting: an operator can
         * rewrite it with {@code PUT /{index}/_settings} on a running index,
         * so the refresh path must consult cluster state rather than the
         * value captured at engine open.
         */
        String currentTag() {
            String tagSetting = config().getIndexSettings().getSettings().get(TAG_SETTING, "");
            return tagSetting.isEmpty() ? null : tagSetting;
        }

        /**
         * Make this node's shallow clone current at the source version the
         * shard serves. {@code target} is the pinned or tag-resolved
         * version, or empty for a latest-following shard, in which case an
         * existing clone's recorded version is kept (a version advance is
         * the refresh path's job) and a first contact resolves the
         * source's latest version. Returns the clone's base source
         * version, or {@code null} when no clone service is installed
         * (test harness), in which case reads keep opening the source.
         */
        private Long ensureLocalClone(Optional<Long> target) throws IOException {
            LanceLocalClones clones = LanceLocalClones.instance();
            if (clones == null) {
                return null;
            }
            String indexName = config().getShardId().getIndexName();
            long version;
            if (target.isPresent()) {
                version = target.get();
            } else {
                Optional<LanceLocalClones.Marker> marker = clones.current(indexName);
                if (marker.isPresent()) {
                    version = marker.get().sourceVersion();
                } else {
                    try (Dataset probe = LanceRegistry.openDataset(tablePath, storageOptions)) {
                        version = probe.version();
                    }
                }
            }
            clones.ensure(indexName, tablePath, storageOptions, version, false);
            return version;
        }

        /**
         * Open the shard's whole-table reader at {@code version} (empty
         * for the latest manifest). With a {@link LanceWarmCache} the
         * reader is a view over the node's snapshot of that version, the
         * one the fragment path reads too, and owns a lease on it that
         * {@link LanceDirectoryReader#doClose} releases; without one the
         * reader opens and owns its own dataset. Either way the reader
         * charges the columns it materialises in heap to the node's
         * request breaker ({@link #requestBreaker()}).
         */
        OpenSearchDirectoryReader openLanceReader(Optional<Long> version) throws IOException {
            Directory directory = engineConfig.getStore().directory();
            SegmentInfos infos = getLastCommittedSegmentInfos();
            IndexCommit commit = Lucene.getIndexCommit(infos, directory);
            if (warmCache != null) {
                return openSnapshotReader(directory, commit, version);
            }
            String openUri = tablePath;
            StorageOptions openOptions = storageOptions;
            Optional<Long> openVersion = version;
            if (nodeLocal) {
                LanceTableLocation location = LanceTableLocation.forNode(config().getIndexSettings());
                if (!location.uri().equals(tablePath)) {
                    // The clone is opened at its own latest version: the
                    // requested version numbers the source's manifest chain,
                    // and the clone's chain has moved past it with every
                    // index build.
                    openUri = location.uri();
                    openOptions = location.storageOptions();
                    openVersion = Optional.empty();
                }
            }
            Dataset dataset = LanceRegistry.openDataset(openUri, openOptions, openVersion);
            // If wrapping the dataset in a directory reader fails, close it
            // here — otherwise the JNI-owned Dataset handle leaks and
            // eventually starves the native allocator. `LanceDirectoryReader`
            // takes ownership of `dataset` only on the happy path via its
            // `doClose`.
            OpenSearchDirectoryReader wrapped = null;
            LanceDirectoryReader reader = null;
            try {
                reader = LanceDirectoryReader.open(
                    directory,
                    commit,
                    dataset,
                    field,
                    pkType,
                    currentOverrides(),
                    requestBreaker(),
                    maxDocsPerReader.getAsLong()
                );
                warnIfBoundExceeded(reader);
                wrapped = OpenSearchDirectoryReader.wrap(reader, config().getShardId());
                return wrapped;
            } catch (Throwable t) {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                } else {
                    try {
                        dataset.close();
                    } catch (Throwable suppressed) {
                        t.addSuppressed(suppressed);
                    }
                }
                if (t instanceof IOException io) {
                    throw io;
                }
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                if (t instanceof Error err) {
                    throw err;
                }
                throw new IOException(t);
            }
        }

        /**
         * The per-column mapping overrides as the index settings carry
         * them now (base type overrides and keyword sub-fields). Read
         * per reader open instead of captured at engine construction:
         * the freshness check rewrites {@code index.lance.overrides} when
         * the Lance table renames an overridden column, and the reader
         * opened for the new manifest version must classify columns and
         * resolve sub-field paths through the rewritten keys.
         * {@link org.opensearch.index.IndexSettings#getSettings} reflects
         * dynamic setting updates, so the refresh after the rewrite sees
         * the new value.
         */
        private LanceOverrides currentOverrides() {
            return LanceOverrides.of(config().getIndexSettings().getSettings());
        }

        private OpenSearchDirectoryReader openSnapshotReader(Directory directory, IndexCommit commit, Optional<Long> version)
            throws IOException {
            LanceWarmCache.Lease lease = warmCache.acquire(
                indexUuid,
                tablePath,
                storageOptions,
                version,
                field,
                pkType,
                currentOverrides()
            );
            // openForSnapshot releases the lease itself when it fails; from
            // its return on the reader owns the lease and releases it in
            // doClose, so only the wrap step needs the reader closed here.
            LanceDirectoryReader reader = LanceDirectoryReader.openForSnapshot(
                directory,
                commit,
                lease,
                lease.snapshot().isCached() ? warmCache.columnStore() : null,
                requestBreaker(),
                maxDocsPerReader.getAsLong()
            );
            try {
                warnIfBoundExceeded(reader);
                return OpenSearchDirectoryReader.wrap(reader, config().getShardId());
            } catch (Throwable t) {
                try {
                    reader.close();
                } catch (Throwable suppressed) {
                    t.addSuppressed(suppressed);
                }
                throw t;
            }
        }

        /**
         * One WARN per reader open of a table one Lucene reader cannot
         * hold whole: GET and the fragment path still see every row, but
         * {@code _stats} counts the reader's rows only.
         */
        private void warnIfBoundExceeded(LanceDirectoryReader reader) {
            if (reader.luceneBoundExceeded()) {
                LOG.warn(
                    "lance: shard reader of [{}] holds {} of {} physical rows (table [{}] is above the bound of {} rows per Lucene reader); "
                        + "searches and GET read every row through Lance, _stats docs.count reports the reader's rows",
                    config().getShardId().getIndexName(),
                    reader.maxDoc(),
                    reader.tablePhysicalRows(),
                    tablePath,
                    maxDocsPerReader.getAsLong()
                );
            }
        }

        /**
         * The node's request circuit breaker, which the shard reader charges
         * for every column it materialises in heap. The engine test
         * harness builds its {@link EngineConfig} without a breaker
         * service; that case gets a {@link NoopCircuitBreaker}.
         */
        private CircuitBreaker requestBreaker() {
            CircuitBreakerService breakers = engineConfig.getCircuitBreakerService();
            return breakers == null ? new NoopCircuitBreaker(CircuitBreaker.REQUEST) : breakers.getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
            return lanceReaderManager;
        }

        @Override
        public void refresh(String source) {
            try {
                lanceReaderManager.maybeRefresh();
            } catch (IOException e) {
                LOG.warn("Lance refresh failed for shard {}", config().getShardId(), e);
            }
        }

        // OpenSearch's default docStats() / segmentsStats() implementations
        // walk every leaf and call Lucene.segmentReader(reader) to extract a
        // SegmentReader for byte-size and segment-name bookkeeping. Lance
        // leaves are neither SegmentReaders nor Lucene segments, so those
        // helpers throw on every stats API call and take out _stats /
        // _cat/indices docs.count / _nodes/stats/indices / _cluster/stats
        // across the whole node. Recompute the fields Lance can provide,
        // leave the ones tied to Lucene segment files empty, and never
        // delegate to Lucene.segmentReader.
        //
        // count / deleted come from the leaves: each LanceFragmentLeafReader
        // reports maxDoc = physical rows and numDocs = rows outside the
        // fragment's deletion file, so the sums equal Dataset.countRows()
        // and the manifest's deletion total for the served version.
        // totalSizeInBytes is the manifest-recorded data file total the
        // reader captured at open (LanceDirectoryReader.dataFileSizesOf).
        // It is not what _cat/indices shows as store.size: that column is
        // IndexShard.storeStats() -> Store.stats(), which sums the files in
        // the shard's Lucene Directory (only the bootstrap commit here) and
        // has no engine-level override in OpenSearch 3.8.
        @Override
        public DocsStats docStats() {
            try (Searcher searcher = acquireSearcher("docStats", SearcherScope.INTERNAL)) {
                long numDocs = 0;
                long numDeletedDocs = 0;
                for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                    numDocs += ctx.reader().numDocs();
                    numDeletedDocs += ctx.reader().numDeletedDocs();
                }
                return new DocsStats.Builder().count(numDocs)
                    .deleted(numDeletedDocs)
                    .totalSizeInBytes(LanceDirectoryReader.dataFileSizesOf(searcher.getIndexReader()).knownBytes())
                    .build();
            }
        }

        @Override
        public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
            ensureOpen();
            // A Lance-backed index has no Lucene segments to report on. Return
            // an empty stats object so the request completes with sane zeros
            // instead of blowing up on Lucene.segmentReader(reader).
            return new SegmentsStats();
        }

        // Same reason as segmentsStats(): the inherited implementation walks
        // the searcher's leaves through Lucene.segmentReader to describe each
        // segment, which fails the shard on _segments. Lance fragments are
        // not Lucene segments, so report none rather than invent entries.
        @Override
        public List<Segment> segments(boolean verbose) {
            ensureOpen();
            return Collections.emptyList();
        }

        @Override
        public boolean maybeRefresh(String source) throws EngineException {
            try {
                return lanceReaderManager.maybeRefresh();
            } catch (IOException e) {
                throw new EngineException(config().getShardId(), "Lance refresh failed", e);
            }
        }

        @Override
        protected void closeNoLock(String reason, CountDownLatch closedLatch) {
            // Null while the constructor is still opening the Lance side and
            // rolls back through close(); there is no Lance reader to release
            // yet, only what ReadOnlyEngine took.
            if (servedVersions != null && servedVersionEntry != null) {
                servedVersions.unregister(config().getShardId(), servedVersionEntry);
            }
            if (lanceReaderManager != null) {
                try {
                    lanceReaderManager.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close Lance reader manager", e);
                }
            }
            super.closeNoLock(reason, closedLatch);
        }

        /**
         * RFC contract: {@code _id} get maps to a primary key point lookup.
         * Pushes the equality predicate to the Lance scalar index by scanning
         * the shard's own fragment ids with {@code <field> = <key>} as the
         * filter. A row address batch returned by Lance identifies both the
         * hit fragment and the intra-fragment offset, which are mapped back to
         * the corresponding Lucene leaf so the standard GET path can fetch the
         * source.
         */
        @Override
        public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) {
            if (field == null || field.isEmpty() || pkType == LancePrimaryKeyType.NONE) {
                // The table did not declare a primary key. With no `_id`
                // lookup column there is nothing to resolve, so return
                // NOT_EXISTS immediately rather than passing an empty field
                // name to Lance and getting a 500 back.
                return GetResult.NOT_EXISTS;
            }
            // Lance-backed indices are always single-shard (attach rejects
            // number_of_shards > 1), so GET /_doc/{id} always lands on the
            // shard holding every fragment. When multi-shard support is
            // revisited, the coordinator will need to fan out to every
            // shard because a Lance primary key has no relationship to
            // OpenSearch's hash(_id) % numShards routing.
            //
            // The Lance scan filter is built to match the declared PK
            // type. For a signed integer PK the filter is
            // `<field> = <long>`; parsing failures short-circuit to
            // NOT_EXISTS so `GET /{index}/_doc/alpha` on an integer PK
            // does not throw a 500. For a Utf8 PK the filter is
            // `<field> = '<escaped>'` with single-quote doubling on
            // the id value so ids that contain a quote (`o'brien`) do
            // not break the filter expression or open an injection
            // path; ids never fail to parse, so the empty-id short
            // circuit is the only NOT_EXISTS branch on this path.
            // For an UNSIGNED_LONG PK the filter is
            // `<field> = <literal>` where the literal is the
            // BigInteger's decimal form; Lance's SQL parser (DataFusion
            // based) accepts numeric literals wider than 64 bits and
            // matches them against a UInt64 column. Non-numeric ids
            // (including negative) short-circuit to NOT_EXISTS so
            // GET /{index}/_doc/-1 or /alpha does not throw a 500.
            String filter;
            switch (pkType) {
                case LONG: {
                    long key;
                    try {
                        key = Long.parseLong(get.id());
                    } catch (NumberFormatException e) {
                        return GetResult.NOT_EXISTS;
                    }
                    filter = field + " = " + key;
                    break;
                }
                case UNSIGNED_LONG: {
                    java.math.BigInteger key;
                    try {
                        key = new java.math.BigInteger(get.id());
                    } catch (NumberFormatException e) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (key.signum() < 0 || key.bitLength() > 64) {
                        // Outside the UInt64 range: no row can match.
                        return GetResult.NOT_EXISTS;
                    }
                    filter = field + " = " + key.toString();
                    break;
                }
                case KEYWORD: {
                    if (get.id().isEmpty()) {
                        return GetResult.NOT_EXISTS;
                    }
                    filter = field + " = '" + get.id().replace("'", "''") + "'";
                    break;
                }
                default:
                    return GetResult.NOT_EXISTS;
            }

            Engine.Searcher searcher = searcherFactory.apply("get", SearcherScope.EXTERNAL);
            try {
                List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
                List<Integer> fragmentIds = new ArrayList<>(leaves.size());
                Map<Integer, LeafReaderContext> leavesByFragment = new HashMap<>(leaves.size());
                Dataset dataset = null;
                for (LeafReaderContext ctx : leaves) {
                    LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
                    if (lance == null) {
                        continue;
                    }
                    fragmentIds.add(lance.fragmentId());
                    leavesByFragment.put(lance.fragmentId(), ctx);
                    dataset = lance.dataset();
                }
                if (dataset == null || fragmentIds.isEmpty()) {
                    searcher.close();
                    return GetResult.NOT_EXISTS;
                }
                // A reader over a table above the Lucene bound holds the
                // leading fragments only; the key may sit in any fragment,
                // so the scan then covers the whole table and a hit outside
                // the reader is served through a reader over its fragment
                // (resolveOutsideReader).
                LanceDirectoryReader lanceReader = LanceDirectoryReader.unwrap(searcher.getDirectoryReader());
                boolean boundExceeded = lanceReader != null && lanceReader.luceneBoundExceeded();

                ScanOptions.Builder options = new ScanOptions.Builder().filter(filter)
                    .columns(Collections.singletonList(field))
                    .withRowAddress(true);
                if (!boundExceeded) {
                    options.fragmentIds(fragmentIds);
                }

                try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
                    // Set once the wrapper's liveDocs hid a match inside the
                    // reader: the caller may not see the key's row, so a
                    // later scan match outside the reader must answer
                    // NOT_EXISTS rather than resolve through a reader the
                    // wrapper does not cover.
                    boolean hiddenByWrapper = false;
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowaddr = (UInt8Vector) root.getVector("_rowaddr");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            long addr = rowaddr.get(i);
                            int fragmentId = (int) (addr >>> 32);
                            int offset = (int) (addr & 0xFFFFFFFFL);
                            LeafReaderContext ctx = leavesByFragment.get(fragmentId);
                            if (ctx == null) {
                                if (boundExceeded && !hiddenByWrapper) {
                                    return resolveOutsideReader(searcher, lanceReader, dataset, fragmentId, offset);
                                }
                                continue;
                            }
                            // The wrapper reader that the security plugin
                            // interposes for DLS applies its filter through
                            // liveDocs. Consult it before returning the hit so
                            // GET honours the same DLS predicate that
                            // _search / _count already respect (otherwise a
                            // user restricted to `rating >= 4` could still
                            // GET a row with `rating = 1`).
                            LanceFragmentLeafReader lanceLeaf = LanceFragmentLeafReader.unwrap(ctx.reader());
                            int docId = lanceLeaf == null ? offset : lanceLeaf.docOfRow(offset);
                            Bits liveDocs = ctx.reader().getLiveDocs();
                            if (liveDocs != null && !liveDocs.get(docId)) {
                                hiddenByWrapper = true;
                                continue;
                            }
                            DocIdAndVersion dv = new DocIdAndVersion(docId, 1, 1, 1, ctx.reader(), ctx.docBase);
                            return new GetResult(searcher, dv, false);
                        }
                    }
                }
                searcher.close();
                return GetResult.NOT_EXISTS;
            } catch (IOException e) {
                searcher.close();
                throw new java.io.UncheckedIOException(e);
            } catch (Exception e) {
                searcher.close();
                throw new RuntimeException(e);
            }
        }

        /**
         * Serve a GET hit that sits in a fragment the shard reader does not
         * hold (the table is above the Lucene bound): open a reader over
         * that one fragment, on the shard reader's snapshot when it has
         * one and on a dataset of its own at the same version otherwise,
         * and return the hit through it. The result keeps the shard
         * searcher open until it is released, which keeps the snapshot
         * the extra reader reads from, and closes both together.
         *
         * <p>The shard searcher's top reader is the engine's own
         * {@link OpenSearchDirectoryReader} unless the index's reader
         * wrapper (the security plugin's DLS / FLS) applied itself for the
         * caller; that filter cannot be applied to a reader opened here,
         * so the lookup refuses instead of answering outside the wrapper.
         */
        private GetResult resolveOutsideReader(
            Engine.Searcher searcher,
            LanceDirectoryReader shardReader,
            Dataset dataset,
            int fragmentId,
            int offset
        ) throws IOException {
            if (!(searcher.getDirectoryReader() instanceof OpenSearchDirectoryReader)) {
                throw new IllegalStateException(
                    "GET of a row outside the shard reader of ["
                        + config().getShardId().getIndexName()
                        + "] (table above the Lucene document bound) cannot apply the index's reader wrapper (DLS / FLS)"
                );
            }
            LanceDirectoryReader single;
            LanceWarmCache.Snapshot snapshot = shardReader.snapshot();
            if (snapshot != null) {
                single = LanceDirectoryReader.openForSnapshot(
                    new ByteBuffersDirectory(),
                    snapshot,
                    snapshot.isCached() && warmCache != null ? warmCache.columnStore() : null,
                    Collections.singletonList(fragmentId),
                    null,
                    requestBreaker()
                );
            } else {
                Dataset own = LanceRegistry.openDataset(tablePath, storageOptions, Optional.of(dataset.version()));
                try {
                    single = LanceDirectoryReader.openForFragments(
                        new ByteBuffersDirectory(),
                        null,
                        own,
                        field,
                        pkType,
                        currentOverrides(),
                        Collections.singletonList(fragmentId)
                    );
                } catch (Throwable t) {
                    own.close();
                    throw t;
                }
            }
            if (single.leaves().isEmpty()) {
                single.close();
                searcher.close();
                return GetResult.NOT_EXISTS;
            }
            LeafReaderContext ctx = single.leaves().get(0);
            LanceFragmentLeafReader outsideLeaf = LanceFragmentLeafReader.unwrap(ctx.reader());
            int docId = outsideLeaf == null ? offset : outsideLeaf.docOfRow(offset);
            Engine.Searcher both = new Engine.Searcher(
                "get",
                single,
                engineConfig.getSimilarity(),
                engineConfig.getQueryCache(),
                engineConfig.getQueryCachingPolicy(),
                () -> {
                    try {
                        single.close();
                    } finally {
                        searcher.close();
                    }
                }
            );
            return new GetResult(both, new DocIdAndVersion(docId, 1, 1, 1, ctx.reader(), ctx.docBase), false);
        }
    }

    /**
     * Reference manager that swaps in a fresh {@link LanceDirectoryReader}
     * when the Lance manifest advances beyond the served version.
     */
    @SuppressForbidden(reason = "reference counting is required to atomically swap the Lance-backed reader")
    static final class LanceReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {

        private final LanceReadOnlyEngine engine;
        private volatile long servedVersion;

        LanceReaderManager(OpenSearchDirectoryReader initial, LanceReadOnlyEngine engine, long initialVersion) {
            this.current = initial;
            this.engine = engine;
            this.servedVersion = initialVersion;
        }

        /**
         * Manifest version the current reader serves: the version the
         * last {@link #refreshIfNeeded} opened, or the initial one. For a
         * {@code node_local} shard this is the source version the clone
         * was created at, not the clone's own version.
         */
        long servedVersion() {
            return servedVersion;
        }

        @Override
        protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
            // Lucene's ReferenceManager serialises calls into this method
            // (refreshLock), so `servedVersion` does not need CAS: only one
            // refresh advances it at a time. When we return a non-null
            // reference, Lucene swaps it in as `current`, decRefs the old
            // reference, and — because in-flight callers hold at least one
            // additional refcount via `tryIncRef` — the old reader (and its
            // Dataset) stays alive until the last search that acquired it
            // releases. Setting servedVersion before returning is therefore
            // safe: a concurrent `maybeRefresh()` blocks on refreshLock
            // and will observe the updated version once we return.
            //
            if (engine.pinnedVersion.isPresent()) {
                // A version pin never moves: the initial reader already
                // serves it, so there is nothing to probe.
                return null;
            }
            // One open of the latest manifest answers both questions: the
            // latest version for a latest-following shard, and the version
            // the tag points at right now for a tag-following one (tags
            // live in the table's refs, so they are readable from any
            // checkout). A tag moved on the Lance side, forwards or
            // backwards, yields a version different from servedVersion and
            // swaps the reader. The tag itself is re-read from cluster
            // state each refresh because it is a Dynamic setting: an
            // operator repointing the tag with PUT /{index}/_settings must
            // take effect on the next refresh without a shard reopen.
            String currentTag = engine.currentTag();
            long target;
            try (Dataset latest = LanceRegistry.openDataset(engine.tablePath, engine.storageOptions)) {
                target = currentTag != null ? latest.tags().getVersion(currentTag) : latest.version();
            }
            if (engine.nodeLocal && LanceLocalClones.instance() != null) {
                return refreshNodeLocal(referenceToRefresh, target);
            }
            if (target == servedVersion) {
                return null;
            }
            // A tag-following shard opens the resolved version explicitly.
            // With a warm cache every shard does, so the snapshot the new
            // reader leases is keyed on the version recorded as served (a
            // latest-following table that advanced again between the probe
            // above and the acquire is caught by the next poll). Without a
            // cache a latest-following shard opens latest again, as before.
            Optional<Long> openAt = currentTag != null || engine.warmCache != null ? Optional.of(target) : Optional.empty();
            OpenSearchDirectoryReader newReader = engine.openLanceReader(openAt);
            servedVersion = target;
            return newReader;
        }

        /**
         * The {@code node_local} refresh: the reader serves this node's
         * clone, so two advances can require a swap. A source advance
         * ({@code sourceTarget} differs from the served base version)
         * re-creates the clone at the new version and rebuilds the reader
         * over it; the clone inherits whatever indexes the source carries,
         * and indexes that were built into the previous clone only exist
         * again after the next {@code POST /_lance/build_indexes}. A build
         * commit into the current clone (the clone's own latest version
         * moved past the version the reader holds) swaps the reader so GET
         * and the shard-path shapes see the new indexes; the served base
         * version does not change in that case.
         */
        private OpenSearchDirectoryReader refreshNodeLocal(OpenSearchDirectoryReader referenceToRefresh, long sourceTarget)
            throws IOException {
            LanceLocalClones clones = LanceLocalClones.instance();
            String indexName = engine.config().getShardId().getIndexName();
            if (sourceTarget != servedVersion) {
                clones.ensure(indexName, engine.tablePath, engine.storageOptions, sourceTarget, true);
                if (engine.warmCache != null) {
                    engine.warmCache.retireAll(engine.indexUuid);
                }
                LanceReadOnlyEngine.LOG.info(
                    "lance.index_placement: source of [{}] moved to version {} (serving {}); re-cloned on this node and rebuilding the reader",
                    engine.config().getShardId().getIndexName(),
                    sourceTarget,
                    servedVersion
                );
                OpenSearchDirectoryReader newReader = engine.openLanceReader(Optional.of(sourceTarget));
                servedVersion = sourceTarget;
                return newReader;
            }
            Optional<LanceLocalClones.Marker> marker = clones.current(indexName);
            if (marker.isEmpty()) {
                return null;
            }
            long cloneLatest;
            try (Dataset clone = LanceRegistry.openDataset(marker.get().cloneUri(), StorageOptions.empty())) {
                cloneLatest = clone.version();
            }
            long readerVersion = LanceDirectoryReader.snapshotVersionOf(referenceToRefresh);
            if (readerVersion >= 0 && cloneLatest != readerVersion) {
                return engine.openLanceReader(Optional.empty());
            }
            return null;
        }

        @Override
        protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(OpenSearchDirectoryReader reference) {
            return reference.getRefCount();
        }

        @Override
        protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
            reference.decRef();
        }
    }
}
