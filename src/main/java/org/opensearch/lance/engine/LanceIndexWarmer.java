/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.index.IndexDescription;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;

/**
 * Reads the indexes of a Lance table into this node's shared Lance
 * {@code Session} cache right after the table is surfaced as an index,
 * so the first request that needs an index does not load it on its own
 * thread.
 *
 * <p>Lance opens an index lazily: the first scan that uses a BTree reads
 * {@code page_lookup.lance} and then every page that holds a matching
 * value with one object store request per page; the first full-text
 * scan reads the token dictionaries of every partition; the first
 * nearest scan reads the IVF centroids and the codes of the probed
 * partitions. All of it lands in the Session index cache and later
 * requests find it there, so on a table read from S3 the first request
 * pays for the load in latency bound page reads while the second takes
 * seconds. This class issues, for every index the table carries, the
 * smallest scan that makes Lance load the part named by the
 * {@link Mode}, and records what it did for {@code GET /_lance/stats}.
 *
 * <p>Trigger: the class listens to cluster state and starts one
 * warm-up per Lance-backed index that appears in the metadata (attach,
 * the namespace poll surfacing a table, or this node applying its first
 * cluster state after a restart). Every data node warms its own Session
 * cache, since the fragment path fans out to every data node. Deleting
 * the index cancels a pending or running warm-up.
 *
 * <p>Execution: one task per table on the {@link #THREAD_POOL} pool,
 * which has a single thread, so tables warm one after another and the
 * indexes of a table one after another. The task leases the table's
 * {@link LanceWarmCache} snapshot for the duration, which builds the
 * snapshot when the request path has not yet, and reads through the
 * snapshot's dataset. Requests never wait for a warm-up: they run their
 * own lazy load and Lance's cache reconciles the two. A failing index
 * is logged at WARN and the task moves to the next index; nothing here
 * can fail a request.
 */
public final class LanceIndexWarmer implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LanceIndexWarmer.class);

    /** Name of the single-threaded pool the warm-ups run on. */
    public static final String THREAD_POOL = "lance_warm_up";

    /**
     * Token the full-text probe searches for. It should match nothing:
     * Lance loads the token dictionaries to look it up (the index open)
     * and reads no posting list.
     */
    static final String FTS_PROBE_TOKEN = "lancewarmupprobe";

    /** Probe count handed to a nearest scan under {@link Mode#ALL}; Lance clamps it to the partition count. */
    static final int ALL_PARTITIONS_NPROBES = 1_000_000;

    /**
     * Largest share of the Session index cache a table's index files may
     * take for {@link Mode#ALL} to read them whole; above it the table's
     * indexes are only opened.
     */
    static final double ALL_MODE_CACHE_SHARE = 0.5d;

    /** Value of {@code lance.attach.warm_indexes}. */
    public enum Mode {
        /** Do nothing. */
        NONE,
        /**
         * Open every index: the BTree page lookup, the bitmap keys, the
         * full-text token dictionaries, the IVF centroids and one
         * partition.
         */
        METADATA,
        /**
         * Read every BTree page and every bitmap, every IVF partition;
         * full-text indexes are opened as under {@link #METADATA}
         * because their posting lists are only reachable by token.
         */
        ALL;

        public static Mode parse(String value) {
            if (value == null) {
                throw new IllegalArgumentException("lance.attach.warm_indexes must be one of none, metadata, all");
            }
            switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "none":
                    return NONE;
                case "metadata":
                    return METADATA;
                case "all":
                    return ALL;
                default:
                    throw new IllegalArgumentException("lance.attach.warm_indexes must be one of none, metadata, all, got [" + value + "]");
            }
        }

        public String settingValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Progress of one table or of one of its indexes. */
    public enum State {
        PENDING,
        RUNNING,
        DONE,
        FAILED,
        /** The mode was {@code none}, or the index type has no warm-up scan. */
        SKIPPED,
        /** The index was deleted before the warm-up finished. */
        CANCELLED;

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Immutable view of one index's warm-up. */
    public record IndexStatus(String name, String type, String column, State state, double seconds, String detail) {
    }

    /** Immutable view of one table's warm-up. */
    public record TableStatus(String index, String table, long version, Mode mode, State state, long startedAtMillis, double seconds, List<
        IndexStatus> indexes) {
    }

    /** Mutable progress of one table, published as {@link TableStatus}. */
    private static final class Task {
        final String indexName;
        final String indexUuid;
        final String table;
        final StorageOptions storageOptions;
        final Optional<Long> pinnedVersion;
        final String tag;
        final String pkField;
        final LancePrimaryKeyType pkType;
        final LanceOverrides overrides;
        final Mode mode;
        final AtomicBoolean cancelled = new AtomicBoolean();
        /** Guarded by {@code this}. */
        private State state = State.PENDING;
        private long version = -1L;
        private long startedAtMillis;
        private long startedNanos;
        private double seconds;
        private final List<IndexStatus> indexes = new ArrayList<>();

        Task(IndexMetadata metadata, Mode mode) {
            Settings settings = metadata.getSettings();
            this.indexName = metadata.getIndex().getName();
            this.indexUuid = metadata.getIndexUUID();
            this.table = settings.get(LanceEngineFactory.TABLE_SETTING);
            this.storageOptions = StorageOptions.fromIndexSettings(settings);
            long versionSetting = settings.getAsLong(LanceEngineFactory.VERSION_SETTING, -1L);
            this.pinnedVersion = versionSetting >= 0 ? Optional.of(versionSetting) : Optional.empty();
            String tagSetting = settings.get(LanceEngineFactory.TAG_SETTING, "");
            this.tag = tagSetting.isEmpty() ? null : tagSetting;
            this.pkField = settings.get(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, "");
            this.pkType = pkField.isEmpty()
                ? LancePrimaryKeyType.NONE
                : LancePrimaryKeyType.fromSetting(settings.get(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, "long"));
            this.overrides = LanceOverrides.of(settings);
            this.mode = mode;
        }

        synchronized void start(long version) {
            this.state = State.RUNNING;
            this.version = version;
            this.startedAtMillis = System.currentTimeMillis();
            this.startedNanos = System.nanoTime();
        }

        synchronized void finish(State state) {
            this.state = state;
            if (startedNanos != 0L) {
                this.seconds = (System.nanoTime() - startedNanos) / 1e9;
            }
        }

        synchronized void record(IndexStatus status) {
            indexes.add(status);
        }

        synchronized TableStatus status() {
            return new TableStatus(indexName, table, version, mode, state, startedAtMillis, seconds, List.copyOf(indexes));
        }
    }

    private final LanceWarmCache warmCache;
    private final ExecutorService executor;
    private volatile Mode mode;
    private final Map<String, Task> tasks = new ConcurrentHashMap<>();
    private volatile boolean closed;
    /**
     * Read locked by every warm-up while it runs, write locked by
     * {@link #close}, so close returns only when no warm-up is inside a
     * Lance scan.
     */
    private final ReentrantReadWriteLock runningLock = new ReentrantReadWriteLock();

    /** How long {@link #close} waits for a warm-up that is inside a Lance scan. */
    static final long CLOSE_WAIT_MILLIS = 60_000L;

    /**
     * @param warmCache the node's snapshot cache the warm-up leases the
     *                  table through
     * @param executor  where the warm-ups run; the plugin passes the
     *                  {@link #THREAD_POOL} pool
     * @param mode      initial value of {@code lance.attach.warm_indexes}
     */
    public LanceIndexWarmer(LanceWarmCache warmCache, ExecutorService executor, Mode mode) {
        this.warmCache = warmCache;
        this.executor = executor;
        this.mode = mode;
    }

    /** Dynamic {@code lance.attach.warm_indexes}; applies to warm-ups started after the change. */
    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Mode mode() {
        return mode;
    }

    /** Warm-ups this node has seen since it started, one per Lance-backed index, in no particular order. */
    public List<TableStatus> statuses() {
        List<TableStatus> out = new ArrayList<>(tasks.size());
        for (Task task : tasks.values()) {
            out.add(task.status());
        }
        return out;
    }

    /** Status of the warm-up of {@code indexName}, or empty when the node has not seen the index. */
    public Optional<TableStatus> status(String indexName) {
        Task task = tasks.get(indexName);
        return task == null ? Optional.empty() : Optional.of(task.status());
    }

    /**
     * Start a warm-up for every Lance-backed index that appeared in the
     * metadata and cancel the one of every Lance-backed index that left
     * it. Only data nodes execute fragment requests, so only they warm;
     * a node without the data role cancels whatever it still holds.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (closed) {
            return;
        }
        if (!event.state().nodes().getLocalNode().isDataNode()) {
            cancelAll();
            return;
        }
        if (!event.metadataChanged()) {
            return;
        }
        Metadata current = event.state().metadata();
        Metadata previous = event.previousState().metadata();
        for (String name : previous.indices().keySet()) {
            if (!current.hasIndex(name)) {
                Task task = tasks.remove(name);
                if (task != null) {
                    task.cancelled.set(true);
                }
            }
        }
        for (String name : current.indices().keySet()) {
            if (previous.hasIndex(name)) {
                continue;
            }
            IndexMetadata metadata = current.index(name);
            String table = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (table == null || table.isEmpty()) {
                continue;
            }
            schedule(metadata);
        }
    }

    /**
     * Queue the warm-up of {@code metadata}'s table under the current
     * mode. Under {@link Mode#NONE} the index is recorded as skipped so
     * the stats show the attach was seen, and nothing runs.
     */
    public void schedule(IndexMetadata metadata) {
        Task task = new Task(metadata, mode);
        Task previous = tasks.put(task.indexName, task);
        if (previous != null) {
            previous.cancelled.set(true);
        }
        if (task.mode == Mode.NONE) {
            task.finish(State.SKIPPED);
            return;
        }
        try {
            executor.execute(() -> run(task));
        } catch (RejectedExecutionException e) {
            task.record(new IndexStatus("", "", "", State.FAILED, 0d, "warm-up queue full: " + e.getMessage()));
            task.finish(State.FAILED);
            LOGGER.warn("warm-up of [{}] (table {}) not started: {}", task.indexName, task.table, e.getMessage());
        }
    }

    private void run(Task task) {
        if (!runningLock.readLock().tryLock()) {
            // close() holds the write lock: nothing runs any more.
            task.finish(State.CANCELLED);
            return;
        }
        try {
            warm(task);
        } finally {
            runningLock.readLock().unlock();
        }
    }

    private void warm(Task task) {
        if (task.cancelled.get() || closed) {
            task.finish(State.CANCELLED);
            return;
        }
        Optional<Long> version = task.pinnedVersion;
        LanceWarmCache.Lease lease;
        try {
            if (version.isEmpty() && task.tag != null) {
                version = Optional.of(LanceRegistry.resolveTagVersion(task.table, task.storageOptions, task.tag));
            }
            lease = warmCache.acquire(task.indexUuid, task.table, task.storageOptions, version, task.pkField, task.pkType, task.overrides);
        } catch (Exception e) {
            task.start(-1L);
            task.record(new IndexStatus("", "", "", State.FAILED, 0d, "could not open the table: " + e.getMessage()));
            task.finish(State.FAILED);
            LOGGER.warn("warm-up of [{}] (table {}) could not open the table", task.indexName, task.table, e);
            return;
        }
        try {
            Dataset dataset = lease.snapshot().dataset();
            task.start(lease.snapshot().version());
            List<IndexDescription> descriptions = dataset.describeIndices();
            Map<Integer, LanceField> fieldsById = new HashMap<>();
            for (LanceField field : dataset.getLanceSchema().fields()) {
                fieldsById.put(field.getId(), field);
            }
            Mode effective = task.mode;
            String detail = "";
            if (effective == Mode.ALL) {
                String tooLarge = exceedsCacheShare(descriptions);
                if (tooLarge != null) {
                    // Pages that do not fit are evicted while the rest
                    // load, so reading them all buys nothing; open the
                    // indexes and leave the pages to the requests.
                    effective = Mode.METADATA;
                    detail = tooLarge;
                    LOGGER.info("warm-up of [{}] (table {}) opens the indexes only: {}", task.indexName, task.table, detail);
                }
            }
            boolean anyFailed = false;
            for (IndexDescription description : descriptions) {
                if (task.cancelled.get() || closed) {
                    task.finish(State.CANCELLED);
                    return;
                }
                IndexStatus status = warmIndex(dataset, description, fieldsById, task, effective, detail);
                task.record(status);
                anyFailed |= status.state() == State.FAILED;
            }
            task.finish(anyFailed ? State.FAILED : State.DONE);
            TableStatus done = task.status();
            LOGGER.info(
                "warm-up of [{}] (table {}, version {}, mode {}) finished in {} s: {} indexes",
                task.indexName,
                task.table,
                done.version(),
                task.mode.settingValue(),
                String.format(Locale.ROOT, "%.2f", done.seconds()),
                done.indexes().size()
            );
        } catch (Exception e) {
            task.record(new IndexStatus("", "", "", State.FAILED, 0d, e.getMessage()));
            task.finish(State.FAILED);
            LOGGER.warn("warm-up of [{}] (table {}) failed", task.indexName, task.table, e);
        } finally {
            lease.release();
        }
    }

    private IndexStatus warmIndex(
        Dataset dataset,
        IndexDescription description,
        Map<Integer, LanceField> fieldsById,
        Task task,
        Mode effective,
        String detail
    ) {
        String name = description.getName();
        String type = description.getIndexType();
        List<Integer> fieldIds = description.getFieldIds();
        LanceField field = fieldIds.isEmpty() ? null : fieldsById.get(fieldIds.get(0));
        if (field == null) {
            return new IndexStatus(name, type, "", State.SKIPPED, 0d, "index column is not a top level field");
        }
        String column = field.getName();
        long startNanos = System.nanoTime();
        try {
            LanceCircuitBreaker.checkAndTrip("lance_warm_up");
            ScanOptions options = warmScan(field, type, effective);
            if (options == null) {
                return new IndexStatus(name, type, column, State.SKIPPED, 0d, "no warm-up scan for index type " + type);
            }
            drain(dataset, options);
            double seconds = (System.nanoTime() - startNanos) / 1e9;
            LOGGER.info(
                "warmed index [{}] ({} on {}) of [{}] in {} s, mode {}{}",
                name,
                type,
                column,
                task.indexName,
                String.format(Locale.ROOT, "%.2f", seconds),
                task.mode.settingValue(),
                detail.isEmpty() ? "" : " (" + detail + ")"
            );
            return new IndexStatus(name, type, column, State.DONE, seconds, detail);
        } catch (Exception e) {
            double seconds = (System.nanoTime() - startNanos) / 1e9;
            LOGGER.warn("warm-up of index [{}] ({} on {}) of [{}] failed", name, type, column, task.indexName, e);
            return new IndexStatus(name, type, column, State.FAILED, seconds, String.valueOf(e.getMessage()));
        }
    }

    /**
     * The scan that makes Lance load the part of the index the mode
     * names, or {@code null} for an index type without one. No data
     * column is projected and at most one row is taken, so the scan
     * costs what the index load costs and nothing else.
     */
    static ScanOptions warmScan(LanceField field, String indexType, Mode mode) {
        String normalised = indexType == null ? "" : indexType.replace("_", "").toUpperCase(Locale.ROOT);
        String column = field.getName();
        ScanOptions.Builder builder = new ScanOptions.Builder().columns(Collections.emptyList()).withRowAddress(true).limit(1L);
        switch (normalised) {
            case "BTREE":
            case "BITMAP": {
                // IS NULL makes Lance load the index (the BTree page
                // lookup or the bitmap keys) and read only the null
                // pages, none on a column without nulls. A lower bound
                // below every value of the type touches every page.
                String lowerBound = mode == Mode.ALL ? minimumLiteral(field.getType()) : null;
                String filter = lowerBound == null ? quote(column) + " IS NULL" : quote(column) + " >= " + lowerBound;
                return builder.filter(filter).build();
            }
            case "INVERTED":
                // A token that matches nothing loads the token
                // dictionaries and no posting list; the doc lengths and
                // postings are read per token by the first real query.
                return builder.fullTextQuery(FullTextQuery.match(FTS_PROBE_TOKEN, column)).build();
            default:
                if (normalised.startsWith("IVF")) {
                    int dimension = vectorDimension(field);
                    if (dimension <= 0) {
                        return null;
                    }
                    // k = 1 on one partition loads the centroids and that
                    // partition's codes; a probe count above any
                    // partition count (Lance clamps it) loads them all.
                    Query query = new Query.Builder().setColumn(column)
                        .setKey(new float[dimension])
                        .setK(1)
                        .setNprobes(mode == Mode.ALL ? ALL_PARTITIONS_NPROBES : 1)
                        .build();
                    return builder.nearest(query).build();
                }
                return null;
        }
    }

    /** Dimension of a {@code FixedSizeList<float32>} column, or 0 for any other type. */
    private static int vectorDimension(LanceField field) {
        if (!(field.getType() instanceof ArrowType.FixedSizeList list)) {
            return 0;
        }
        Field arrow = field.asArrowField();
        if (arrow.getChildren().isEmpty()) {
            return 0;
        }
        ArrowType child = arrow.getChildren().get(0).getType();
        boolean float32 = child instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
        return float32 ? list.getListSize() : 0;
    }

    /**
     * SQL literal below or equal to every value a column of {@code type}
     * can hold, so {@code col >= literal} is a range that covers every
     * BTree page, or {@code null} when the type has no such literal in
     * DataFusion's syntax. {@code arrow_cast} is folded to a typed
     * literal before Lance extracts the index query, which is why a
     * temporal lower bound can be written without knowing the epoch
     * layout of the column.
     */
    static String minimumLiteral(ArrowType type) {
        if (type instanceof ArrowType.Int intType) {
            if (!intType.getIsSigned()) {
                return "0";
            }
            switch (intType.getBitWidth()) {
                case 8:
                    return "-128";
                case 16:
                    return "-32768";
                case 32:
                    return "-2147483648";
                default:
                    // Long.MIN_VALUE itself parses as a float literal in
                    // DataFusion; the value above it stays an integer.
                    return "-9223372036854775807";
            }
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            switch (fp.getPrecision()) {
                case HALF:
                    return "-65504";
                case SINGLE:
                    return "-3.4028235e38";
                default:
                    return "-1.7976931348623157e308";
            }
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return "''";
        }
        if (type instanceof ArrowType.Bool) {
            return "false";
        }
        if (type instanceof ArrowType.Timestamp ts) {
            String unit = switch (ts.getUnit()) {
                case SECOND -> "Second";
                case MILLISECOND -> "Millisecond";
                case MICROSECOND -> "Microsecond";
                case NANOSECOND -> "Nanosecond";
            };
            String zone = ts.getTimezone() == null ? "None" : "Some(\"" + ts.getTimezone() + "\")";
            return "arrow_cast(-9223372036854775807, 'Timestamp(" + unit + ", " + zone + ")')";
        }
        if (type instanceof ArrowType.Date date) {
            return switch (date.getUnit()) {
                case DAY -> "arrow_cast(-2147483648, 'Date32')";
                case MILLISECOND -> "arrow_cast(-9223372036854775807, 'Date64')";
            };
        }
        return null;
    }

    /**
     * Reason to open a table's indexes instead of reading them whole
     * under {@link Mode#ALL}, or {@code null} when they may be read
     * whole: their files together are larger than
     * {@link #ALL_MODE_CACHE_SHARE} of the Session index cache. The pages
     * share the cache with every other table on the node and take more
     * memory decoded than on disk, so a table near the capacity would
     * evict its first pages before the last ones are read.
     */
    static String exceedsCacheShare(List<IndexDescription> descriptions) {
        NativeMemoryLimit.IndexCacheSizing sizing = LanceRegistry.indexCacheSizing();
        if (sizing == null) {
            return null;
        }
        long total = 0L;
        for (IndexDescription description : descriptions) {
            String type = description.getIndexType() == null ? "" : description.getIndexType().toUpperCase(Locale.ROOT);
            if (type.equals("INVERTED")) {
                // Only the token dictionaries of a full-text index are
                // read under either mode, and its files are mostly
                // posting lists.
                continue;
            }
            total += description.getTotalSizeBytes().orElse(0L);
        }
        long budget = (long) (sizing.capacityBytes() * ALL_MODE_CACHE_SHARE);
        if (total <= budget) {
            return null;
        }
        return "index files total "
            + NativeMemoryLimit.humanReadable(total)
            + ", above "
            + NativeMemoryLimit.humanReadable(budget)
            + " ("
            + (int) (ALL_MODE_CACHE_SHARE * 100)
            + "% of the index cache)";
    }

    private static String quote(String column) {
        return "`" + column.replace("`", "``") + "`";
    }

    private static void drain(Dataset dataset, ScanOptions options) throws Exception {
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                // The rows are not needed; the load Lance did to
                // produce them is.
            }
        }
    }

    private void cancelAll() {
        for (Task task : tasks.values()) {
            task.cancelled.set(true);
        }
        tasks.clear();
    }

    /**
     * Stop scheduling, cancel every pending warm-up and wait for the one
     * inside a Lance scan to leave it, so the caller can close the
     * snapshot cache without a scan still reading its dataset. A scan
     * against a slow object store ends at the current index; the wait
     * gives up after {@link #CLOSE_WAIT_MILLIS} with a WARN.
     */
    @Override
    public void close() {
        closed = true;
        cancelAll();
        try {
            if (!runningLock.writeLock().tryLock(CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("a warm-up is still running after {} ms; closing without it", CLOSE_WAIT_MILLIS);
            }
            // The write lock is kept: a warm-up that starts after close
            // finds it held and records itself as cancelled.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Whether a warm-up is inside a Lance scan right now, for tests. */
    boolean isWarmUpRunning() {
        return runningLock.getReadLockCount() > 0;
    }
}
