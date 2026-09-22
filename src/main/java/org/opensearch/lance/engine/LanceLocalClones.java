/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Ref;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.threadpool.ThreadPool;

/**
 * Node-local shallow clones of Lance tables, for indexes attached with
 * {@code index.lance.index_placement = node_local}.
 *
 * <p>A table on a source the OpenSearch process cannot write to (a
 * read-only mount, an object-store prefix without write access) cannot
 * take the {@code Dataset.createIndex} commit an index build performs.
 * With {@code node_local} placement each data node shallow-clones the
 * table into its own data path ({@code Dataset.shallowClone} writes only
 * metadata at the destination; the clone's manifest references the
 * source's data files through {@code base_paths}), builds search indexes
 * into the clone, and opens the clone instead of the source for
 * everything that runs on that node. Index-build commits land in the
 * clone's own manifest chain; the source is never written.
 *
 * <p>Layout under the node's data path:
 *
 * <pre>
 * &lt;data path&gt;/lance-local/&lt;index name&gt;/clone.json         marker: source URI + source version
 * &lt;data path&gt;/lance-local/&lt;index name&gt;/v&lt;version&gt;/table.lance   the clone dataset
 * </pre>
 *
 * One clone per index per node, shared by every shard and every fragment
 * executor of the index on that node; {@link #ensure} serialises
 * concurrent creation with a per-index lock. The marker file lets a
 * restarted node tell whether its clone is current without opening the
 * source. The clone lives in a versioned directory so a re-clone after a
 * source version advance writes a fresh directory and drops the old one,
 * rather than mutating a directory an open reader may still be reading.
 * Directories are keyed by index name rather than UUID so the rebuild an
 * index goes through when a Utf8 column flips between keyword and
 * lance_text (delete plus re-create, a new UUID) finds its clones, and
 * the built search structures, again; the marker's source URI check
 * protects against a same-named index over a different table.
 *
 * <p>Lifecycle: created in {@code LancePlugin.createComponents} and
 * registered as a {@link ClusterStateListener}; when an index leaves the
 * cluster state its clone directory is removed on every node (the
 * fragment path opens clones on nodes that never host the shard, so a
 * shard-scoped removal hook would leak those).
 */
public final class LanceLocalClones implements ClusterStateListener {

    private static final Logger LOG = LogManager.getLogger(LanceLocalClones.class);

    /** Values of {@code index.lance.index_placement}. */
    public static final String PLACEMENT_IN_TABLE = "in_table";
    public static final String PLACEMENT_NODE_LOCAL = "node_local";

    /** Directory under the node's data path that holds every clone. */
    static final String ROOT_DIR_NAME = "lance-local";

    private static final String MARKER_FILE_NAME = "clone.json";
    private static final String DATASET_DIR_NAME = "table.lance";

    /**
     * Node-wide instance, installed by {@code LancePlugin.createComponents}
     * so the open paths that have no injection channel (the warm cache,
     * the engine's reader opens) can reach the clone state. {@code null}
     * until the plugin created one; every consumer treats that as
     * "placement resolution off" and opens the source.
     */
    private static volatile LanceLocalClones INSTANCE;

    public static void setInstance(LanceLocalClones instance) {
        INSTANCE = instance;
    }

    public static LanceLocalClones instance() {
        return INSTANCE;
    }

    /** Whether the index settings ask for node-local placement. */
    public static boolean isNodeLocal(Settings indexSettings) {
        return PLACEMENT_NODE_LOCAL.equals(indexSettings.get(LanceEngineFactory.INDEX_PLACEMENT_SETTING, PLACEMENT_IN_TABLE));
    }

    /** What the marker file records about the current clone. */
    public record Marker(String sourceUri, long sourceVersion, String cloneUri) {
    }

    /** A resolved clone URI plus whether {@link #ensure} just re-created it. */
    public record CloneLocation(String uri, boolean recreated) {
    }

    /**
     * How long a clone directory outlives its index in cluster state
     * before the cleanup removes it. The delay exists because the
     * keyword / lance_text mapping flip rebuilds an index as a delete
     * immediately followed by a re-create under the same name, and the
     * deletion event must not tear down the clones (and the structures
     * just built into them) that the re-created index picks up again;
     * the cleanup re-checks cluster state when it runs, so a re-created
     * index keeps its directory.
     */
    static final TimeValue CLEANUP_DELAY = TimeValue.timeValueSeconds(5);

    private final Path root;
    /**
     * Reads index metadata by UUID for {@link #locateForRead} and the
     * cleanup listener. {@code null} in unit tests that drive
     * {@link #ensure} directly.
     */
    private final ClusterService clusterService;
    /** Runs clone-directory deletion off the cluster applier thread; {@code null} runs it inline (unit tests). */
    private final ThreadPool threadPool;
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private volatile boolean orphanSweepDone;

    /**
     * @param dataPath       the node data path the clones live under; the
     *                       service creates {@code lance-local} inside it
     * @param clusterService index-metadata lookups and the deletion
     *                       listener, or {@code null} for unit tests
     * @param threadPool     schedules the delayed clone-directory
     *                       deletions, or {@code null} to run them inline
     */
    public LanceLocalClones(Path dataPath, ClusterService clusterService, ThreadPool threadPool) {
        this.root = dataPath.resolve(ROOT_DIR_NAME);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    Path rootDir() {
        return root;
    }

    private Path indexDir(String indexName) {
        return root.resolve(indexName);
    }

    private Path markerPath(String indexName) {
        return indexDir(indexName).resolve(MARKER_FILE_NAME);
    }

    private Path cloneDatasetDir(String indexName, long version) {
        return indexDir(indexName).resolve("v" + version).resolve(DATASET_DIR_NAME);
    }

    private Object lockFor(String indexName) {
        return locks.computeIfAbsent(indexName, k -> new Object());
    }

    /**
     * The current clone of {@code indexName} as recorded in its marker
     * file, or empty when no clone exists (or its directory is gone).
     * Reads only local files; the source is never opened.
     */
    public Optional<Marker> current(String indexName) {
        Marker marker = readMarker(indexName);
        if (marker == null || !Files.isDirectory(Path.of(marker.cloneUri()))) {
            return Optional.empty();
        }
        return Optional.of(marker);
    }

    /**
     * Make sure a clone of {@code sourceUri} at {@code sourceVersion}
     * exists for {@code indexName} and return its dataset URI. When the
     * recorded clone already matches, nothing is opened and the existing
     * URI comes back. Otherwise the source is opened, shallow-cloned at
     * {@code Ref.ofMain(sourceVersion)} into a fresh versioned directory,
     * the marker is rewritten, and stale versioned directories are
     * removed.
     *
     * <p>{@code exact = false} additionally accepts a recorded clone that
     * is <em>ahead</em> of the requested version, so a fragment request
     * that raced a re-clone does not tear the newer clone down again.
     * The engine's refresh path passes {@code exact = true} because it is
     * the authority on which version the index serves (a followed tag can
     * move backwards).
     */
    public CloneLocation ensure(String indexName, String sourceUri, StorageOptions sourceOptions, long sourceVersion, boolean exact)
        throws IOException {
        synchronized (lockFor(indexName)) {
            Marker marker = readMarker(indexName);
            if (marker != null && Files.isDirectory(Path.of(marker.cloneUri()))) {
                boolean matches = exact ? marker.sourceVersion() == sourceVersion : marker.sourceVersion() >= sourceVersion;
                if (matches && marker.sourceUri().equals(sourceUri)) {
                    return new CloneLocation(marker.cloneUri(), false);
                }
            }
            Path datasetDir = cloneDatasetDir(indexName, sourceVersion);
            Path versionDir = datasetDir.getParent();
            if (Files.exists(versionDir)) {
                // A half-written leftover from an interrupted clone at the
                // same version; the shallow clone below refuses an existing
                // dataset, so clear it first.
                deleteRecursively(versionDir);
            }
            Files.createDirectories(versionDir);
            try (Dataset source = LanceRegistry.openDataset(sourceUri, sourceOptions)) {
                // An explicit empty map so the clone's store options do not
                // inherit the source's (an object-store source must not leak
                // its credentials into the local clone's parameters).
                source.shallowClone(datasetDir.toString(), Ref.ofMain(sourceVersion), Map.of()).close();
            }
            writeMarker(indexName, new Marker(sourceUri, sourceVersion, datasetDir.toString()));
            dropStaleVersionDirs(indexName, versionDir);
            LOG.info(
                "lance.index_placement: node-local clone of table [{}] at version {} created under [{}]",
                sourceUri,
                sourceVersion,
                datasetDir
            );
            return new CloneLocation(datasetDir.toString(), true);
        }
    }

    /**
     * Resolve where a read of {@code indexUuid} should open, for callers
     * that only know the index UUID (the warm cache). Empty when the index
     * is not {@code node_local} (or is unknown, or this service has no
     * cluster state to ask), in which case the caller opens the source it
     * was handed. Otherwise the clone is ensured at {@code requestedVersion}
     * (the source version the coordinator resolved) or, when none was
     * shipped, at the recorded clone's version, falling back to the
     * source's latest version for a first contact on this node.
     *
     * <p>A failure to clone logs and returns empty so the read falls back
     * to the source: data reads on the source always work, only the
     * clone-built indexes are missing there.
     */
    public Optional<CloneLocation> locateForRead(
        String indexUuid,
        String tableUri,
        StorageOptions storageOptions,
        Optional<Long> requestedVersion
    ) {
        if (clusterService == null) {
            return Optional.empty();
        }
        IndexMetadata metadata = indexByUuid(indexUuid);
        if (metadata == null || !isNodeLocal(metadata.getSettings())) {
            return Optional.empty();
        }
        String indexName = metadata.getIndex().getName();
        try {
            long target;
            if (requestedVersion.isPresent()) {
                target = requestedVersion.get();
            } else {
                Optional<Marker> marker = current(indexName);
                if (marker.isPresent()) {
                    return Optional.of(new CloneLocation(marker.get().cloneUri(), false));
                }
                try (Dataset source = LanceRegistry.openDataset(tableUri, storageOptions)) {
                    target = source.version();
                }
            }
            return Optional.of(ensure(indexName, tableUri, storageOptions, target, false));
        } catch (Exception e) {
            LOG.warn("lance.index_placement: could not resolve the node-local clone of [{}]; reading the source", tableUri, e);
            return Optional.empty();
        }
    }

    /** Remove the clone directory of {@code indexName}, if any. */
    public void delete(String indexName) throws IOException {
        synchronized (lockFor(indexName)) {
            Path dir = indexDir(indexName);
            if (Files.exists(dir)) {
                deleteRecursively(dir);
                LOG.info("lance.index_placement: removed node-local clone directory [{}]", dir);
            }
        }
        locks.remove(indexName);
    }

    /** Bytes and recorded source version of one clone. */
    public record CloneStat(long bytes, long sourceVersion) {
    }

    /**
     * Every clone on this node, keyed by index name (resolved through
     * cluster state; falls back to the index UUID when the index is
     * already gone from the state). Sizes count the files under the
     * index's clone directory, which are manifests and index files only
     * because the data files stay in the source.
     */
    public Map<String, CloneStat> cloneStats() {
        Map<String, CloneStat> stats = new LinkedHashMap<>();
        if (!Files.isDirectory(root)) {
            return stats;
        }
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(root)) {
            for (Path dir : dirs) {
                if (!Files.isDirectory(dir)) {
                    continue;
                }
                String indexName = dir.getFileName().toString();
                Marker marker = readMarker(indexName);
                long version = marker == null ? -1L : marker.sourceVersion();
                stats.put(indexName, new CloneStat(sizeOf(dir), version));
            }
        } catch (IOException e) {
            LOG.warn("lance.index_placement: could not list clone directories under [{}]", root, e);
        }
        return stats;
    }

    private IndexMetadata indexByUuid(String indexUuid) {
        if (clusterService == null) {
            return null;
        }
        for (IndexMetadata metadata : clusterService.state().metadata().indices().values()) {
            if (indexUuid.equals(metadata.getIndexUUID())) {
                return metadata;
            }
        }
        return null;
    }

    /**
     * Deletes the clone directory of every index that left the cluster
     * state, plus (once, on the first applied state) any orphan directory
     * a previous process left behind. The deletion runs off the applier
     * thread and, for index deletions, after {@link #CLEANUP_DELAY}, so
     * the delete half of a mapping-flip rebuild does not remove the
     * clones the re-created index is about to reuse.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean sweep = !orphanSweepDone;
        if (sweep) {
            orphanSweepDone = true;
        }
        if (!sweep && event.indicesDeleted().isEmpty()) {
            return;
        }
        Runnable task = () -> {
            if (!Files.isDirectory(root)) {
                return;
            }
            try (DirectoryStream<Path> dirs = Files.newDirectoryStream(root)) {
                for (Path dir : dirs) {
                    String indexName = dir.getFileName().toString();
                    // Re-check against the state that is current at
                    // deletion time, not the one that queued this task: a
                    // rebuild (delete immediately followed by re-create
                    // under the same name) must find its clones again.
                    if (clusterService != null && clusterService.state().metadata().hasIndex(indexName)) {
                        continue;
                    }
                    try {
                        delete(indexName);
                    } catch (IOException e) {
                        LOG.warn("lance.index_placement: could not remove clone directory [{}]", dir, e);
                    }
                }
            } catch (IOException e) {
                LOG.warn("lance.index_placement: could not list clone directories under [{}]", root, e);
            }
        };
        if (threadPool == null) {
            task.run();
        } else if (sweep) {
            threadPool.generic().execute(task);
        } else {
            threadPool.schedule(task, CLEANUP_DELAY, ThreadPool.Names.GENERIC);
        }
    }

    private void dropStaleVersionDirs(String indexName, Path keep) {
        Path dir = indexDir(indexName);
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir)) {
            for (Path child : children) {
                if (!Files.isDirectory(child) || child.equals(keep)) {
                    continue;
                }
                deleteRecursively(child);
            }
        } catch (IOException e) {
            // A stale directory only wastes a few metadata files; the next
            // ensure() retries the removal.
            LOG.warn("lance.index_placement: could not drop stale clone directories under [{}]", dir, e);
        }
    }

    Marker readMarker(String indexName) {
        Path path = markerPath(indexName);
        if (!Files.isRegularFile(path)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(path);
            Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(bytes), false, XContentType.JSON).v2();
            Object uri = map.get("source_uri");
            Object version = map.get("source_version");
            Object clone = map.get("clone_uri");
            if (!(uri instanceof String) || !(version instanceof Number) || !(clone instanceof String)) {
                return null;
            }
            return new Marker((String) uri, ((Number) version).longValue(), (String) clone);
        } catch (Exception e) {
            LOG.warn("lance.index_placement: unreadable clone marker [{}]; the clone will be re-created", path, e);
            return null;
        }
    }

    void writeMarker(String indexName, Marker marker) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject()
                .field("source_uri", marker.sourceUri())
                .field("source_version", marker.sourceVersion())
                .field("clone_uri", marker.cloneUri())
                .endObject();
            Files.write(markerPath(indexName), BytesReference.toBytes(BytesReference.bytes(builder)));
        }
    }

    private static long sizeOf(Path dir) {
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
