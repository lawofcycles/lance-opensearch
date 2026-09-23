/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;

/**
 * Adoption of Lance-backed indexes the cluster knows about but the
 * poll's per-node bookkeeping does not. Cluster state keeps
 * {@code index.lance.table}, {@code index.lance.tag} and
 * {@code index.lance.storage_options.*} across a snapshot restore, a
 * full cluster restart and a manager failover, while the tracking maps
 * are per node and start empty, so they are rebuilt from those
 * settings here.
 */
final class LanceIndexAdopter {

    private static final Logger LOG = LogManager.getLogger(LanceIndexAdopter.class);

    private final Map<String, Long> servedVersions;
    private final Map<String, LanceNamespaceService.AttachedIndex> attachedIndexes;
    private final Set<String> warnedUnreachableAdopt;
    private final Set<String> warnedUnowned;
    private final LanceResurfaceGuard resurfaceGuard;

    /**
     * @param servedVersions        the poll's index name to served manifest
     *                              version map, owned by the service
     * @param attachedIndexes       the attach bookkeeping map, owned by the
     *                              service
     * @param warnedUnreachableAdopt names already warned about because
     *                              their table could not be opened
     * @param warnedUnowned         names already warned about as name
     *                              collisions, cleared on adoption
     * @param resurfaceGuard        the tombstone bookkeeping to clear when
     *                              an index is present again
     */
    LanceIndexAdopter(
        Map<String, Long> servedVersions,
        Map<String, LanceNamespaceService.AttachedIndex> attachedIndexes,
        Set<String> warnedUnreachableAdopt,
        Set<String> warnedUnowned,
        LanceResurfaceGuard resurfaceGuard
    ) {
        this.servedVersions = servedVersions;
        this.attachedIndexes = attachedIndexes;
        this.warnedUnreachableAdopt = warnedUnreachableAdopt;
        this.warnedUnowned = warnedUnowned;
        this.resurfaceGuard = resurfaceGuard;
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
     * about but the served-version map does not back into the poll's
     * bookkeeping.
     *
     * <p>The table is opened once before adopting so an index whose table
     * is unreachable is not handed to the sync loops, which would fail on
     * it every cycle; it is warned about once and retried on the next
     * poll. The served version is recorded as {@code -1}, below any real
     * manifest version, so the first sync cycle after adoption sees a
     * move and re-derives the mapping and refreshes the reader exactly
     * once. That refresh is wanted: the engine may have opened an older
     * manifest than the one the table is at now.
     */
    void adoptUntrackedIndexes(ClusterState state) {
        Set<String> roots = new HashSet<>();
        for (LanceNamespaceMetadata.Entry entry : LanceNamespaceService.currentMetadata(state).entries()) {
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
            attachedIndexes.put(indexName, new LanceNamespaceService.AttachedIndex(table, storageOptions, tag.isEmpty() ? null : tag));
        }
        servedVersions.put(indexName, -1L);
        // A restore can bring back an index under a name that was
        // deleted within the resurface grace; the index is present
        // again, so the tombstone no longer describes anything.
        resurfaceGuard.clearTombstone(indexName);
        warnedUnreachableAdopt.remove(indexName);
        warnedUnowned.remove(indexName);
        LOG.info(
            "adopting Lance-backed index {} (table {}, source {}) into the poll",
            indexName,
            table,
            adoption == Adoption.NAMESPACE ? "namespace" : "attach"
        );
    }
}
