/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.Optional;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.lance.StorageOptions;

/**
 * Where a given node should open a Lance-backed index's table: the source
 * table for {@code index.lance.index_placement = in_table} (the default),
 * or this node's shallow clone for {@code node_local} when one exists.
 *
 * <p>This is the resolution point for the data-node open paths (the shard
 * engine's direct opens and the per-node index build). The coordinator's
 * opens (schema derivation, fragment enumeration, row counts) stay on the
 * source on purpose: schema, fragments and row counts are identical
 * between source and clone, and the coordinator must not depend on a
 * clone that only exists on data nodes.
 */
public record LanceTableLocation(String uri, StorageOptions storageOptions, boolean nodeLocal) {

    /**
     * Resolve the URI and storage options this node should open for
     * {@code indexSettings}. For {@code node_local} placement the current
     * clone (as recorded by {@link LanceLocalClones}) is returned with
     * empty storage options because the clone is on the local filesystem;
     * when no clone exists yet on this node the source comes back with
     * {@link #nodeLocal()} still {@code true}, so the caller knows to
     * create the clone before opening if it can.
     */
    public static LanceTableLocation forNode(IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();
        String source = settings.get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions sourceOptions = StorageOptions.fromIndexSettings(settings);
        if (!LanceLocalClones.isNodeLocal(settings)) {
            return new LanceTableLocation(source, sourceOptions, false);
        }
        LanceLocalClones clones = LanceLocalClones.instance();
        if (clones == null) {
            return new LanceTableLocation(source, sourceOptions, true);
        }
        Optional<LanceLocalClones.Marker> marker = clones.current(indexSettings.getIndex().getName());
        return marker.map(m -> new LanceTableLocation(m.cloneUri(), StorageOptions.empty(), true))
            .orElseGet(() -> new LanceTableLocation(source, sourceOptions, true));
    }
}
