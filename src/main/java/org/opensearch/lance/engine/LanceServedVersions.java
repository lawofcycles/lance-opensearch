/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import org.opensearch.core.index.shard.ShardId;

/**
 * The manifest version each Lance backed shard on this node serves, read
 * straight from the shard's {@code LanceReaderManager}. One instance per
 * node: every engine the node's {@link LanceEngineFactory} opens registers
 * its reader manager here when it is built and removes it when it closes,
 * and the freshness check reads the served version from here. The
 * alternative, acquiring a searcher and unwrapping its reader chain, would
 * read past the reader wrapper a security plugin installs on the shard;
 * this registry sits beside the engine, not behind the wrapper.
 */
public final class LanceServedVersions {

    private final Map<ShardId, LongSupplier> byShard = new ConcurrentHashMap<>();

    /** Record {@code servedVersion} as the reader of {@code shardId}'s served version source. */
    void register(ShardId shardId, LongSupplier servedVersion) {
        byShard.put(shardId, servedVersion);
    }

    /**
     * Forget {@code shardId}'s source, but only if it is still
     * {@code servedVersion}: a shard whose engine was replaced must not
     * lose the replacement's entry when the old engine closes.
     */
    void unregister(ShardId shardId, LongSupplier servedVersion) {
        byShard.remove(shardId, servedVersion);
    }

    /**
     * The manifest version the engine of {@code shardId} serves right now,
     * or {@code -1} when no engine of that shard is open on this node.
     */
    public long servedVersion(ShardId shardId) {
        LongSupplier supplier = byShard.get(shardId);
        return supplier == null ? -1L : supplier.getAsLong();
    }

    /** Whether an engine of {@code shardId} is open on this node. */
    public boolean has(ShardId shardId) {
        return byShard.containsKey(shardId);
    }
}
