/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Locale;

/**
 * Dispatch mode for Lance-backed index searches.
 *
 * <p>{@link #SHARD} keeps the standard OpenSearch shard fan-out: each
 * shard opens its own {@code LanceReadOnlyEngine} over a fragment
 * partition determined at attach time, and OpenSearch's usual
 * {@code TransportSearchAction} routes queries through
 * {@code SearchService.executeQueryPhase}. This is the default and
 * matches the initial-PR contract that users have exercised so far.
 *
 * <p>{@link #FRAGMENT} is the shard-free prototype path. The plugin
 * intercepts {@code indices:data/read/search} through an
 * {@link org.opensearch.action.support.ActionFilter}, enumerates the
 * Lance table's fragments at request time, and dispatches per-fragment
 * work directly to data nodes without going through the standard
 * shard iterator. Cluster state stays at one shard per Lance-backed
 * index and runtime rebalancing becomes a per-query decision. This
 * mode is under construction and returns an empty response at the
 * current milestone.
 */
public enum LanceDispatchMode {
    SHARD,
    FRAGMENT;

    /**
     * Parse the raw setting value produced by
     * {@link org.opensearch.common.settings.Setting.Property#NodeScope}
     * / dynamic updates. Values are matched case-insensitively so
     * operators can write {@code shard} or {@code SHARD} without
     * caring.
     */
    public static LanceDispatchMode parse(String rawValue) {
        if (rawValue == null) {
            throw new IllegalArgumentException("lance.dispatch.mode cannot be null");
        }
        String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "shard":
                return SHARD;
            case "fragment":
                return FRAGMENT;
            default:
                throw new IllegalArgumentException("lance.dispatch.mode must be 'shard' or 'fragment', got [" + rawValue + "]");
        }
    }
}
