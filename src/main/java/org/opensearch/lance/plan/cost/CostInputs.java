/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.opensearch.lance.NativeMemoryLimit;

import java.util.Objects;

/**
 * What the cost model needs beyond the plan tree: the cluster the request
 * fans out over and the aggregation routing settings the executors run
 * with. The table's row count and column statistics are already on the
 * tree through the scan's table, so they are not repeated here. Built
 * once per planning run by the caller and handed to
 * {@code LancePlannerFactory.plan(RelNode, CostInputs)}; a caller without
 * cluster knowledge (a data node planning its own request, a unit test)
 * uses {@link #local()}.
 *
 * <p>The two settings that used to gate the aggregation pushdown outside
 * the planner are cost inputs: {@code lance.aggregation.pushdown} off and
 * a group estimate above {@code lance.aggregation.pushdown_max_groups}
 * both make the pushed scan's cost infinite, so the planner implements
 * the aggregate through the Lucene operator and explain shows that
 * choice as a cost decision.
 *
 * @param nodes data nodes the coordinator fans the request out to; 1
 *     when planning on a data node or for a single node cluster
 * @param storage where the pushed scan reads the table's bytes from
 * @param cpusPerNode CPUs of one data node; the planning node's own
 *     count stands in for the cluster's, since the nodes are assumed
 *     to match
 * @param pushdownParallelism effective {@code lance.aggregation.pushdown_parallelism}:
 *     how many Lance scans a pushed aggregate runs side by side on one
 *     node
 * @param slices effective {@code lance.fragment_path.slices}: how many
 *     collector threads the Lucene aggregator path runs on one node
 * @param pushdownEnabled effective {@code lance.aggregation.pushdown}:
 *     false prices every pushed aggregate as infinite
 * @param maxGroups effective {@code lance.aggregation.pushdown_max_groups}:
 *     a pushed aggregate whose statistics based estimate of the group
 *     rows the executor holds exceeds it is priced as infinite
 */
public record CostInputs(int nodes, StorageKind storage, int cpusPerNode, int pushdownParallelism, int slices, boolean pushdownEnabled,
    long maxGroups) {

    /** Upper bound of both parallelism settings, as {@code LancePlugin} registers them. */
    public static final int MAX_PARALLELISM = 32;

    /** Default of {@code lance.aggregation.pushdown_max_groups}, as {@code LancePlugin} registers it. */
    public static final long DEFAULT_MAX_GROUPS = 1_000_000L;

    public CostInputs {
        Objects.requireNonNull(storage, "storage");
        if (nodes < 1) {
            throw new IllegalArgumentException("nodes must be at least 1, got " + nodes);
        }
        if (cpusPerNode < 1) {
            throw new IllegalArgumentException("cpusPerNode must be at least 1, got " + cpusPerNode);
        }
        if (pushdownParallelism < 1) {
            throw new IllegalArgumentException("pushdownParallelism must be at least 1, got " + pushdownParallelism);
        }
        if (slices < 1) {
            throw new IllegalArgumentException("slices must be at least 1, got " + slices);
        }
        if (maxGroups < 1) {
            throw new IllegalArgumentException("maxGroups must be at least 1, got " + maxGroups);
        }
    }

    /** The cluster and parallelism inputs with the pushdown on and the group bound at its default. */
    public CostInputs(int nodes, StorageKind storage, int cpusPerNode, int pushdownParallelism, int slices) {
        this(nodes, storage, cpusPerNode, pushdownParallelism, slices, true, DEFAULT_MAX_GROUPS);
    }

    /**
     * The default value both parallelism settings take on a node with
     * {@code cpus} CPUs: half the CPUs, at least 1 and at most
     * {@link #MAX_PARALLELISM}, the same formula {@code LancePlugin}
     * uses for the settings' defaults.
     */
    public static int defaultParallelism(int cpus) {
        return Math.max(1, Math.min(MAX_PARALLELISM, cpus / 2));
    }

    /**
     * Inputs for a plan built without cluster knowledge: one node, local
     * storage, this JVM's CPU count, the two parallelism settings at
     * their defaults for that count, the pushdown on and the group
     * bound at its default. What a data node uses when it plans the
     * request it received, and what {@code plan(RelNode)} without inputs
     * assumes.
     */
    public static CostInputs local() {
        return local(NativeMemoryLimit.availableCpus());
    }

    /** {@link #local()} for a node with {@code cpus} CPUs, so tests can pin the parallelism. */
    public static CostInputs local(int cpus) {
        int parallelism = defaultParallelism(cpus);
        return new CostInputs(1, StorageKind.LOCAL, cpus, parallelism, parallelism);
    }

    /**
     * Inputs for a coordinator fanning out to {@code nodes} data nodes
     * over a table at {@code tableUri}, with the four settings'
     * effective values.
     */
    public static CostInputs forCluster(
        int nodes,
        String tableUri,
        int cpusPerNode,
        int pushdownParallelism,
        int slices,
        boolean pushdownEnabled,
        long maxGroups
    ) {
        return new CostInputs(nodes, StorageKind.fromUri(tableUri), cpusPerNode, pushdownParallelism, slices, pushdownEnabled, maxGroups);
    }

    /** The same inputs with {@code pushdownEnabled} replaced. */
    public CostInputs withPushdownEnabled(boolean enabled) {
        return new CostInputs(nodes, storage, cpusPerNode, pushdownParallelism, slices, enabled, maxGroups);
    }

    /** The same inputs with {@code maxGroups} replaced. */
    public CostInputs withMaxGroups(long bound) {
        return new CostInputs(nodes, storage, cpusPerNode, pushdownParallelism, slices, pushdownEnabled, bound);
    }
}
