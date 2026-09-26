/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.lance.LancePlugin;

/**
 * Names the thread pool a piece of background work must not run on, for
 * the {@code assert} at the entry of that work.
 *
 * <p>The {@code lance_coordinator} pool carries the request: it plans,
 * fans the fragments out and merges the answers, and a request waits on
 * it. Work that takes seconds or minutes on a large table (reading the
 * statistics of every index, opening every index to warm the Session
 * cache, checking a manifest for a new version) belongs on the generic
 * pool or on a pool of its own, and a wiring change that puts it back on
 * the coordinator turns the request's latency into that work's duration
 * without failing anything. The entry points of such work assert
 * {@link #notOnCoordinator()}: the test JVM and the test clusters run
 * with {@code -ea}, so every unit and integration test that drives the
 * work fails with an {@link AssertionError} naming the thread, while a
 * production node runs without {@code -ea} and pays nothing.
 *
 * <p>Usage: {@code assert LanceCoordinatorThreads.notOnCoordinator() :
 * LanceCoordinatorThreads.message("table statistics collection");}
 */
public final class LanceCoordinatorThreads {

    private LanceCoordinatorThreads() {}

    /**
     * Whether the calling thread is not one of the {@code lance_coordinator}
     * pool. OpenSearch names a pool's threads
     * {@code opensearch[<node>][<pool>][T#<n>]}, so the pool name is looked
     * for in the thread name.
     */
    public static boolean notOnCoordinator() {
        return Thread.currentThread().getName().contains(LancePlugin.LANCE_COORDINATOR_THREAD_POOL) == false;
    }

    /**
     * The assertion message for {@code work} (a short noun phrase such
     * as {@code "table statistics collection"}) that ran on the calling
     * thread.
     */
    public static String message(String work) {
        return work
            + " must not run on the "
            + LancePlugin.LANCE_COORDINATOR_THREAD_POOL
            + " pool but ran on thread ["
            + Thread.currentThread().getName()
            + "]";
    }
}
