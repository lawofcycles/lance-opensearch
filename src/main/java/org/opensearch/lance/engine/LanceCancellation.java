/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;

/**
 * The cancellation state of the task a fragment path request runs
 * under, in the shape the Lance scan loops check it in.
 *
 * <p>A Lance scan cannot be interrupted from Java while it is inside
 * a batch (the work happens in native code behind the JNI call), so
 * every loop that reads Arrow batches from a {@code LanceScanner}
 * calls {@link #checkCancelled()} once per batch, before it asks for
 * the next one. The check throws {@link TaskCancelledException} when
 * the task has been cancelled, which ends the scan at the batch
 * boundary and closes the scanner through the loop's
 * try-with-resources; the request then fails with that exception.
 * The same instance drives {@code ContextIndexSearcher}'s query
 * cancellation, so Lucene's collection loop checks it between leaves
 * and every few hundred documents.
 *
 * <p>{@link #NONE} is the instance for code that runs without a task
 * (unit tests, the shard engine's reader): it never reports a
 * cancellation, so the loops need no null checks.
 */
public final class LanceCancellation {

    /**
     * Implemented by an {@link IndexSearcher} that carries the request's
     * cancellation, so {@code Query.createWeight(searcher, ...)} can
     * pick it up.
     */
    public interface Provider {
        LanceCancellation cancellation();
    }

    /** Never cancelled. */
    public static final LanceCancellation NONE = new LanceCancellation(null);

    private final CancellableTask task;

    private LanceCancellation(CancellableTask task) {
        this.task = task;
    }

    /** The cancellation of {@code task}; {@link #NONE} for a null task. */
    public static LanceCancellation of(CancellableTask task) {
        return task == null ? NONE : new LanceCancellation(task);
    }

    /**
     * The cancellation of {@code searcher} when it is a
     * {@link Provider}, otherwise {@link #NONE}.
     */
    public static LanceCancellation of(IndexSearcher searcher) {
        if (searcher instanceof Provider provider) {
            LanceCancellation cancellation = provider.cancellation();
            if (cancellation != null) {
                return cancellation;
            }
        }
        return NONE;
    }

    /** Whether this cancellation is backed by a task at all. */
    public boolean hasTask() {
        return task != null;
    }

    /** Whether the task has been cancelled. */
    public boolean isCancelled() {
        return task != null && task.isCancelled();
    }

    /**
     * Throw {@link TaskCancelledException} when the task has been
     * cancelled. Called at every batch boundary of a Lance scan and by
     * Lucene's collection loop.
     */
    public void checkCancelled() {
        if (task != null && task.isCancelled()) {
            throw new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
        }
    }

    /**
     * The {@link TaskCancelledException} in the cause chain of
     * {@code e}, or {@code null} when the chain has none. A cancelled
     * scan or collection often surfaces wrapped in the
     * {@code IOException} the Lucene Weight and reader contracts force
     * on the loops, or in the transport layer's remote exception; the
     * callers that decide "was this a cancellation" walk the chain
     * with this helper.
     */
    public static TaskCancelledException findCancelled(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TaskCancelledException cancelled) {
                return cancelled;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }
}
