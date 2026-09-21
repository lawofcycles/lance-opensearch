/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Map;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

/**
 * Task of one per-node {@link LanceFragmentQueryAction} request.
 *
 * <p>Cancellable so the coordinator can stop an executor whose request
 * has timed out and so a cancelled coordinator task (through
 * {@code _tasks/_cancel} or the client closing its connection) reaches
 * the executors as children: the coordinator sends every per-node
 * request as a child request of its own task, and the task manager
 * cancels the children of a cancelled parent. The executor checks
 * {@link #isCancelled()} between Lance batches and between Lucene
 * leaves and ends its scans with {@code TaskCancelledException}.
 *
 * <p>The executor sends no child requests today; should one be added,
 * cancelling this task cancels it too.
 */
public final class LanceFragmentQueryTask extends CancellableTask {

    public LanceFragmentQueryTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }
}
