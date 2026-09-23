/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.dispatch.LanceFragmentQueryRequest;
import org.opensearch.lance.dispatch.LanceFragmentQueryResponse;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * The per-node requests of one fan-out and the delivery of their
 * responses to the merge: the runtime of a
 * {@link org.opensearch.lance.plan.rel.physical.FanOutExec}, driven by
 * {@link PlanExecutor}.
 *
 * <p>Responses arrive on the transport thread that read them
 * ({@link TransportResponseHandler#executor()} is
 * {@link ThreadPool.Names#SAME}), so the transport layer never has
 * to queue a response on a thread pool and can never reject one.
 * A rejected response would close the channel it came on and fail
 * every other request in flight on that channel, and the request
 * whose response was dropped would never complete. On that thread
 * a response is only stored in its slot and counted; nothing of
 * Lance, Lucene or the reduce runs there.
 *
 * <p>When the last node has answered, the merge of the ordered
 * responses is submitted to the coordinator pool. Every way the
 * merge can fail to run or to finish reaches {@code done} exactly
 * once: the pool rejecting the merge, the merge throwing, a node
 * failing, and {@link #send} throwing before a request left the
 * node all end in {@link ActionListener#onFailure}; the last
 * response's merge completing ends in
 * {@link ActionListener#onResponse}. A pool rejection is passed
 * through as the pool's {@code OpenSearchRejectedExecutionException}
 * so the client sees HTTP 429.
 *
 * <p>A node whose answer will not come counts as answered with an
 * empty slot: its request timed out
 * ({@link ReceiveTimeoutTransportException}, the transport layer
 * has dropped the handler and a late answer is discarded there), or
 * its executor task was cancelled and it answered
 * {@link TaskCancelledException}. The {@link IncompleteNodeListener}
 * is told once per such node. With partial results allowed the
 * merge then runs over the responses that did arrive and the
 * {@link Outcome} carries the count of missing nodes; otherwise the
 * fan-out fails with {@link OpenSearchTimeoutException} (HTTP 504)
 * carrying the transport exception as its cause, once every node
 * has answered or timed out.
 *
 * <p>A cancelled coordinator task ends the fan-out with
 * {@link TaskCancelledException} in place of the merge (and in
 * place of any other failure), whatever the nodes answered. The
 * task's state is read at the moment {@code done} is completed, so
 * a cancellation that lands between a node's failure and the
 * completion still wins.
 */
public final class FragmentFanOut {

    private static final Logger LOGGER = LogManager.getLogger(FragmentFanOut.class);

    /** How a per-node request leaves the coordinator; {@code TransportService::sendChildRequest} outside tests. */
    public interface Sender {
        void send(DiscoveryNode node, LanceFragmentQueryRequest request, TransportResponseHandler<LanceFragmentQueryResponse> handler);
    }

    /**
     * Told about a node whose answer will not come, with the
     * exception that said so. {@code node} and {@code request} are
     * those {@link #send} was called with for the slot, null when
     * the slot was never sent.
     */
    public interface IncompleteNodeListener {
        void onIncomplete(DiscoveryNode node, LanceFragmentQueryRequest request, TransportException cause);
    }

    /**
     * What the merge receives: the responses that arrived, in slot
     * order, and how many nodes did not answer (0 when every node
     * did).
     */
    public record Outcome(List<LanceFragmentQueryResponse> responses, int incompleteNodes) {
    }

    private final int size;
    private final Executor notifyExecutor;
    private final AtomicReferenceArray<LanceFragmentQueryResponse> slots;
    private final AtomicReferenceArray<DiscoveryNode> nodes;
    private final AtomicReferenceArray<LanceFragmentQueryRequest> requests;
    private final AtomicInteger incompleteNodes = new AtomicInteger();
    private final boolean allowPartialResults;
    private final CancellableTask task;
    private final IncompleteNodeListener incompleteListener;
    private final GroupedActionListener<LanceFragmentQueryResponse> gathered;

    /**
     * A fan-out that allows partial results, runs under no task and
     * tells nobody about a node that did not answer.
     */
    public FragmentFanOut(int size, Executor mergeExecutor, Consumer<Outcome> merge, ActionListener<Void> done) {
        this(size, mergeExecutor, merge, done, true, null, Runnable::run, (node, request, cause) -> {});
    }

    /**
     * @param size                number of per-node requests
     * @param mergeExecutor       pool the merge runs on once every response is in
     * @param merge               consumes the responses in slot order
     * @param done                completed once, after the merge or on the first failure
     * @param allowPartialResults whether a node that did not answer leaves its slot empty (true) or fails the fan-out (false)
     * @param task                the coordinator task, or null; a cancelled task ends the fan-out with TaskCancelledException
     * @param notifyExecutor      pool the incomplete listener runs on, off the transport thread; a pool with an
     *                            unbounded queue, so a notification never takes a queue slot from the merge
     * @param incompleteListener  told once about every node that did not answer
     */
    public FragmentFanOut(
        int size,
        Executor mergeExecutor,
        Consumer<Outcome> merge,
        ActionListener<Void> done,
        boolean allowPartialResults,
        CancellableTask task,
        Executor notifyExecutor,
        IncompleteNodeListener incompleteListener
    ) {
        this.size = size;
        this.notifyExecutor = notifyExecutor;
        this.slots = new AtomicReferenceArray<>(size);
        this.nodes = new AtomicReferenceArray<>(size);
        this.requests = new AtomicReferenceArray<>(size);
        this.allowPartialResults = allowPartialResults;
        this.task = task;
        this.incompleteListener = incompleteListener;
        // The task's state is read here, at the completion of done,
        // and not where the failure or the merge result was produced:
        // a cancellation that lands in between still ends the request
        // as cancelled.
        ActionListener<Void> once = ActionListener.notifyOnce(ActionListener.wrap(v -> {
            if (isCancelled()) {
                done.onFailure(cancelled());
            } else {
                done.onResponse(v);
            }
        }, e -> done.onFailure(cancelledOr(e))));
        this.gathered = new GroupedActionListener<>(ActionListener.wrap(responses -> {
            // ActionRunnable routes a throwing merge and a
            // rejected submit (AbstractRunnable.onRejection
            // defaults to onFailure) to once.onFailure, and a
            // completed merge to once.onResponse.
            mergeExecutor.execute(ActionRunnable.run(once, () -> {
                ensureNotCancelled();
                List<LanceFragmentQueryResponse> ordered = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    LanceFragmentQueryResponse response = slots.get(i);
                    if (response != null) {
                        ordered.add(response);
                    }
                }
                merge.accept(new Outcome(ordered, incompleteNodes.get()));
            }));
        }, once::onFailure), size);
    }

    /**
     * Send the request for {@code slot}. A synchronous failure of
     * the sender (the node is gone, the request does not serialise,
     * the coordinator task has been cancelled and refuses new
     * children) counts as that node's failure so the fan-out still
     * completes once the other nodes have answered.
     */
    public void send(Sender sender, int slot, DiscoveryNode node, LanceFragmentQueryRequest request) {
        nodes.set(slot, node);
        requests.set(slot, request);
        try {
            sender.send(node, request, handler(slot));
        } catch (Exception e) {
            gathered.onFailure(e);
        }
    }

    /** The response handler for {@code slot}. */
    public TransportResponseHandler<LanceFragmentQueryResponse> handler(int slot) {
        return new TransportResponseHandler<>() {
            @Override
            public LanceFragmentQueryResponse read(StreamInput in) throws IOException {
                return new LanceFragmentQueryResponse(in);
            }

            @Override
            public void handleResponse(LanceFragmentQueryResponse response) {
                slots.set(slot, response);
                gathered.onResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                if (!isIncomplete(exp)) {
                    gathered.onFailure(exp);
                    return;
                }
                incompleteNodes.incrementAndGet();
                notifyIncomplete(nodes.get(slot), requests.get(slot), exp);
                if (allowPartialResults) {
                    gathered.onResponse(null);
                } else {
                    gathered.onFailure(timedOut(nodes.get(slot), exp));
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };
    }

    /**
     * Tell the listener about a node that will not answer, on the
     * notify pool: this runs from the transport thread that delivered
     * the exception (or the timeout handler's thread), and the
     * listener formats a log line and sends a cancel request, neither
     * of which belongs on a transport thread. The node is counted as
     * incomplete before this is called, so a pool that refuses the
     * runnable only costs the log line and the cancel of that
     * executor, which the timeout has already detached from the
     * request; the fan-out itself completes as before.
     */
    private void notifyIncomplete(DiscoveryNode node, LanceFragmentQueryRequest request, TransportException cause) {
        try {
            notifyExecutor.execute(() -> {
                try {
                    incompleteListener.onIncomplete(node, request, cause);
                } catch (Exception e) {
                    LOGGER.warn("lance.dispatch: the incomplete node listener failed for node [{}]", node == null ? "?" : node.getId(), e);
                }
            });
        } catch (Exception rejected) {
            LOGGER.warn(
                "lance.dispatch: could not report node [{}] as incomplete on the coordinator pool: {}",
                node == null ? "?" : node.getId(),
                rejected.toString()
            );
        }
    }

    /**
     * Whether {@code exp} says the node's answer will not come: the
     * request timed out, or the executor's task was cancelled (the
     * exception then arrives wrapped in the transport layer's
     * {@code RemoteTransportException}).
     */
    public static boolean isIncomplete(TransportException exp) {
        return exp instanceof ReceiveTimeoutTransportException || LanceCancellation.findCancelled(exp) != null;
    }

    private static OpenSearchTimeoutException timedOut(DiscoveryNode node, TransportException cause) {
        return new OpenSearchTimeoutException(
            "lance fragment request to node [" + (node == null ? "?" : node.getId()) + "] did not complete in time",
            cause
        );
    }

    private boolean isCancelled() {
        return task != null && task.isCancelled();
    }

    private TaskCancelledException cancelled() {
        return new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
    }

    private void ensureNotCancelled() {
        if (isCancelled()) {
            throw cancelled();
        }
    }

    /** {@code e}, or a {@link TaskCancelledException} carrying it when the coordinator task has been cancelled. */
    private Exception cancelledOr(Exception e) {
        if (isCancelled()) {
            TaskCancelledException cancelled = cancelled();
            cancelled.addSuppressed(e);
            return cancelled;
        }
        return e;
    }
}
