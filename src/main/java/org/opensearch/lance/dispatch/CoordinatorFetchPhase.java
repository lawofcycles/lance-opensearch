/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.search.SearchHit;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

/**
 * The coordinator's fetch round: once every target's query round has
 * been absorbed into the {@link MergeReducer} and the merged page is
 * known, the hits the executors deferred are rendered on the data nodes
 * that collected them, one {@link LanceFragmentFetchRequest} per
 * (target, node) that holds rows of the page, and handed back to the
 * reducer in page position. A page whose hits were all rendered on the
 * query round (a single executor, a collapsed page, an older executor,
 * a wrapped index, the setting off) sends nothing.
 *
 * <p>Threading and failure follow the query round's fan-out
 * ({@link org.opensearch.lance.plan.execute.FragmentFanOut}): a response
 * is stored in its slot on the transport thread that read it, the
 * reducer is filled on the coordinator pool once the last response is
 * in, and {@code done} completes exactly once. A node that fails,
 * times out or is cancelled fails the whole request: the page would be
 * short of the rows that node holds, and a silently short page is worse
 * than an error. A timeout is reported as {@link OpenSearchTimeoutException}
 * (HTTP 504) carrying the transport exception, as a query round timeout
 * without partial results is; the {@link IncompleteNodeListener} is
 * told so the coordinator can cancel the executor's task. A cancelled
 * coordinator task ends the round with {@link TaskCancelledException}.
 */
final class CoordinatorFetchPhase {

    private static final Logger LOGGER = LogManager.getLogger(CoordinatorFetchPhase.class);

    private CoordinatorFetchPhase() {}

    /** How a fetch request leaves the coordinator; {@code TransportService::sendChildRequest} outside tests. */
    interface Sender {
        void send(DiscoveryNode node, LanceFragmentFetchRequest request, TransportResponseHandler<LanceFragmentFetchResponse> handler);
    }

    /** Told about a node whose fetch request timed out, with the exception that said so. */
    interface IncompleteNodeListener {
        void onIncomplete(DiscoveryNode node, LanceFragmentFetchRequest request, TransportException cause);
    }

    /** What a fetch request of one target names: the table, its storage options and the version the query round read. */
    record Target(String tableUri, StorageOptions storageOptions, long version) {
    }

    /** The rows of one (target, node) pair: their positions in the page, in page order. */
    private record Group(int target, DiscoveryNode node, List<Integer> positions) {
    }

    /**
     * Run the round over the deferred hits of {@code merged}'s page.
     *
     * @param merged the reducer every target has been absorbed into
     * @param targets the fetch target of an index name
     * @param projection the body's per hit projections
     * @param sender how a request leaves the node
     * @param completeExecutor the pool the reducer is filled on once every node answered
     * @param notifyExecutor the pool the incomplete listener runs on, off the transport thread
     * @param task the coordinator task, or null
     * @param profile the request's profile, or null without {@code profile: true}
     * @param incompleteListener told about a node whose request timed out
     * @param done completed once: after the reducer holds every hit of the page, or on the first failure
     */
    static void run(
        MergeReducer merged,
        Function<String, Target> targets,
        HitProjection projection,
        Sender sender,
        Executor completeExecutor,
        Executor notifyExecutor,
        CancellableTask task,
        LanceSearchProfile profile,
        IncompleteNodeListener incompleteListener,
        ActionListener<Void> done
    ) {
        List<MergeReducer.RankedHit> page = merged.page();
        Map<String, Group> groups = new LinkedHashMap<>();
        for (int position = 0; position < page.size(); position++) {
            MergeReducer.RankedHit ranked = page.get(position);
            if (ranked.hit() != null) {
                continue;
            }
            DiscoveryNode node = ranked.deferred().node();
            groups.computeIfAbsent(ranked.target() + "@" + node.getId(), key -> new Group(ranked.target(), node, new ArrayList<>()))
                .positions()
                .add(position);
        }
        if (groups.isEmpty()) {
            done.onResponse(null);
            return;
        }
        List<Group> ordered = new ArrayList<>(groups.values());
        AtomicReferenceArray<LanceFragmentFetchResponse> slots = new AtomicReferenceArray<>(ordered.size());
        ActionListener<Void> once = ActionListener.notifyOnce(done);
        GroupedActionListener<Void> gathered = new GroupedActionListener<>(ActionListener.wrap(ignored -> {
            completeExecutor.execute(ActionRunnable.run(once, () -> {
                if (task != null && task.isCancelled()) {
                    throw new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
                }
                for (int i = 0; i < ordered.size(); i++) {
                    Group group = ordered.get(i);
                    List<SearchHit> hits = slots.get(i).hits();
                    if (hits.size() != group.positions().size()) {
                        throw new IllegalStateException(
                            "node ["
                                + group.node().getId()
                                + "] rendered "
                                + hits.size()
                                + " rows of the "
                                + group.positions().size()
                                + " asked"
                        );
                    }
                    for (int j = 0; j < hits.size(); j++) {
                        merged.render(group.positions().get(j), hits.get(j));
                    }
                }
            }));
        }, once::onFailure), ordered.size());
        for (int i = 0; i < ordered.size(); i++) {
            Group group = ordered.get(i);
            String indexName = merged.targetName(group.target());
            Target target = targets.apply(indexName);
            long[] rowAddrs = new long[group.positions().size()];
            for (int j = 0; j < rowAddrs.length; j++) {
                rowAddrs[j] = page.get(group.positions().get(j)).rowAddr();
            }
            LanceFragmentFetchRequest request = new LanceFragmentFetchRequest(
                target.tableUri(),
                indexName,
                target.storageOptions(),
                target.version(),
                rowAddrs,
                projection
            );
            LOGGER.debug(
                "lance.dispatch: fetch round index [{}] asks node [{}] for {} rows",
                indexName,
                group.node().getId(),
                rowAddrs.length
            );
            if (profile != null) {
                profile.fetchRoundTrip();
            }
            int slot = i;
            try {
                sender.send(group.node(), request, new TransportResponseHandler<>() {
                    @Override
                    public LanceFragmentFetchResponse read(StreamInput in) throws IOException {
                        return new LanceFragmentFetchResponse(in);
                    }

                    @Override
                    public void handleResponse(LanceFragmentFetchResponse response) {
                        if (profile != null) {
                            profile.record(group.node(), response.profile());
                        }
                        slots.set(slot, response);
                        gathered.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (exp instanceof ReceiveTimeoutTransportException) {
                            // Off the transport thread: the listener logs
                            // and sends a cancel request.
                            try {
                                notifyExecutor.execute(() -> incompleteListener.onIncomplete(group.node(), request, exp));
                            } catch (Exception rejected) {
                                LOGGER.warn(
                                    "lance.dispatch: could not report node [{}] as incomplete after the fetch round timed out: {}",
                                    group.node().getId(),
                                    rejected.toString()
                                );
                            }
                            gathered.onFailure(
                                new OpenSearchTimeoutException(
                                    "lance fragment fetch request to node [" + group.node().getId() + "] did not complete in time",
                                    exp
                                )
                            );
                            return;
                        }
                        gathered.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
            } catch (Exception e) {
                gathered.onFailure(e);
            }
        }
    }
}
