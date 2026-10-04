/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.namespace.LanceIndexFreshnessService;
import org.opensearch.lance.namespace.LanceIndexSyncAction;
import org.opensearch.lance.namespace.LanceIndexSyncRequest;
import org.opensearch.lance.namespace.LanceIndexSyncResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Asks the node holding an index's shard to run the freshness check at
 * a version the fan out observed, so the mapping follows the first
 * request that reads a new table version instead of the next scheduled
 * check. One {@link LanceIndexSyncAction} per index and observed
 * version leaves this node: the highest version asked for is kept per
 * index uuid, and a request that observes it or an older one again
 * sends nothing. The shard's node skips the check when the mapping was
 * derived at that version already, so two coordinators observing the
 * same version cost one check. The request does not wait for the
 * answer; the check's outcome goes to the log. The entry of an index is
 * dropped when the index leaves the cluster state.
 *
 * <p>A pinned index ({@code index.plugins.lance.version}) is never
 * checked and nothing is sent for it. The sync action is an index
 * level admin permission, so the request leaves under a stashed thread
 * context: the search's caller needs no privilege for it, as for the
 * statistics prefetch the freshness check broadcasts.
 */
final class LanceFreshnessTrigger implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(LanceFreshnessTrigger.class);

    /** Sends one sync request; the production one goes through the node client. */
    interface Sender {
        void send(String indexName, long observedVersion);
    }

    private final Sender sender;
    /** Index uuid to the highest version a check was asked for. */
    private final Map<String, Long> requested = new ConcurrentHashMap<>();

    LanceFreshnessTrigger(Client client, ThreadPool threadPool, ClusterService clusterService) {
        this((indexName, observedVersion) -> {
            ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                client.execute(
                    LanceIndexSyncAction.INSTANCE,
                    new LanceIndexSyncRequest(indexName, observedVersion),
                    ActionListener.wrap(
                        response -> logOutcome(indexName, observedVersion, response),
                        e -> LOGGER.debug(
                            "lance.freshness: check of [{}] at version {} could not be asked for",
                            indexName,
                            observedVersion,
                            e
                        )
                    )
                );
            }
        }, clusterService);
    }

    /** Visible for tests: the trigger with the request's delivery replaced. */
    LanceFreshnessTrigger(Sender sender, ClusterService clusterService) {
        this.sender = sender;
        clusterService.addListener(this);
    }

    /**
     * A fan out of {@code index} read the table at {@code observedVersion}.
     * Sends the check request unless this node asked for that version
     * already or the index is pinned. For an index that follows the
     * latest version the entry keeps the highest version asked for, so a
     * fan out that read an older version than one already asked for
     * sends nothing and leaves the entry; otherwise concurrent fan outs
     * observing {@code N} then {@code N-1} would make the next
     * observation of {@code N} send again. A tag can move backwards, so
     * a tag following index is asked for whenever the version differs
     * from the last one asked for, as the shard's own skip rule does.
     *
     * @param indexMetadata the index as the cluster state has it now
     * @param observedVersion the manifest version the fan out read
     * @return whether a request was sent
     */
    boolean observed(IndexMetadata indexMetadata, long observedVersion) {
        if (observedVersion < 0 || LanceSettings.VERSION_SETTING.get(indexMetadata.getSettings()) >= 0) {
            return false;
        }
        String uuid = indexMetadata.getIndexUUID();
        boolean followsTag = !LanceSettings.TAG_SETTING.get(indexMetadata.getSettings()).isEmpty();
        boolean[] send = new boolean[1];
        requested.compute(uuid, (ignored, previous) -> {
            if (previous != null && (followsTag ? previous == observedVersion : previous >= observedVersion)) {
                return previous;
            }
            send[0] = true;
            return observedVersion;
        });
        if (!send[0]) {
            return false;
        }
        sender.send(indexMetadata.getIndex().getName(), observedVersion);
        return true;
    }

    /** Visible for tests: the last version a check of {@code indexUuid} was asked for, or {@code null}. */
    Long lastRequested(String indexUuid) {
        return requested.get(indexUuid);
    }

    /** Drops the entries of every index that left the cluster state. */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        List<Index> deleted = event.indicesDeleted();
        for (Index index : deleted) {
            requested.remove(index.getUUID());
        }
    }

    private static void logOutcome(String indexName, long observedVersion, LanceIndexSyncResponse response) {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        LanceIndexFreshnessService.Outcome outcome = response.outcome();
        if (!outcome.checked()) {
            LOGGER.debug("lance.freshness: check of [{}] at version {} skipped: {}", indexName, observedVersion, outcome.reason());
        } else {
            LOGGER.debug(
                "lance.freshness: check of [{}] at version {}: moved {}, mapping changed {}, rebuilt {}",
                indexName,
                observedVersion,
                outcome.moved(),
                outcome.mappingChanged(),
                outcome.rebuilt()
            );
        }
    }

    /** Visible for tests: how many indexes have an entry. */
    int size() {
        return requested.size();
    }
}
